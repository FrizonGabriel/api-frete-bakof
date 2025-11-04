# app.py
import os, math, re
from typing import Dict, Any, List, Tuple, Optional
import pandas as pd
from flask import Flask, request, Response

# ==========================
# CONFIG
# ==========================
TOKEN_SECRETO = os.getenv("TOKEN_SECRETO", "teste123")
ARQ_PLANILHA = os.getenv("PLANILHA_FRETE", "tabela de frete atualizada(2)(Recuperado Automaticamente).xlsx")

DEFAULT_VALOR_KM = float(os.getenv("DEFAULT_VALOR_KM", "7.0"))
DEFAULT_TAM_CAMINHAO = float(os.getenv("DEFAULT_TAM_CAMINHAO", "8.5"))   # metros úteis
DEFAULT_KM = float(os.getenv("DEFAULT_KM", "100.0"))                      # fallback final

PALAVRAS_IGNORAR = {
    "VALOR KM","TAMANHO CAMINHAO","TAMANHO CAMINHÃO",
    "CALCULO DE FRETE POR TAMANHO DE PEÇA","CÁLCULO DE FRETE POR TAMANHO DE PEÇA"
}

# Estimativas de distância (km) saindo de Frederico Westphalen/RS (usado só como fallback por UF)
KM_APROX_POR_UF = {
    "RS": 150, "SC": 450, "PR": 700, "SP": 1100, "RJ": 1500, "MG": 1600, "ES": 1800,
    "MS": 1600, "MT": 2200, "DF": 2000, "GO": 2100, "TO": 2500, "BA": 2600, "SE": 2700,
    "AL": 2800, "PE": 3000, "PB": 3100, "RN": 3200, "CE": 3400, "PI": 3300, "MA": 3500,
    "PA": 3800, "AP": 4100, "AM": 4200, "RO": 4000, "AC": 4300, "RR": 4500,
}

UF_CEP_RANGES = [
    ("SP",  01000000, 19999999),
    ("RJ",  20000000, 28999999),
    ("ES",  29000000, 29999999),
    ("MG",  30000000, 39999999),
    ("BA",  40000000, 48999999),
    ("SE",  49000000, 49999999),
    ("PE",  50000000, 56999999),
    ("AL",  57000000, 57999999),
    ("PB",  58000000, 58999999),
    ("RN",  59000000, 59999999),
    ("CE",  60000000, 63999999),
    ("PI",  64000000, 64999999),
    ("MA",  65000000, 65999999),
    ("PA",  66000000, 68899999),
    ("AP",  68900000, 68999999),
    ("AM",  69000000, 69899999),
    ("RR",  69300000, 69399999),
    ("AC",  69900000, 69999999),
    ("DF",  70000000, 73699999),
    ("GO",  72800000, 76799999),
    ("TO",  77000000, 77999999),
    ("MT",  78000000, 78899999),
    ("MS",  79000000, 79999999),
    ("PR",  80000000, 87999999),
    ("SC",  88000000, 89999999),
    ("RS",  90000000, 99999999),
]

app = Flask(__name__)

# ==========================
# HELPERS
# ==========================
def limpar_texto(nome: Any) -> str:
    if not isinstance(nome, str): return ""
    return " ".join(nome.replace("\n"," ").split()).strip()

def so_digitos(cep: Any) -> str:
    s = re.sub(r"\D","", str(cep or ""))
    return s[:8] if len(s) >= 8 else s.zfill(8)

def uf_por_cep(cep8: str) -> Optional[str]:
    try:
        n = int(cep8)
    except Exception:
        return None
    for uf, a, b in UF_CEP_RANGES:
        if a <= n <= b:
            return uf
    return None

def extrai_constante(sheet_raw: pd.DataFrame, chave: str, default: float) -> float:
    chave = chave.upper()
    for _, row in sheet_raw.iterrows():
        textos = [str(v).strip() for v in row if isinstance(v, str)]
        if any(chave in t.upper() for t in textos):
            for v in row:
                try:
                    fv = float(str(v).replace(",", "."))
                    if math.isfinite(fv) and fv > 0: 
                        return fv
                except Exception:
                    pass
    return default

# ==========================
# LEITURA DA PLANILHA
# ==========================
def carregar_constantes(xls: pd.ExcelFile) -> Dict[str, float]:
    # tenta abas mais comuns
    for aba in ("D", "BASE_CALCULO", "BASE", "CONSTANTES"):
        try:
            raw = pd.read_excel(xls, aba, header=None)
            return {
                "VALOR_KM": extrai_constante(raw, "VALOR KM", DEFAULT_VALOR_KM),
                "TAM_CAMINHAO": extrai_constante(raw, "TAMANHO CAMINHAO", DEFAULT_TAM_CAMINHAO),
            }
        except Exception:
            continue
    # fallback
    return {"VALOR_KM": DEFAULT_VALOR_KM, "TAM_CAMINHAO": DEFAULT_TAM_CAMINHAO}

def carregar_cadastro_produtos(xls: pd.ExcelFile) -> pd.DataFrame:
    # tenta achar aba com cadastro
    sheet = None
    for aba in ("CADASTRO_PRODUTO","CADASTRO","PRODUTOS"):
        if aba in xls.sheet_names:
            sheet = aba; break
    if not sheet:
        return pd.DataFrame(columns=["nome","dim1","dim2"])

    raw = pd.read_excel(xls, sheet, header=None)
    # Heurística simples de posições
    nome_col = 2 if raw.shape[1] > 2 else 0
    dim1_col = 3 if raw.shape[1] > 3 else (1 if raw.shape[1] > 1 else 0)
    dim2_col = 4 if raw.shape[1] > 4 else (2 if raw.shape[1] > 2 else 1)
    df = raw[[nome_col, dim1_col, dim2_col]].copy()
    df.columns = ["nome","dim1","dim2"]

    df["nome"] = df["nome"].apply(limpar_texto)
    df = df[~df["nome"].str.upper().isin(PALAVRAS_IGNORAR)]
    df = df[df["nome"].astype(str).str.len() > 0]

    df["dim1"] = pd.to_numeric(df["dim1"], errors="coerce").fillna(0.0)
    df["dim2"] = pd.to_numeric(df["dim2"], errors="coerce").fillna(0.0)
    df = df.drop_duplicates(subset=["nome"], keep="first").reset_index(drop=True)
    return df[["nome","dim1","dim2"]]

def tipo_produto(nome: str) -> str:
    n = (nome or "").lower()
    if "fossa" in n: return "fossa"
    if "vertical" in n: return "vertical"
    if "horizontal" in n: return "horizontal"
    if "tc" in n and ("10.000" in n or "10000" in n): return "tc_ate_10k"
    return "auto"

def tamanho_peca_por_nome(nome: str, dim1: float, dim2: float) -> float:
    t = tipo_produto(nome)
    if t in ("fossa","vertical"): return float(dim1 or 0.0)
    if t in ("horizontal","tc_ate_10k"): return float(dim2 or 0.0)
    return float(max(float(dim1 or 0.0), float(dim2 or 0.0)))

def montar_catalogo_tamanho(df: pd.DataFrame) -> Dict[str, float]:
    mapa: Dict[str,float] = {}
    for _, r in df.iterrows():
        try:
            nome = limpar_texto(r["nome"])
            if not nome or nome.upper() in PALAVRAS_IGNORAR: continue
            tam = tamanho_peca_por_nome(nome, float(r["dim1"]), float(r["dim2"]))
            if tam > 0: mapa[nome] = tam
        except Exception:
            continue
    return mapa

# --------- Faixas CEP -> KM (robusto) ----------
COL_CEP_INI = ("cep_inicio","cep_inicial","cep ini","inicio","início","de","start")
COL_CEP_FIM = ("cep_fim","cep final","final","ate","até","to","end")
COL_KM = ("km","dist","distancia","distância","valor km","km faixa","km_faixa")

def detectar_colunas(df: pd.DataFrame) -> Tuple[Optional[int],Optional[int],Optional[int]]:
    def match(colname: str, candidatos: Tuple[str,...]) -> bool:
        s = limpar_texto(colname).lower()
        return any(c in s for c in candidatos)
    idx_ini = idx_fim = idx_km = None
    for i, c in enumerate(df.columns):
        if idx_ini is None and match(str(c), COL_CEP_INI): idx_ini = i
        if idx_fim is None and match(str(c), COL_CEP_FIM): idx_fim = i
        if idx_km  is None and match(str(c), COL_KM):     idx_km  = i
    return idx_ini, idx_fim, idx_km

def coletar_faixas_cep_km(xls: pd.ExcelFile) -> List[Tuple[str,str,float]]:
    faixas: List[Tuple[str,str,float]] = []

    def primeiro_km_valido(vals):
        for v in vals:
            if v is None: 
                continue
            s = str(v).strip().lower().replace(",", ".")
            if s in ("", "nan", "none", "null"):
                continue
            try:
                f = float(s)
                if math.isfinite(f) and f > 0:
                    return f
            except Exception:
                continue
        return None

    def extrai_ceps_de_texto(s: Any) -> Optional[Tuple[str,str]]:
        if not isinstance(s, str):
            return None
        # aceita formatos: "39300-000 → 39529-999", "39300-000 a 39529-999", etc.
        m = re.search(r"(\d{5}-?\d{3}).*?(\d{5}-?\d{3})", s)
        if m:
            a = so_digitos(m.group(1))
            b = so_digitos(m.group(2))
            if len(a)==8 and len(b)==8:
                return a,b
        return None

    for sheet in xls.sheet_names:
        try:
            df = pd.read_excel(xls, sheet, dtype=str)
        except Exception:
            continue
        if df.empty: 
            continue

        # 1) tenta colunas clássicas
        ini, fim, km = detectar_colunas(df)
        if None not in (ini, fim, km):
            sub = df.iloc[:, [ini, fim, km]]
            for _, row in sub.iterrows():
                a = so_digitos(row.iloc[0])
                b = so_digitos(row.iloc[1])
                kv = primeiro_km_valido([row.iloc[2]])
                if len(a)==8 and len(b)==8 and kv:
                    faixas.append((a, b, float(kv)))
            continue

        # 2) fallback: uma coluna texto com 2 CEPs + qualquer coluna numérica p/ KM
        for _, row in df.iterrows():
            vals = list(row.values)
            ab = None
            for v in vals:
                ab = extrai_ceps_de_texto(v)
                if ab:
                    break
            if not ab:
                continue
            kmv = primeiro_km_valido(vals)
            if not kmv:
                continue
            a, b = ab
            faixas.append((a, b, float(kmv)))

    # dedup + ordena
    uniq = {}
    for a, b, k in faixas:
        if len(a)==8 and len(b)==8 and math.isfinite(k) and k>0:
            uniq[(a, b)] = k
    out = [(a, b, k) for (a, b), k in uniq.items()]
    out.sort(key=lambda x: (x[0], x[1]))
    return out

def km_por_cep(faixas: List[Tuple[str,str,float]], cep_dest: str) -> Tuple[float, str]:
    """
    Retorna (km, fonte), onde fonte ∈ {"faixa","aprox_faixa","uf_fallback","default"}
    """
    d = so_digitos(cep_dest)
    if len(d) != 8:
        return DEFAULT_KM, "default"

    if faixas:
        n = int(d)
        lo, hi = 0, len(faixas)-1
        while lo <= hi:
            mid = (lo+hi)//2
            a, b, k = faixas[mid]
            na, nb = int(a), int(b)
            if n < na: hi = mid - 1
            elif n > nb: lo = mid + 1
            else: return float(k), "faixa"

        # não bateu: escolhe faixa mais próxima (bordas)
        cand = []
        if 0 <= hi < len(faixas):
            a,b,k = faixas[hi]
            dist = min(abs(int(a)-n), abs(int(b)-n))
            cand.append((dist, float(k)))
        if 0 <= lo < len(faixas):
            a,b,k = faixas[lo]
            dist = min(abs(int(a)-n), abs(int(b)-n))
            cand.append((dist, float(k)))
        if cand:
            cand.sort(key=lambda x: x[0])
            return cand[0][1], "aprox_faixa"

    uf = uf_por_cep(d)
    if uf and uf in KM_APROX_POR_UF:
        return float(KM_APROX_POR_UF[uf]), "uf_fallback"

    return DEFAULT_KM, "default"

# ==========================
# CARREGAMENTO INICIAL
# ==========================
def carregar_tudo() -> Dict[str, Any]:
    xls = pd.ExcelFile(ARQ_PLANILHA)
    consts = carregar_constantes(xls)
    cadastro = carregar_cadastro_produtos(xls)
    catalogo = montar_catalogo_tamanho(cadastro)
    faixas = coletar_faixas_cep_km(xls)
    return {"consts": consts, "catalogo": catalogo, "faixas": faixas}

try:
    DATA = carregar_tudo()
    print(f"[INIT] Constantes: {DATA['consts']}, Itens catálogo: {len(DATA['catalogo'])}, Faixas: {len(DATA['faixas'])}")
except Exception as e:
    DATA = {
        "consts": {"VALOR_KM": DEFAULT_VALOR_KM, "TAM_CAMINHAO": DEFAULT_TAM_CAMINHAO},
        "catalogo": {}, "faixas": []
    }
    print(f"[WARN] Falha ao carregar planilha: {e}")

# ==========================
# CÁLCULO
# ==========================
def calcula_valor_item(tamanho_peca_m: float, km: float, valor_km: float, tam_caminhao: float) -> float:
    ocupacao = max(0.01, float(tamanho_peca_m)/float(tam_caminhao))
    valor_km_item = float(valor_km) * ocupacao
    return round(valor_km_item * float(km), 2)

def parse_prods(prods_str: str) -> List[Dict[str, Any]]:
    itens: List[Dict[str, Any]] = []
    if not prods_str: return itens

    blocos = []
    for sep in ["/","|"]:
        if sep in prods_str:
            blocos = [b for b in prods_str.split(sep) if b.strip()]
            break
    if not blocos: blocos = [prods_str]

    def norm_num(x):
        if x is None: return 0.0
        s = str(x).strip().lower()
        if s in ("","null","none","nan"): return 0.0
        s = s.replace(",",".")
        try: return float(s)
        except Exception: return 0.0

    def cm_to_m(x):
        # heurística: Tray costuma mandar cm; se >10, interpretamos como cm
        return x/100.0 if x and x > 10 else x

    for raw in blocos:
        try:
            # comp;larg;alt;cub;qty;peso;codigo;valor   (código pode ser o "nome" também)
            comp, larg, alt, cub, qty, peso, codigo, valor = raw.split(";")
            comp_v = cm_to_m(norm_num(comp))
            larg_v = cm_to_m(norm_num(larg))
            alt_v  = cm_to_m(norm_num(alt))
            item = {
                "comp": comp_v,
                "larg": larg_v,
                "alt":  alt_v,
                "cub":  norm_num(cub),
                "qty":  int(norm_num(qty)) if norm_num(qty)>0 else 1,
                "peso": norm_num(peso),
                "codigo": (codigo or "").strip(),
                "valor": norm_num(valor),
            }
            itens.append(item)
        except Exception:
            continue
    return itens

# ==========================
# ENDPOINTS
# ==========================
@app.route("/health")
def health():
    return {
        "ok": True,
        "valores": DATA["consts"],
        "itens_catalogo": len(DATA["catalogo"]),
        "faixas_cep_km": len(DATA["faixas"])
    }

@app.route("/km")
def km_endpoint():
    cep_dest = request.args.get("cep_destino","")
    km, fonte = km_por_cep(DATA.get("faixas", []), cep_dest)
    return {"cep_destino": so_digitos(cep_dest), "km": km, "fonte": fonte, "faixas_carregadas": len(DATA.get("faixas", []))}

@app.route("/debug_faixas")
def debug_faixas():
    fxs = DATA.get("faixas", [])[:30]
    return {"amostra": [{"ini": a, "fim": b, "km": k} for a,b,k in fxs], "total_faixas": len(DATA.get("faixas", []))}

@app.route("/frete")
def frete():
    # token
    token = request.args.get("token","")
    if token != TOKEN_SECRETO:
        return Response("Token inválido", status=403)

    cep_origem = request.args.get("cep","")
    cep_dest = request.args.get("cep_destino","")
    prods = request.args.get("prods","")
    km_param = request.args.get("km","")  # opcional (teste)

    if not cep_dest or not prods:
        return Response("Parâmetros insuficientes", status=400)

    itens = parse_prods(prods)
    if not itens:
        return Response("Nenhum item válido em 'prods'", status=400)

    # constantes saneadas
    valor_km = DATA["consts"].get("VALOR_KM", DEFAULT_VALOR_KM)
    tam_caminhao = DATA["consts"].get("TAM_CAMINHAO", DEFAULT_TAM_CAMINHAO)
    if not isinstance(valor_km,(int,float)) or not math.isfinite(valor_km) or valor_km<=0:
        valor_km = DEFAULT_VALOR_KM
    if not isinstance(tam_caminhao,(int,float)) or not math.isfinite(tam_caminhao) or tam_caminhao<=0:
        tam_caminhao = DEFAULT_TAM_CAMINHAO

    # permite override via query (teste rápido)
    try:
        if request.args.get("valor_km"):
            valor_km = float(str(request.args["valor_km"]).replace(",", "."))
        if request.args.get("tam_caminhao"):
            tam_caminhao = float(str(request.args["tam_caminhao"]).replace(",", "."))
    except Exception:
        pass

    # KM: prioridade p/ parâmetro (teste); senão, CEP->faixa/UF/default
    km = None
    km_fonte = "default"
    if km_param:
        try:
            km = max(1.0, float(str(km_param).replace(",", ".")))
            km_fonte = "param"
        except Exception:
            km = None
    if km is None:
        km, km_fonte = km_por_cep(DATA.get("faixas", []), cep_dest)

    # soma dos itens (pelo maior lado de controle da peça)
    total = 0.0
    itens_xml = []
    for it in itens:
        nome = it["codigo"] or "Item"
        tam_catalogo = DATA["catalogo"].get(nome)
        if tam_catalogo is None:
            # se não está no catálogo, assume maior entre altura e largura informadas
            tam_catalogo = tamanho_peca_por_nome(nome, it["alt"], it["larg"])
        valor_item = calcula_valor_item(tam_catalogo, km, valor_km, tam_caminhao) * max(1, it["qty"])
        total += valor_item
        itens_xml.append(f"""
      <item>
        <codigo>{nome}</codigo>
        <tamanho_controle>{tam_catalogo:.3f}</tamanho_controle>
        <km>{km:.1f}</km>
        <valor>{valor_item:.2f}</valor>
      </item>""")

    debug_info = f"<debug km='{km}' fonte_km='{km_fonte}' valor_km='{valor_km}' tam_caminhao='{tam_caminhao}'/>"

    # ====== ÚNICO SERVIÇO ======
    xml = f"""<?xml version="1.0"?>
<cotacao>
  <resultado>
    <codigo>BAKOF</codigo>
    <transportadora>Bakof Log</transportadora>
    <servico>Transporte</servico>
    <transporte>TERRESTRE</transporte>
    <valor>{total:.2f}</valor>
    <prazo_min>4</prazo_min>
    <prazo_max>7</prazo_max>
    <entrega_domiciliar>1</entrega_domiciliar>
    <detalhes>{"".join(itens_xml)}
    </detalhes>
    {debug_info}
  </resultado>
</cotacao>"""
    return Response(xml, mimetype="application/xml")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT","8000")), debug=True)
