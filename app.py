# app.py — FRETE por Faixa de CEP + OVERRIDE por MUNICÍPIO (pronto p/ Tray)
import os, math, re
from typing import Dict, Any, List, Tuple, Optional
import pandas as pd
from flask import Flask, request, Response

# ==========================
# CONFIG
# ==========================
TOKEN_SECRETO = os.getenv("TOKEN_SECRETO", "teste123")
ARQ_PLANILHA  = os.getenv("PLANILHA_FRETE", "tabela de frete atualizada(2)(Recuperado Automaticamente).xlsx")

DEFAULT_VALOR_KM     = float(os.getenv("DEFAULT_VALOR_KM", "7.0"))
DEFAULT_TAM_CAMINHAO = float(os.getenv("DEFAULT_TAM_CAMINHAO", "8.5"))
DEFAULT_KM           = float(os.getenv("DEFAULT_KM", "450.0"))  # fallback mais realista

PALAVRAS_IGNORAR = {
    "VALOR KM","TAMANHO CAMINHAO","TAMANHO CAMINHÃO",
    "CALCULO DE FRETE POR TAMANHO DE PEÇA","CÁLCULO DE FRETE POR TAMANHO DE PEÇA"
}

# Estimativa por UF (guard-rail quando não há faixa)
KM_APROX_POR_UF = {
    "RS":150,"SC":450,"PR":700,"SP":1100,"RJ":1500,"MG":1600,"ES":1800,
    "MS":1600,"MT":2200,"DF":2000,"GO":2100,"TO":2500,"BA":2600,"SE":2700,
    "AL":2800,"PE":3000,"PB":3100,"RN":3200,"CE":3400,"PI":3300,"MA":3500,
    "PA":3800,"AP":4100,"AM":4200,"RO":4000,"AC":4300,"RR":4500,
}

UF_CEP_RANGES = [
    ("SP","01000000","19999999"),("RJ","20000000","28999999"),
    ("ES","29000000","29999999"),("MG","30000000","39999999"),
    ("BA","40000000","48999999"),("SE","49000000","49999999"),
    ("PE","50000000","56999999"),("AL","57000000","57999999"),
    ("PB","58000000","58999999"),("RN","59000000","59999999"),
    ("CE","60000000","63999999"),("PI","64000000","64999999"),
    ("MA","65000000","65999999"),("PA","66000000","68899999"),
    ("AP","68900000","68999999"),("AM","69000000","69899999"),
    ("RR","69300000","69399999"),("AC","69900000","69999999"),
    ("DF","70000000","73699999"),("GO","72800000","76799999"),
    ("TO","77000000","77999999"),("MT","78000000","78899999"),
    ("MS","79000000","79999999"),("PR","80000000","87999999"),
    ("SC","88000000","89999999"),("RS","90000000","99999999"),
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
        if int(a) <= n <= int(b):
            return uf
    return None

def extrai_numero_linha(row) -> Optional[float]:
    """Extrai primeiro número válido de uma linha (aceita '1.500 km', '100,00/km', etc)."""
    for v in row:
        if v is None or pd.isna(v): continue
        s = str(v).strip().upper()
        if s in ("", "NAN", "NONE", "NULL"): continue
        s = s.replace(",", ".")
        s = re.sub(r'(METROS?|KM|R\$|REAIS|/KM)', '', s, flags=re.IGNORECASE).strip()
        try:
            f = float(s)
            if math.isfinite(f) and f > 0:
                return f
        except Exception:
            continue
    return None

# ==========================
# LEITURA DA PLANILHA
# ==========================
def carregar_constantes(xls: pd.ExcelFile) -> Dict[str, float]:
    """Busca VALOR KM e TAMANHO CAMINHAO na aba BASE_CALCULO/D/BASE/CONSTANTES."""
    valor_km = DEFAULT_VALOR_KM
    tam_caminhao = DEFAULT_TAM_CAMINHAO
    for aba in ("BASE_CALCULO","D","BASE","CONSTANTES"):
        if aba not in xls.sheet_names: continue
        try:
            raw = pd.read_excel(xls, aba, header=None)
            for _, row in raw.iterrows():
                texto = " ".join([str(v).upper() for v in row if isinstance(v, str)])
                if "VALOR" in texto or "KM" in texto:
                    num = extrai_numero_linha(row)
                    if num and 3 <= num <= 50:  # range amplo de R$/km
                        valor_km = num
                if "TAMANHO" in texto and "CAMINH" in texto:
                    num = extrai_numero_linha(row)
                    if num and 3 <= num <= 20:  # metros úteis de baú/plataforma
                        tam_caminhao = num
        except Exception:
            continue
    return {"VALOR_KM": valor_km, "TAM_CAMINHAO": tam_caminhao}

def carregar_cadastro_produtos(xls: pd.ExcelFile) -> pd.DataFrame:
    """Carrega produtos da aba CADASTRO_PRODUTO e normaliza 2 dimensões."""
    for aba in ("CADASTRO_PRODUTO","CADASTRO","PRODUTOS"):
        if aba not in xls.sheet_names: continue
        try:
            raw = pd.read_excel(xls, aba, header=None)
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
        except Exception:
            continue
    return pd.DataFrame(columns=["nome","dim1","dim2"])

def tipo_produto(nome: str) -> str:
    n = (nome or "").lower()
    if "fossa" in n: return "fossa"
    if "vertical" in n: return "vertical"
    if "horizontal" in n: return "horizontal"
    if "tc" in n and ("10.000" in n or "10000" in n or "10.0" in n): return "tc_ate_10k"
    return "auto"

def tamanho_peca_por_nome(nome: str, dim1: float, dim2: float) -> float:
    """Regras: vertical/fossa → dim1; horizontal/TC<=10k → dim2; auto → maior lado."""
    t = tipo_produto(nome)
    if t in ("fossa","vertical"):  return float(dim1 or 0.0)
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

# --------- Faixas CEP -> KM ----------
def coletar_faixas_cep_km(xls: pd.ExcelFile) -> List[Tuple[str,str,float]]:
    """Lê trincas (CEP_INI, CEP_FIM, KM). Prioriza aba 'D'."""
    faixas: List[Tuple[str,str,float]] = []

    def extrai_cep_limpo(v) -> Optional[str]:
        if pd.isna(v): return None
        s = re.sub(r'[.\-\s]', '', str(v).strip())
        if len(s) == 8 and s.isdigit(): return s
        return None

    def extrai_km_val(v) -> Optional[float]:
        if pd.isna(v): return None
        m = re.search(r'[-+]?\d[\d\.\,]*', str(v))
        if not m: return None
        num = m.group(0).replace('.','').replace(',','.')
        try:
            f = float(num)
            if 10 <= f <= 5000: return f
        except: pass
        return None

    abas = ["D"] + [s for s in xls.sheet_names if s != "D"]
    for aba in abas:
        try:
            df = pd.read_excel(xls, aba, dtype=str, header=None)
            if df.empty: continue
            for i in range(len(df.columns) - 2):
                col_ini, col_fim, col_km = df.iloc[:, i], df.iloc[:, i+1], df.iloc[:, i+2]
                ok = False
                for idx in range(len(df)):
                    a = extrai_cep_limpo(col_ini.iloc[idx])
                    b = extrai_cep_limpo(col_fim.iloc[idx])
                    k = extrai_km_val(col_km.iloc[idx])
                    if a and b and k:
                        faixas.append((a, b, k)); ok = True
                if ok: break
            if len(faixas) > 10: break
        except: continue

    uniq = {}
    for a,b,k in faixas: uniq[(a,b)] = k
    out = [(a,b,k) for (a,b),k in uniq.items()]
    out.sort(key=lambda x:(x[0],x[1]))
    return out

def km_por_cep(faixas: List[Tuple[str,str,float]], cep_dest: str) -> Tuple[float, str]:
    d = so_digitos(cep_dest)
    if len(d) != 8: return DEFAULT_KM, "default"
    if faixas:
        n = int(d)
        for a,b,k in faixas:
            if int(a) <= n <= int(b): return float(k), "faixa"
    uf = uf_por_cep(d)
    if uf and uf in KM_APROX_POR_UF: return float(KM_APROX_POR_UF[uf]), "uf_fallback"
    return DEFAULT_KM, "default"

# --------- REGRAS POR MUNICÍPIO (NOVA) ----------
ALIASES_MUNI = {
    "uf": {"uf","estado"},
    "municipio": {"municipio","município","cidade"},
    "cep_ini": {"cep_ini","cep inicial","inicio","início","de","start"},
    "cep_fim": {"cep_fim","cep final","final","ate","até","to","end"},
    "km": {"km","dist","distancia","distância"},
    "valor_km": {"valor_km","valor km","vl_km","preco_km","preço_km"},
    "tam_caminhao": {"tamanho_caminhao","tamanho caminhao","tam_caminhao","tam caminhao","tam_caminhão"},
    "fator_mult": {"fator_mult","fator","multiplicador","ida_volta"},
    "pedagio": {"pedagio","pedágio","taxa pedagio","taxa pedágio"},
    "acrescimo_pct": {"acrescimo_pct","acréscimo_pct","acrescimo","acréscimo","percentual","%"},
    "min_frete": {"min_frete","minimo","mínimo","frete_min","valor_minimo"},
}

def _match_col(header: List[str], targets: set[str]) -> Optional[int]:
    for i, c in enumerate(header):
        n = limpar_texto(str(c)).lower()
        if any(t in n for t in targets): return i
    return None

def carregar_regras_municipio(xls: pd.ExcelFile) -> List[Dict[str, Any]]:
    """Lê aba REGRAS_MUNICIPIO: UF | MUNICIPIO | CEP_INI | CEP_FIM | KM | (opcionais: fatores)."""
    if "REGRAS_MUNICIPIO" not in xls.sheet_names: return []
    df = pd.read_excel(xls, "REGRAS_MUNICIPIO", header=0, dtype=str).fillna("")
    if df.empty: return []

    header = [str(x) for x in df.columns]
    idx = {k:_match_col(header, v) for k,v in ALIASES_MUNI.items()}

    regras = []
    for row in df.itertuples(index=False):
        vals = list(row)
        def get(col):
            j = idx[col]
            return (vals[j] if j is not None else "").strip()
        def get_num(col):
            s = get(col)
            m = re.search(r'[-+]?\d[\d\.\,]*', s)
            if not m: return None
            num = m.group(0).replace('.','').replace(',','.')
            try:
                f = float(num)
                return f if math.isfinite(f) else None
            except: return None

        cep_ini = so_digitos(get("cep_ini"))
        cep_fim = so_digitos(get("cep_fim"))
        if len(cep_ini)!=8 or len(cep_fim)!=8: continue

        regra = {
            "uf": limpar_texto(get("uf")).upper(),
            "municipio": limpar_texto(get("municipio")).upper(),
            "cep_ini": cep_ini,
            "cep_fim": cep_fim,
            "km": get_num("km"),
            "valor_km": get_num("valor_km"),
            "tam_caminhao": get_num("tam_caminhao"),
            "fator_mult": get_num("fator_mult"),
            "pedagio": get_num("pedagio"),
            "acrescimo_pct": get_num("acrescimo_pct"),
            "min_frete": get_num("min_frete"),
        }
        regras.append(regra)

    regras.sort(key=lambda r: (r["cep_ini"], r["cep_fim"]))
    return regras

def buscar_regra_municipio(regras_muni: List[Dict[str, Any]], cep_dest: str) -> Optional[Dict[str, Any]]:
    d = so_digitos(cep_dest)
    if len(d)!=8: return None
    n = int(d)
    for r in regras_muni:
        if int(r["cep_ini"]) <= n <= int(r["cep_fim"]):
            return r
    return None

# ==========================
# CARREGAMENTO INICIAL
# ==========================
def carregar_tudo() -> Dict[str, Any]:
    xls = pd.ExcelFile(ARQ_PLANILHA)
    consts     = carregar_constantes(xls)
    cadastro   = carregar_cadastro_produtos(xls)
    catalogo   = montar_catalogo_tamanho(cadastro)
    faixas     = coletar_faixas_cep_km(xls)
    regras_mun = carregar_regras_municipio(xls)
    return {"consts": consts, "catalogo": catalogo, "faixas": faixas, "regras_municipio": regras_mun}

try:
    DATA = carregar_tudo()
    print(f"[INIT] ✓ Constantes: {DATA['consts']}")
    print(f"[INIT] ✓ Produtos: {len(DATA['catalogo'])}")
    print(f"[INIT] ✓ Faixas CEP: {len(DATA['faixas'])}")
    print(f"[INIT] ✓ Regras Município: {len(DATA['regras_municipio'])}")
except Exception as e:
    DATA = {"consts":{"VALOR_KM":DEFAULT_VALOR_KM,"TAM_CAMINHAO":DEFAULT_TAM_CAMINHAO}, "catalogo":{}, "faixas":[], "regras_municipio":[]}
    print(f"[ERROR] Falha ao carregar planilha: {e}")

# ==========================
# CÁLCULO
# ==========================
def calcula_valor_item(tamanho_peca_m: float, km: float, valor_km: float, tam_caminhao: float) -> float:
    """Fórmula: (tamanho_peça / tam_caminhao) * valor_km * km"""
    if tamanho_peca_m <= 0 or tam_caminhao <= 0: return 0.0
    ocup = float(tamanho_peca_m) / float(tam_caminhao)
    return round(float(valor_km) * float(km) * ocup, 2)

def parse_prods(prods_str: str) -> List[Dict[str, Any]]:
    """prods = comp;larg;alt;cub;qty;peso;codigo;valor  (separados por '/' ou '|')"""
    itens: List[Dict[str, Any]] = []
    if not prods_str: return itens
    blocos = []
    for sep in ("/","|"):
        if sep in prods_str:
            blocos = [b for b in prods_str.split(sep) if b.strip()]
            break
    if not blocos: blocos = [prods_str]

    def norm_num(x):
        if x is None: return 0.0
        s = str(x).strip().lower()
        if s in ("","null","none","nan"): return 0.0
        s = s.replace(",", ".")
        try: return float(s)
        except: return 0.0

    def cm_to_m(x):
        if not x or x == 0: return 0.0
        return x/100.0 if x > 20 else x  # cm→m quando parece cm

    for raw in blocos:
        try:
            comp, larg, alt, cub, qty, peso, codigo, valor = raw.split(";")
            item = {
                "comp": cm_to_m(norm_num(comp)),
                "larg": cm_to_m(norm_num(larg)),
                "alt":  cm_to_m(norm_num(alt)),
                "cub":  norm_num(cub),
                "qty":  int(norm_num(qty)) if norm_num(qty)>0 else 1,
                "peso": norm_num(peso),
                "codigo": (codigo or "").strip(),
                "valor": norm_num(valor),
            }
            itens.append(item)
        except Exception as e:
            print(f"[WARN] Erro parse item: {raw} - {e}")
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
        "faixas_cep_km": len(DATA["faixas"]),
        "regras_municipio": len(DATA.get("regras_municipio", [])),
        "amostra_faixas": [{"ini": a, "fim": b, "km": k} for a,b,k in DATA["faixas"][:5]],
    }

@app.route("/km")
def km_endpoint():
    cep_dest = request.args.get("cep_destino","")
    km, fonte = km_por_cep(DATA.get("faixas", []), cep_dest)
    uf = uf_por_cep(so_digitos(cep_dest))
    return {"cep_destino": so_digitos(cep_dest), "uf": uf, "km": km, "fonte": fonte,
            "faixas_carregadas": len(DATA.get("faixas", []))}

@app.route("/debug_faixas")
def debug_faixas():
    fxs = DATA.get("faixas", [])[:30]
    return {"amostra":[{"ini":a,"fim":b,"km":k} for a,b,k in fxs], "total_faixas":len(DATA.get("faixas", []))}

@app.route("/frete")
def frete():
    # Token
    token = request.args.get("token","")
    if token != TOKEN_SECRETO:
        return Response("Token inválido", status=403)

    cep_dest = request.args.get("cep_destino","")
    prods    = request.args.get("prods","")
    km_param = request.args.get("km","")

    if not cep_dest or not prods:
        return Response("Parâmetros insuficientes", status=400)

    itens = parse_prods(prods)
    if not itens:
        return Response("Nenhum item válido em 'prods'", status=400)

    # Constantes base
    valor_km     = DATA["consts"].get("VALOR_KM", DEFAULT_VALOR_KM)
    tam_caminhao = DATA["consts"].get("TAM_CAMINHAO", DEFAULT_TAM_CAMINHAO)

    # Overrides por query (para testes)
    try:
        if request.args.get("valor_km"):
            valor_km = float(str(request.args["valor_km"]).replace(",", "."))
        if request.args.get("tam_caminhao"):
            tam_caminhao = float(str(request.args["tam_caminhao"]).replace(",", "."))
    except: pass

    # KM inicial
    km = None
    km_fonte = "default"

    # 0) km=? na URL → manda
    if km_param:
        try:
            km = max(1.0, float(str(km_param).replace(",", ".")))
            km_fonte = "param"
        except: km = None

    # 1) OVERRIDE por MUNICÍPIO (aba REGRAS_MUNICIPIO)
    muni_fator_mult = muni_pedagio = muni_acresc_pct = muni_min_frete = None
    reg_m = buscar_regra_municipio(DATA.get("regras_municipio", []), cep_dest)
    if km is None and reg_m:
        if reg_m.get("km"):
            km = float(reg_m["km"]); km_fonte = "municipio"
        if reg_m.get("valor_km"):     valor_km = float(reg_m["valor_km"])
        if reg_m.get("tam_caminhao"): tam_caminhao = float(reg_m["tam_caminhao"])
        muni_fator_mult = reg_m.get("fator_mult")
        muni_pedagio    = reg_m.get("pedagio")
        muni_acresc_pct = reg_m.get("acrescimo_pct")
        muni_min_frete  = reg_m.get("min_frete")

    # 2) Faixa de CEP → KM
    if km is None:
        km, km_fonte = km_por_cep(DATA.get("faixas", []), cep_dest)

    # Calcula por item
    total = 0.0
    itens_xml = []

    for it in itens:
        nome = it["codigo"] or "Item"
        tam_catalogo = DATA["catalogo"].get(nome)
        if tam_catalogo is None:
            # pegar das dimensões informadas
            # regra de produto pelo nome (vertical/fossa usa alt=dim1; horizontal/TC usa larg=dim2)
            tam_catalogo = tamanho_peca_por_nome(nome, it["alt"], it["larg"])
            if tam_catalogo == 0:
                tam_catalogo = max(it["comp"], it["larg"], it["alt"])

        valor_item = calcula_valor_item(tam_catalogo, km, valor_km, tam_caminhao)
        valor_total_item = valor_item * max(1, it["qty"])
        total += valor_total_item

        itens_xml.append(f"""
      <item>
        <codigo>{nome}</codigo>
        <tamanho_controle>{tam_catalogo:.3f}</tamanho_controle>
        <km>{km:.1f}</km>
        <valor>{valor_total_item:.2f}</valor>
      </item>""")

    # Ajustes por MUNICÍPIO (se definidos)
    if muni_fator_mult:
        total = total * float(muni_fator_mult)
    if muni_acresc_pct:
        total = total * (1.0 + float(muni_acresc_pct)/100.0)
    if muni_pedagio:
        total = total + float(muni_pedagio)
    if muni_min_frete and float(muni_min_frete) > 0:
        total = max(total, float(muni_min_frete))

    debug_info = (f"<debug km='{km:.1f}' fonte_km='{km_fonte}' valor_km='{valor_km}' tam_caminhao='{tam_caminhao}' "
                  f"muni='mult:{muni_fator_mult},ped:{muni_pedagio},acr:{muni_acresc_pct},min:{muni_min_frete}'/>")

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
