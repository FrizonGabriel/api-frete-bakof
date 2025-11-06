# app.py — API de Frete (Tray) com FAIXAS de KM (100 em 100 km)
import os
import math
import re
from typing import Dict, Any, List, Tuple, Optional
import pandas as pd
from flask import Flask, request, Response, jsonify

# ==========================
# CONFIGURAÇÕES
# ==========================
TOKEN_SECRETO = os.getenv("TOKEN_SECRETO", "teste123")
CEP_ORIGEM = os.getenv("CEP_ORIGEM", "98400000")
ARQ_PLANILHA = os.getenv("PLANILHA_FRETE", "tabela de frete atualizada(2)(Recuperado Automaticamente).xlsx")

DEFAULT_VALOR_KM = float(os.getenv("DEFAULT_VALOR_KM", "7.0"))
DEFAULT_TAM_CAMINHAO = float(os.getenv("DEFAULT_TAM_CAMINHAO", "8.5"))
DEFAULT_KM = float(os.getenv("DEFAULT_KM", "450.0"))
MIN_FRETE = float(os.getenv("MIN_FRETE", "120.0"))

# NOVO: controle de faixas
FAIXA_KM_LARGURA = int(os.getenv("FAIXA_KM_LARGURA", "100"))  # tamanho da faixa (padrão 100 km)
MODO_FAIXA = os.getenv("MODO_FAIXA", "arredondar").lower()    # "arredondar" | "tabela"
# Exemplo: "0-100:180;101-200:260;201-300:350"
TABELA_FAIXAS = os.getenv("TABELA_FAIXAS", "").strip()

PALAVRAS_IGNORAR = {
    "VALOR KM", "TAMANHO CAMINHAO", "TAMANHO CAMINHÃO",
    "CALCULO DE FRETE POR TAMANHO DE PEÇA", "CÁLCULO DE FRETE POR TAMANHO DE PEÇA"
}

app = Flask(__name__)

# ==========================
# TABELA DE COORDENADAS (trecho reduzido para exemplo)
# ==========================
COORDENADAS_MUNICIPIOS = {
    "FREDERICO WESTPHALEN-RS": (-27.3594, -53.3937),
    "PORTO ALEGRE-RS": (-30.0346, -51.2177),
    "CAXIAS DO SUL-RS": (-29.1634, -51.1797),
    "PELOTAS-RS": (-31.7654, -52.3376),
    "CANOAS-RS": (-29.9177, -51.1844),
    "SANTA MARIA-RS": (-29.6868, -53.8149),
    "PASSO FUNDO-RS": (-28.2620, -52.4083),
    "ERECHIM-RS": (-27.6336, -52.2736),
    "FLORIANOPOLIS-SC": (-27.5954, -48.5480),
    "JOINVILLE-SC": (-26.3045, -48.8487),
    "BLUMENAU-SC": (-26.9194, -49.0661),
    "CHAPECO-SC": (-27.0965, -52.6146),
    "CURITIBA-PR": (-25.4284, -49.2733),
    "LONDRINA-PR": (-23.3045, -51.1696),
    "MARINGA-PR": (-23.4205, -51.9333),
    "CASCAVEL-PR": (-24.9555, -53.4552),
    "FOZ DO IGUACU-PR": (-25.5163, -54.5854),
    "SAO PAULO-SP": (-23.5505, -46.6333),
    "GUARULHOS-SP": (-23.4538, -46.5333),
    "CAMPINAS-SP": (-22.9099, -47.0626),
    "SAO BERNARDO DO CAMPO-SP": (-23.6914, -46.5647),
    "SANTOS-SP": (-23.9608, -46.3336),
    "SAO JOSE DOS CAMPOS-SP": (-23.1791, -45.8872),
    "RIBEIRAO PRETO-SP": (-21.1767, -47.8103),
    "RIO DE JANEIRO-RJ": (-22.9068, -43.1729),
    "NITEROI-RJ": (-22.8839, -43.1039),
    "BELO HORIZONTE-MG": (-19.9167, -43.9345),
    "CONTAGEM-MG": (-19.9320, -44.0539),
    "BRASILIA-DF": (-15.8267, -47.9218),
}

FAIXAS_CEP_MUNICIPIO = [
    ("98400000", "98419999", "FREDERICO WESTPHALEN-RS"),
    ("90000000", "91999999", "PORTO ALEGRE-RS"),
    ("95000000", "95130999", "CAXIAS DO SUL-RS"),
    ("96000000", "96099999", "PELOTAS-RS"),
    ("92000000", "92999999", "CANOAS-RS"),
    ("97000000", "97119999", "SANTA MARIA-RS"),
    ("99000000", "99099999", "PASSO FUNDO-RS"),
    ("99700000", "99799999", "ERECHIM-RS"),
    ("88000000", "88099999", "FLORIANOPOLIS-SC"),
    ("89200000", "89239999", "JOINVILLE-SC"),
    ("89000000", "89099999", "BLUMENAU-SC"),
    ("89800000", "89879999", "CHAPECO-SC"),
    ("80000000", "82999999", "CURITIBA-PR"),
    ("86000000", "86199999", "LONDRINA-PR"),
    ("87000000", "87099999", "MARINGA-PR"),
    ("85800000", "85879999", "CASCAVEL-PR"),
    ("85850000", "85869999", "FOZ DO IGUACU-PR"),
    ("01000000", "05999999", "SAO PAULO-SP"),
    ("07000000", "07399999", "GUARULHOS-SP"),
    ("13000000", "13149999", "CAMPINAS-SP"),
    ("09700000", "09899999", "SAO BERNARDO DO CAMPO-SP"),
    ("11000000", "11999999", "SANTOS-SP"),
    ("12200000", "12249999", "SAO JOSE DOS CAMPOS-SP"),
    ("14000000", "14109999", "RIBEIRAO PRETO-SP"),
    ("20000000", "23799999", "RIO DE JANEIRO-RJ"),
    ("24000000", "24999999", "NITEROI-RJ"),
    ("30000000", "31999999", "BELO HORIZONTE-MG"),
    ("32000000", "32999999", "CONTAGEM-MG"),
    ("70000000", "72799999", "BRASILIA-DF"),
]

# ==========================
# UTILS
# ==========================
def limpar_cep(cep: str) -> str:
    s = re.sub(r'\D', '', str(cep or ""))
    return s[:8].zfill(8) if s else "00000000"

def uf_por_cep(cep8: str) -> Optional[str]:
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
    try:
        n = int(cep8)
    except:
        return None
    for uf, a, b in UF_CEP_RANGES:
        if int(a) <= n <= int(b):
            return uf
    return None

def buscar_municipio_por_cep(cep: str) -> Optional[str]:
    cep_limpo = limpar_cep(cep)
    cep_num = int(cep_limpo)
    for inicio, fim, municipio in FAIXAS_CEP_MUNICIPIO:
        if int(inicio) <= cep_num <= int(fim):
            return municipio
    uf = uf_por_cep(cep_limpo)
    if uf:
        capitais = {
            "RS": "PORTO ALEGRE-RS","SC": "FLORIANOPOLIS-SC","PR": "CURITIBA-PR",
            "SP": "SAO PAULO-SP","RJ": "RIO DE JANEIRO-RJ","MG": "BELO HORIZONTE-MG",
            "DF": "BRASILIA-DF",
        }
        return capitais.get(uf)
    return None

def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return R * c

# ==========================
# FAIXAS DE KM (NOVO)
# ==========================
def arredonda_para_faixa_km(km: float, largura: int = FAIXA_KM_LARGURA) -> Tuple[int, int, int]:
    """Retorna (km_faixa_topo, faixa_ini_inclusivo, faixa_fim_inclusivo). Ex.: 138 -> (200, 101, 200)"""
    if km <= 0: return (largura, 0 if largura == 100 else 1, largura)
    blocos = math.ceil(km / largura)
    topo = blocos * largura
    ini = 0 if topo == largura else (topo - largura + 1)
    fim = topo
    return (topo, ini, fim)

def parse_tabela_faixas(cfg: str) -> List[Tuple[int,int,float]]:
    """
    Converte "0-100:180;101-200:260" -> [(0,100,180.0),(101,200,260.0)]
    """
    out: List[Tuple[int,int,float]] = []
    if not cfg: return out
    for bloco in cfg.split(";"):
        bloco = bloco.strip()
        if not bloco: continue
        try:
            rng, preco = bloco.split(":")
            a, b = rng.split("-")
            a = int(a.strip()); b = int(b.strip())
            preco = float(str(preco).replace(",", ".").strip())
            if a >= 0 and b >= a and preco >= 0:
                out.append((a, b, preco))
        except:  # silencioso
            continue
    return sorted(out, key=lambda x: (x[0], x[1]))

TABELA_FAIXAS_PARS = parse_tabela_faixas(TABELA_FAIXAS)

def preco_por_tabela_faixas(km: float) -> Optional[Tuple[float, Tuple[int,int]]]:
    """
    Se MODO_FAIXA="tabela" e a faixa existir em TABELA_FAIXAS, retorna (preco, (ini,fim)).
    """
    if MODO_FAIXA != "tabela" or not TABELA_FAIXAS_PARS:
        return None
    k = int(math.ceil(km))
    for a, b, preco in TABELA_FAIXAS_PARS:
        if a <= k <= b:
            return (preco, (a, b))
    return None

# ==========================
# PLANILHA (igual à versão anterior, reduzida)
# ==========================
def limpar_texto(nome: Any) -> str:
    if not isinstance(nome, str): return ""
    return " ".join(nome.replace("\n"," ").split()).strip()

def carregar_constantes(xls: pd.ExcelFile) -> Dict[str, float]:
    valor_km = DEFAULT_VALOR_KM
    tam_caminhao = DEFAULT_TAM_CAMINHAO
    for aba in ("BASE_CALCULO", "D", "BASE", "CONSTANTES"):
        if aba not in xls.sheet_names: continue
        try:
            raw = pd.read_excel(xls, aba, header=None)
            for _, row in raw.iterrows():
                texto = " ".join([str(v).upper() for v in row if isinstance(v, str)])
                if "VALOR" in texto or "KM" in texto:
                    # pega primeiro número que pareça "valor por km"
                    for v in row:
                        try:
                            f = float(str(v).replace(",", "."))
                            if 3 <= f <= 50: valor_km = f
                        except: pass
                if "TAMANHO" in texto and "CAMINH" in texto:
                    for v in row:
                        try:
                            f = float(str(v).replace(",", "."))
                            if 3 <= f <= 20: tam_caminhao = f
                        except: pass
        except Exception as e:
            print(f"[WARN] Erro ao ler aba {aba}: {e}")
    return {"VALOR_KM": valor_km, "TAM_CAMINHAO": tam_caminhao}

def carregar_cadastro_produtos(xls: pd.ExcelFile) -> pd.DataFrame:
    for aba in ("CADASTRO_PRODUTO", "CADASTRO", "PRODUTOS"):
        if aba not in xls.sheet_names: continue
        try:
            raw = pd.read_excel(xls, aba, header=None)
            nome_col = 2 if raw.shape[1] > 2 else 0
            dim1_col = 3 if raw.shape[1] > 3 else (1 if raw.shape[1] > 1 else 0)
            dim2_col = 4 if raw.shape[1] > 4 else (2 if raw.shape[1] > 2 else 1)
            df = raw[[nome_col, dim1_col, dim2_col]].copy()
            df.columns = ["nome", "dim1", "dim2"]
            df["nome"] = df["nome"].apply(limpar_texto)
            df = df[df["nome"].astype(str).str.len() > 0].drop_duplicates(subset=["nome"])
            df["dim1"] = pd.to_numeric(df["dim1"], errors="coerce").fillna(0.0)
            df["dim2"] = pd.to_numeric(df["dim2"], errors="coerce").fillna(0.0)
            return df[["nome", "dim1", "dim2"]]
        except Exception as e:
            print(f"[WARN] Erro ao ler aba {aba}: {e}")
    return pd.DataFrame(columns=["nome", "dim1", "dim2"])

def tipo_produto(nome: str) -> str:
    n = (nome or "").lower()
    if "fossa" in n: return "fossa"
    if "vertical" in n: return "vertical"
    if "horizontal" in n: return "horizontal"
    if "tc" in n and ("10.000" in n or "10000" in n or "10.0" in n): return "tc_ate_10k"
    return "auto"

def tamanho_peca_por_nome(nome: str, dim1: float, dim2: float) -> float:
    t = tipo_produto(nome)
    if t in ("fossa", "vertical"):  return float(dim1 or 0.0)
    if t in ("horizontal", "tc_ate_10k"): return float(dim2 or 0.0)
    return float(max(float(dim1 or 0.0), float(dim2 or 0.0)))

def montar_catalogo_tamanho(df: pd.DataFrame) -> Dict[str, float]:
    mapa: Dict[str, float] = {}
    for _, r in df.iterrows():
        try:
            nome = limpar_texto(r["nome"])
            tam = tamanho_peca_por_nome(nome, float(r["dim1"]), float(r["dim2"]))
            if tam > 0: mapa[nome] = tam
        except: pass
    return mapa

def carregar_tudo() -> Dict[str, Any]:
    try:
        xls = pd.ExcelFile(ARQ_PLANILHA)
        consts = carregar_constantes(xls)
        cadastro = carregar_cadastro_produtos(xls)
        catalogo = montar_catalogo_tamanho(cadastro)
        print(f"[OK] Planilha carregada: {len(catalogo)} produtos")
        return {"consts": consts, "catalogo": catalogo}
    except Exception as e:
        print(f"[WARN] Planilha não carregada: {e}")
        return {"consts": {"VALOR_KM": DEFAULT_VALOR_KM, "TAM_CAMINHAO": DEFAULT_TAM_CAMINHAO}, "catalogo": {}}

DATA = carregar_tudo()

# ==========================
# CÁLCULO DE FRETE
# ==========================
def calcula_valor_item(tamanho_peca_m: float, km_ref: float, valor_km: float, tam_caminhao: float) -> float:
    if tamanho_peca_m <= 0 or tam_caminhao <= 0: return 0.0
    ocupacao = float(tamanho_peca_m) / float(tam_caminhao)
    return round(valor_km * km_ref * ocupacao, 2)

def parse_prods(prods_str: str) -> List[Dict[str, Any]]:
    itens: List[Dict[str, Any]] = []
    if not prods_str: return itens
    blocos = []
    for sep in ("/", "|"):
        if sep in prods_str:
            blocos = [b for b in prods_str.split(sep) if b.strip()]
            break
    if not blocos: blocos = [prods_str]

    def norm_num(x):
        if x is None: return 0.0
        s = str(x).strip().lower()
        if s in ("", "null", "none", "nan"): return 0.0
        s = s.replace(",", ".")
        try: return float(s)
        except: return 0.0

    def cm_to_m(x):
        if not x or x == 0: return 0.0
        return x/100.0 if x > 20 else x

    for raw in blocos:
        try:
            partes = raw.split(";")
            if len(partes) < 8: continue
            comp, larg, alt, cub, qty, peso, codigo, valor = partes[:8]
            itens.append({
                "comp": cm_to_m(norm_num(comp)),
                "larg": cm_to_m(norm_num(larg)),
                "alt": cm_to_m(norm_num(alt)),
                "cub": norm_num(cub),
                "qty": int(norm_num(qty)) if norm_num(qty) > 0 else 1,
                "peso": norm_num(peso),
                "codigo": (codigo or "").strip(),
                "valor": norm_num(valor),
            })
        except Exception as e:
            print(f"[WARN] Erro parse item: {raw} - {e}")
    return itens

def calcular_distancia_real(cep_origem: str, cep_destino: str) -> Tuple[Optional[float], str, Dict[str, Any]]:
    detalhes = {"cep_origem": cep_origem, "cep_destino": cep_destino,
                "municipio_origem": None, "municipio_destino": None}
    muni_origem = buscar_municipio_por_cep(cep_origem)
    muni_destino = buscar_municipio_por_cep(cep_destino)
    detalhes["municipio_origem"] = muni_origem
    detalhes["municipio_destino"] = muni_destino
    if not muni_origem or not muni_destino:
        return (None, "municipio_nao_encontrado", detalhes)
    if muni_origem == muni_destino:
        return (10.0, "mesmo_municipio", detalhes)
    c1 = COORDENADAS_MUNICIPIOS.get(muni_origem)
    c2 = COORDENADAS_MUNICIPIOS.get(muni_destino)
    if not c1 or not c2:
        return (None, "coordenadas_nao_encontradas", detalhes)
    km = haversine(c1[0], c1[1], c2[0], c2[1]) * 1.15
    km = round(km / 5) * 5
    km = max(10.0, km)
    return (km, "distancia_calculada", detalhes)

def calcular_prazo(km: float) -> int:
    if km <= 100: return 2
    if km <= 250: return 3
    if km <= 500: return 5
    if km <= 900: return 7
    if km <= 1400: return 10
    return 15

def money(v: float) -> str:
    return f"{v:.2f}"

def build_xml_ok(servico: str, codigo: str, valor: float, prazo: int, obs: str = "") -> str:
    obs = (obs or "").replace("&", "e")
    return f"""<?xml version="1.0" encoding="utf-8"?>
<frete>
  <servicos>
    <servico>
      <nome>{servico}</nome>
      <codigo>{codigo}</codigo>
      <valor>{money(valor)}</valor>
      <prazo>{prazo}</prazo>
      <erro>0</erro>
      <msg_erro></msg_erro>
      <obs>{obs}</obs>
    </servico>
  </servicos>
</frete>"""

def build_xml_erro(msg: str) -> str:
    msg = (msg or "Erro").replace("&", "e")
    return f"""<?xml version="1.0" encoding="utf-8"?>
<frete>
  <servicos>
    <servico>
      <nome>INDISPONIVEL</nome>
      <codigo>000</codigo>
      <valor>0.00</valor>
      <prazo>0</prazo>
      <erro>1</erro>
      <msg_erro>{msg}</msg_erro>
      <obs></obs>
    </servico>
  </servicos>
</frete>"""

# ==========================
# ENDPOINTS
# ==========================
@app.route("/")
def index():
    return {
        "api": "Bakof Frete",
        "versao": "3.3 - Faixas de KM (Tray)",
        "faixa_km_largura": FAIXA_KM_LARGURA,
        "modo_faixa": MODO_FAIXA,
        "tem_tabela_faixas": bool(TABELA_FAIXAS_PARS),
        "endpoints": {"/health": "Status", "/frete": "Cotação", "/teste-distancia": "Teste KM"}
    }

@app.route("/health")
def health():
    return {"ok": True, "cep_origem": CEP_ORIGEM, "valores": DATA["consts"]}

@app.route("/teste-distancia")
def teste_distancia():
    a = request.args.get("a", CEP_ORIGEM)
    b = request.args.get("b", "")
    km, fonte, det = calcular_distancia_real(a, b)
    (topo, ini, fim) = arredonda_para_faixa_km(km or 0.0)
    return jsonify({"km": km, "km_faixa_topo": topo, "faixa_ini": ini, "faixa_fim": fim, "fonte": fonte, "detalhes": det})

@app.route("/frete", methods=["GET", "POST"])
def frete():
    try:
        params = request.form.to_dict() if request.method == "POST" else request.args.to_dict()
        token = params.get("token", "")
        if token != TOKEN_SECRETO:
            return Response(build_xml_erro("Token inválido"), status=403, mimetype="text/xml; charset=utf-8")

        cep_origem_param = params.get("cep_origem", CEP_ORIGEM)
        cep_destino = params.get("cep_destino") or params.get("cep") or ""
        prods = params.get("prods", "")
        retornar_json = str(params.get("retorna", "")).lower() == "json"

        if not cep_destino:
            return Response(build_xml_erro("CEP destino não informado"), status=400, mimetype="text/xml; charset=utf-8")
        if not prods and MODO_FAIXA != "tabela":
            # no modo tabela, pode não ter itens (preço fechado por faixa)
            return Response(build_xml_erro("Produtos não informados"), status=400, mimetype="text/xml; charset=utf-8")

        # Distância real
        km, km_fonte, detalhes = calcular_distancia_real(cep_origem_param, cep_destino)
        if km is None:
            uf_dest = uf_por_cep(limpar_cep(cep_destino))
            KM_APROX_POR_UF = {"RS":150,"SC":450,"PR":700,"SP":1100,"RJ":1500,"MG":1600,"ES":1800,"MS":1600,
                               "MT":2200,"DF":2000,"GO":2100,"TO":2500,"BA":2600,"SE":2700,"AL":2800,"PE":3000,
                               "PB":3100,"RN":3200,"CE":3400,"PI":3300,"MA":3500,"PA":3800,"AP":4100,"AM":4200,
                               "RO":4000,"AC":4300,"RR":4500}
            km = KM_APROX_POR_UF.get(uf_dest, DEFAULT_KM)
            km_fonte = f"uf_fallback_{uf_dest}" if uf_dest else "default"

        # FAIXA de KM
        km_faixa_topo, faixa_ini, faixa_fim = arredonda_para_faixa_km(km, FAIXA_KM_LARGURA)

        # MODO "tabela": aplica preço fixo por faixa
        tabela_hit = preco_por_tabela_faixas(km)
        if tabela_hit:
            preco_faixa, (fa_ini, fa_fim) = tabela_hit
            total = max(MIN_FRETE, float(preco_faixa))
            prazo = calcular_prazo(km_faixa_topo)
            obs = f"modo=tabela; faixa={fa_ini}-{fa_fim}km; km_real={km} (fonte={km_fonte})"
            xml = build_xml_ok("BK-EXPRESSO", "BKX", total, prazo, obs)
            return jsonify({"ok": True, "valor": total, "prazo": prazo, "faixa": [fa_ini, fa_fim],
                            "km_real": km, "km_faixa_topo": km_faixa_topo}) if retornar_json \
                   else Response(xml, status=200, mimetype="text/xml; charset=utf-8")

        # MODO "arredondar": mantém fórmula, mas usa km da FAIXA
        valor_km = DATA["consts"].get("VALOR_KM", DEFAULT_VALOR_KM)
        tam_caminhao = DATA["consts"].get("TAM_CAMINHAO", DEFAULT_TAM_CAMINHAO)
        try:
            if params.get("valor_km"): valor_km = float(str(params["valor_km"]).replace(",", "."))
            if params.get("tam_caminhao"): tam_caminhao = float(str(params["tam_caminhao"]).replace(",", "."))
        except: pass

        itens = parse_prods(prods) if prods else []
        total = 0.0
        if not itens:
            total = 0.0  # se cair aqui sem itens, MIN_FRETE cobre
        else:
            for idx, it in enumerate(itens):
                codigo = it["codigo"] or f"Item{idx+1}"
                # tenta usar dimensões informadas (alt/larg) como "tamanho"
                tam_ref = tamanho_peca_por_nome(codigo, it["alt"], it["larg"])
                tam_ref = max(tam_ref, float(it["cub"] or 0.0))  # se tiver m³, usa o maior (aprox. ocupação)
                total += calcula_valor_item(tam_ref, km_faixa_topo, valor_km, tam_caminhao) * max(1, int(it["qty"]))

        total = max(total, MIN_FRETE)
        prazo = calcular_prazo(km_faixa_topo)
        obs = f"modo=arredondar; faixa={faixa_ini}-{faixa_fim}km; km_real={km} (fonte={km_fonte}); km_faixa={km_faixa_topo}"
        xml = build_xml_ok("BK-EXPRESSO", "BKX", total, prazo, obs)

        return jsonify({
            "ok": True, "valor": total, "prazo": prazo,
            "km_real": km, "km_faixa_topo": km_faixa_topo, "faixa": [faixa_ini, faixa_fim],
            "valor_km": valor_km, "tam_caminhao": tam_caminhao
        }) if retornar_json else Response(xml, status=200, mimetype="text/xml; charset=utf-8")

    except Exception as e:
        print(f"[FATAL] {e}")
        return Response(build_xml_erro(f"Falha interna: {e}"), status=500, mimetype="text/xml; charset=utf-8")

# ==========================
# MAIN
# ==========================
if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    host = os.getenv("HOST", "0.0.0.0")
    app.run(host=host, port=port)
