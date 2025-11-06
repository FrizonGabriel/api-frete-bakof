# app.py — FRETE com DISTÂNCIA REAL entre CEPs + Regras por Município + XML Tray + BUSCA DE ENDEREÇO
import os, math, re, time, requests, html
from typing import Dict, Any, List, Tuple, Optional
import pandas as pd
from flask import Flask, request, Response, make_response
from functools import lru_cache

# ==========================
# CONFIG
# ==========================
TOKEN_SECRETO = os.getenv("TOKEN_SECRETO", "teste123")
CEP_ORIGEM    = os.getenv("CEP_ORIGEM", "98400000")  # Frederico Westphalen/RS
ARQ_PLANILHA  = os.getenv("PLANILHA_FRETE", "tabela de frete atualizada(2)(Recuperado Automaticamente).xlsx")

DEFAULT_VALOR_KM     = float(os.getenv("DEFAULT_VALOR_KM", "7.0"))
DEFAULT_TAM_CAMINHAO = float(os.getenv("DEFAULT_TAM_CAMINHAO", "8.5"))
DEFAULT_KM           = float(os.getenv("DEFAULT_KM", "450.0"))

# (Opcional) Provedor com token próprio (pago/privado)
API_CEP_URL   = os.getenv("API_CEP_URL", "").strip()    # ex.: https://api.suaempresa.com/cep/{cep}
API_CEP_TOKEN = os.getenv("API_CEP_TOKEN", "").strip()  # ex.: Bearer xxxxx

PALAVRAS_IGNORAR = {
    "VALOR KM","TAMANHO CAMINHAO","TAMANHO CAMINHÃO",
    "CALCULO DE FRETE POR TAMANHO DE PEÇA","CÁLCULO DE FRETE POR TAMANHO DE PEÇA"
}

# Cache simples em memória
cache_coords: Dict[str, Tuple[float, float]] = {}
cache_cep_info: Dict[str, Dict[str, Any]] = {}  # mantém cidade/uf/localização
cache_endereco: Dict[str, Dict[str, Any]] = {}

app = Flask(__name__)

# ==========================
# UTILS / HELPERS
# ==========================
def limpar_cep(cep: str) -> str:
    """Remove formatação e retorna 8 dígitos"""
    return re.sub(r'\D', '', str(cep or ""))[:8].zfill(8)

def limpar_texto(nome: Any) -> str:
    if not isinstance(nome, str): return ""
    return " ".join(nome.replace("\n"," ").split()).strip()

def so_digitos(cep: Any) -> str:
    s = re.sub(r"\D","", str(cep or ""))
    return s[:8] if len(s) >= 8 else s.zfill(8)

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
    try: n = int(cep8)
    except: return None
    for uf, a, b in UF_CEP_RANGES:
        if int(a) <= n <= int(b): return uf
    return None

def extrai_numero_linha(row) -> Optional[float]:
    for v in row:
        if v is None or pd.isna(v): continue
        s = str(v).strip().upper()
        if s in ("", "NAN", "NONE", "NULL"): continue
        s = s.replace(",", ".")
        s = re.sub(r'(METROS?|KM|R\$|REAIS|/KM)', '', s, flags=re.IGNORECASE).strip()
        try:
            f = float(s)
            if math.isfinite(f) and f > 0: return f
        except: pass
    return None

def _request_json(url: str, timeout: int = 5, retries: int = 2, headers: Optional[dict]=None) -> Optional[dict]:
    headers = headers or {}
    for i in range(retries + 1):
        try:
            r = requests.get(url, timeout=timeout, headers=headers)
            if r.status_code == 200:
                return r.json()
        except Exception:
            time.sleep(0.25 * (i+1))
    return None

# ==========================
# BUSCA DE ENDEREÇO (GRÁTIS + OPCIONAL COM TOKEN)
# ==========================
@lru_cache(maxsize=4096)
def buscar_endereco(cep: str) -> Optional[Dict[str, Any]]:
    """
    Retorna dict padronizado:
    {
      'cep': '90020100',
      'logradouro': 'Rua X',
      'bairro': 'Centro',
      'cidade': 'Porto Alegre',
      'uf': 'RS'
    }
    Ordem de tentativas:
    1) Provedor próprio com token (se configurado via env)
    2) BrasilAPI v2 (grátis)
    3) ViaCEP (grátis)
    4) OpenCEP (grátis)
    """
    cep8 = limpar_cep(cep)
    if len(cep8) != 8:
        return None

    # Cache manual paralelo (evita reprocesso de normalização)
    if cep8 in cache_endereco:
        return cache_endereco[cep8]

    # 1) Provedor com token (opcional)
    if API_CEP_URL:
        try:
            url = API_CEP_URL.replace("{cep}", cep8)
            headers = {}
            if API_CEP_TOKEN:
                # Se for Bearer token:
                if not API_CEP_TOKEN.lower().startswith("bearer "):
                    headers["Authorization"] = f"Bearer {API_CEP_TOKEN}"
                else:
                    headers["Authorization"] = API_CEP_TOKEN
            data = _request_json(url, headers=headers)
            if isinstance(data, dict):
                # Tenta mapear chaves comuns
                logradouro = data.get("logradouro") or data.get("street") or data.get("endereco")
                bairro     = data.get("bairro") or data.get("district")
                cidade     = data.get("localidade") or data.get("cidade") or data.get("city")
                uf         = (data.get("uf") or data.get("state") or "").upper()
                if cidade and uf:
                    info = {
                        "cep": cep8,
                        "logradouro": (logradouro or "").strip(),
                        "bairro": (bairro or "").strip(),
                        "cidade": (cidade or "").strip(),
                        "uf": uf.strip(),
                    }
                    cache_endereco[cep8] = info
                    return info
        except Exception:
            pass

    # 2) BrasilAPI v2 (grátis)
    try:
        data = _request_json(f"https://brasilapi.com.br/api/cep/v2/{cep8}")
        if data and isinstance(data, dict) and data.get("city") and data.get("state"):
            info = {
                "cep": cep8,
                "logradouro": (data.get("street") or "").strip(),
                "bairro": (data.get("neighborhood") or "").strip(),
                "cidade": (data.get("city") or "").strip(),
                "uf": (data.get("state") or "").strip().upper()
            }
            cache_endereco[cep8] = info
            return info
    except Exception:
        pass

    # 3) ViaCEP (grátis)
    try:
        data = _request_json(f"https://viacep.com.br/ws/{cep8}/json/")
        if data and not data.get("erro"):
            info = {
                "cep": cep8,
                "logradouro": (data.get("logradouro") or "").strip(),
                "bairro": (data.get("bairro") or "").strip(),
                "cidade": (data.get("localidade") or "").strip(),
                "uf": (data.get("uf") or "").strip().upper()
            }
            cache_endereco[cep8] = info
            return info
    except Exception:
        pass

    # 4) OpenCEP (grátis)
    try:
        data = _request_json(f"https://opencep.com/v1/{cep8}.json")
        if data and (data.get("localidade") or data.get("cidade")) and data.get("uf"):
            info = {
                "cep": cep8,
                "logradouro": (data.get("logradouro") or "").strip(),
                "bairro": (data.get("bairro") or "").strip(),
                "cidade": (data.get("localidade") or data.get("cidade") or "").strip(),
                "uf": (data.get("uf") or "").strip().upper()
            }
            cache_endereco[cep8] = info
            return info
    except Exception:
        pass

    return None

# ==========================
# CEP / COORDENADAS
# ==========================
@lru_cache(maxsize=2048)
def buscar_info_cep(cep: str) -> Optional[Dict[str, Any]]:
    """
    Retorna dict com: { 'cep':..., 'uf':..., 'city':..., 'location': {'lat':..,'lon':..} }
    Tenta BrasilAPI v2 e OpenCEP; guarda em cache_cep_info também.
    """
    cep8 = limpar_cep(cep)
    if len(cep8) != 8:
        return None

    if cep8 in cache_cep_info:
        return cache_cep_info[cep8]

    # BrasilAPI v2
    data = _request_json(f"https://brasilapi.com.br/api/cep/v2/{cep8}", timeout=5, retries=2)
    if data and isinstance(data, dict):
        info = {
            "cep": cep8,
            "uf": data.get("state") or uf_por_cep(cep8),
            "city": data.get("city"),
            "location": None
        }
        loc = data.get("location", {})
        coords = loc.get("coordinates") if isinstance(loc, dict) else {}
        try:
            lat = float(coords.get("latitude"))
            lon = float(coords.get("longitude"))
            info["location"] = {"lat": lat, "lon": lon}
        except Exception:
            pass

        cache_cep_info[cep8] = info
        if info["location"]:
            cache_coords[cep8] = (info["location"]["lat"], info["location"]["lon"])
        return info

    # OpenCEP (fallback)
    data = _request_json(f"https://opencep.com/v1/{cep8}.json", timeout=5, retries=2)
    if data and isinstance(data, dict):
        info = {
            "cep": cep8,
            "uf": data.get("uf") or uf_por_cep(cep8),
            "city": data.get("localidade"),
            "location": None
        }
        try:
            lat = float(data.get("latitude"))
            lon = float(data.get("longitude"))
            info["location"] = {"lat": lat, "lon": lon}
            cache_coords[cep8] = (lat, lon)
        except Exception:
            pass

        cache_cep_info[cep8] = info
        return info

    info = {"cep": cep8, "uf": uf_por_cep(cep8), "city": None, "location": None}
    cache_cep_info[cep8] = info
    return info

@lru_cache(maxsize=1000)
def buscar_coordenadas(cep: str) -> Optional[Tuple[float, float]]:
    info = buscar_info_cep(cep)
    if info and info.get("location"):
        return (info["location"]["lat"], info["location"]["lon"])
    return None

def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2.0)**2 + math.cos(lat1_rad)*math.cos(lat2_rad)*math.sin(dlon/2.0)**2
    c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))
    return R * c

def calcular_distancia_ceps(cep_origem: str, cep_destino: str) -> Tuple[Optional[float], str]:
    coord_origem = buscar_coordenadas(cep_origem)
    coord_destino = buscar_coordenadas(cep_destino)
    if coord_origem and coord_destino:
        lat1, lon1 = coord_origem
        lat2, lon2 = coord_destino
        km = haversine(lat1, lon1, lat2, lon2)
        return (round(km, 1), "distancia_real")
    return (None, "erro_coordenadas")

# ==========================
# PLANILHA (constantes / produtos / regras)
# ==========================
def carregar_constantes(xls: pd.ExcelFile) -> Dict[str, float]:
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
                    if num and 3 <= num <= 50: valor_km = num
                if "TAMANHO" in texto and "CAMINH" in texto:
                    num = extrai_numero_linha(row)
                    if num and 3 <= num <= 20: tam_caminhao = num
        except: pass
    return {"VALOR_KM": valor_km, "TAM_CAMINHAO": tam_caminhao}

def carregar_cadastro_produtos(xls: pd.ExcelFile) -> pd.DataFrame:
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
        except: pass
    return pd.DataFrame(columns=["nome","dim1","dim2"])

def tipo_produto(nome: str) -> str:
    n = (nome or "").lower()
    if "fossa" in n: return "fossa"
    if "vertical" in n: return "vertical"
    if "horizontal" in n: return "horizontal"
    if "tc" in n and ("10.000" in n or "10000" in n or "10.0" in n): return "tc_ate_10k"
    return "auto"

def tamanho_peca_por_nome(nome: str, dim1: float, dim2: float) -> float:
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
        except: pass
    return mapa

def carregar_regras_municipio(xls: pd.ExcelFile) -> List[Dict[str, Any]]:
    """
    Lê aba REGRAS_MUNICIPIO (opcional) com colunas:
      Municipio | UF | Faixa_CEP_Inicio | Faixa_CEP_Fim | KM_Fixo | Multiplicador_ValorKM | Valor_Minimo | Acrescimo_Fixo
    """
    if "REGRAS_MUNICIPIO" not in xls.sheet_names:
        return []
    try:
        df = pd.read_excel(xls, "REGRAS_MUNICIPIO")
        cols = {c.strip().lower(): c for c in df.columns}
        def col(name): return cols.get(name.lower())

        regras = []
        for _, r in df.iterrows():
            reg = {
                "municipio": str(r.get(col("Municipio"), "") or "").strip(),
                "uf": str(r.get(col("UF"), "") or "").strip().upper() or None,
                "cep_ini": so_digitos(r.get(col("Faixa_CEP_Inicio")) or ""),
                "cep_fim": so_digitos(r.get(col("Faixa_CEP_Fim")) or ""),
                "km_fixo": float(str(r.get(col("KM_Fixo"), "")).replace(",", ".") or 0) if col("KM_Fixo") else 0.0,
                "mult_valor_km": float(str(r.get(col("Multiplicador_ValorKM"), "")).replace(",", ".") or 0) if col("Multiplicador_ValorKM") else 0.0,
                "valor_min": float(str(r.get(col("Valor_Minimo"), "")).replace(",", ".") or 0) if col("Valor_Minimo") else 0.0,
                "acrescimo_fixo": float(str(r.get(col("Acrescimo_Fixo"), "")).replace(",", ".") or 0) if col("Acrescimo_Fixo") else 0.0,
            }
            regras.append(reg)
        return regras
    except Exception as e:
        print(f"[WARN] Falha ao ler REGRAS_MUNICIPIO: {e}")
        return []

def regra_cobre_cep(reg: Dict[str, Any], cep8: str) -> bool:
    if reg.get("cep_ini") and reg.get("cep_fim"):
        try:
            n = int(cep8); a = int(reg["cep_ini"]); b = int(reg["cep_fim"])
            return a <= n <= b
        except: return False
    return False

def aplicar_regras_municipio(cep_destino: str, valor_km: float, km: float) -> Tuple[float, float, float]:
    cep8 = so_digitos(cep_destino)
    info = buscar_info_cep(cep8) or {}
    cidade = (info.get("city") or "").strip().upper()
    uf = (info.get("uf") or "").strip().upper()

    # 1) Prioridade por faixa de CEP
    for reg in DATA.get("regras_municipio", []):
        if regra_cobre_cep(reg, cep8):
            vk = valor_km
            k  = km
            if reg.get("km_fixo", 0) > 0: k = float(reg["km_fixo"])
            if reg.get("mult_valor_km", 0) > 0: vk = float(vk) * float(reg["mult_valor_km"])
            acres = float(reg.get("acrescimo_fixo", 0) or 0)
            return (vk, k, acres)

    # 2) Cidade/UF
    for reg in DATA.get("regras_municipio", []):
        muni = (reg.get("municipio") or "").strip().upper()
        uf_reg = (reg.get("uf") or "").strip().upper()
        if muni and muni == cidade and (not uf_reg or uf_reg == uf):
            vk = valor_km
            k  = km
            if reg.get("km_fixo", 0) > 0: k = float(reg["km_fixo"])
            if reg.get("mult_valor_km", 0) > 0: vk = float(vk) * float(reg["mult_valor_km"])
            acres = float(reg.get("acrescimo_fixo", 0) or 0)
            return (vk, k, acres)

    return (valor_km, km, 0.0)

# ==========================
# CARREGAMENTO GERAL
# ==========================
def carregar_tudo() -> Dict[str, Any]:
    try:
        xls = pd.ExcelFile(ARQ_PLANILHA)
    except Exception as e:
        print(f"[WARN] Não foi possível carregar planilha: {e}")
        return {
            "consts": {"VALOR_KM": DEFAULT_VALOR_KM, "TAM_CAMINHAO": DEFAULT_TAM_CAMINHAO},
            "catalogo": {},
            "regras_municipio": []
        }

    consts = carregar_constantes(xls)
    cadastro = carregar_cadastro_produtos(xls)
    catalogo = montar_catalogo_tamanho(cadastro)
    regras_mun = carregar_regras_municipio(xls)

    return {
        "consts": consts,
        "catalogo": catalogo,
        "regras_municipio": regras_mun
    }

DATA = carregar_tudo()

# ==========================
# CÁLCULO DE FRETE
# ==========================
def calcula_valor_item(tamanho_peca_m: float, km: float, valor_km: float, tam_caminhao: float) -> float:
    if tamanho_peca_m <= 0 or tam_caminhao <= 0: return 0.0
    ocupacao = float(tamanho_peca_m) / float(tam_caminhao)
    return round(float(valor_km) * float(km) * ocupacao, 2)

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
            while len(partes) < 8:
                partes.append("0")
            comp, larg, alt, cub, qty, peso, codigo, valor = partes[:8]
            item = {
                "comp": cm_to_m(norm_num(comp)),
                "larg": cm_to_m(norm_num(larg)),
                "alt": cm_to_m(norm_num(alt)),
                "cub": norm_num(cub),
                "qty": int(norm_num(qty)) if norm_num(qty) > 0 else 1,
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
# RESPOSTA XML
# ==========================
def _monta_xml_ok(total: float, itens_xml: List[str], debug_info: str) -> str:
    return f"""<?xml version="1.0" encoding="UTF-8"?>
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
</cotacao>""".strip()

def _monta_xml_erro(msg: str) -> str:
    msg = html.escape(msg or "Erro")
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<cotacao>
  <erro>{msg}</erro>
</cotacao>""".strip()

def _resp_xml(xml: str, status: int = 200) -> Response:
    resp = make_response(xml, status)
    resp.headers["Content-Type"] = "application/xml; charset=utf-8"
    resp.headers["Access-Control-Allow-Origin"] = "*"
    return resp

# ==========================
# ENDPOINTS
# ==========================
@app.route("/health")
def health():
    return {
        "ok": True,
        "cep_origem": CEP_ORIGEM,
        "valores": DATA["consts"],
        "itens_catalogo": len(DATA["catalogo"]),
        "regras_municipio": len(DATA.get("regras_municipio", [])),
        "cache_coordenadas": len(cache_coords),
    }

@app.route("/frete")
@app.route("/cotacao")
def frete():
    token = request.args.get("token", "")
    if token != TOKEN_SECRETO:
        return _resp_xml(_monta_xml_erro("Token inválido"), status=403)

    cep_origem_param = request.args.get("cep_origem", CEP_ORIGEM)
    cep_destino = request.args.get("cep_destino", "")
    prods = request.args.get("prods", "")

    if not cep_destino or not prods:
        return _resp_xml(_monta_xml_erro("Parâmetros insuficientes (cep_destino, prods)"), status=400)

    itens = parse_prods(prods)
    if not itens:
        return _resp_xml(_monta_xml_erro("Nenhum item válido em 'prods'"), status=400)

    valor_km = DATA["consts"].get("VALOR_KM", DEFAULT_VALOR_KM)
    tam_caminhao = DATA["consts"].get("TAM_CAMINHAO", DEFAULT_TAM_CAMINHAO)

    try:
        if request.args.get("valor_km"):
            valor_km = float(str(request.args["valor_km"]).replace(",", "."))
        if request.args.get("tam_caminhao"):
            tam_caminhao = float(str(request.args["tam_caminhao"]).replace(",", "."))
    except: pass

    km, km_fonte = calcular_distancia_ceps(cep_origem_param, cep_destino)
    if km is None:
        uf_dest = uf_por_cep(so_digitos(cep_destino))
        KM_APROX_POR_UF = {
            "RS":150,"SC":450,"PR":700,"SP":1100,"RJ":1500,"MG":1600,"ES":1800,
            "MS":1600,"MT":2200,"DF":2000,"GO":2100,"TO":2500,"BA":2600,"SE":2700,
            "AL":2800,"PE":3000,"PB":3100,"RN":3200,"CE":3400,"PI":3300,"MA":3500,
            "PA":3800,"AP":4100,"AM":4200,"RO":4000,"AC":4300,"RR":4500,
        }
        km = KM_APROX_POR_UF.get(uf_dest, DEFAULT_KM)
        km_fonte = f"uf_fallback_{uf_dest}" if uf_dest else "default"

    valor_km_aplic, km_aplic, acrescimo_fixo = aplicar_regras_municipio(cep_destino, valor_km, km)

    total = 0.0
    itens_xml = []
    for it in itens:
        codigo = it["codigo"] or "Item"
        tam_catalogo = DATA["catalogo"].get(codigo)
        if tam_catalogo is None:
            tam_catalogo = tamanho_peca_por_nome(codigo, it["alt"], it["larg"])
            if tam_catalogo == 0:
                tam_catalogo = max(it["comp"], it["larg"], it["alt"])

        v_unit = calcula_valor_item(tam_catalogo, km_aplic, valor_km_aplic, tam_caminhao)
        v_tot  = v_unit * max(1, it["qty"])
        total += v_tot

        itens_xml.append(f"""
      <item>
        <codigo>{html.escape(codigo)}</codigo>
        <quantidade>{it['qty']}</quantidade>
        <diametro_metros>{tam_catalogo:.3f}</diametro_metros>
        <km_distancia>{km_aplic:.1f}</km_distancia>
        <valor_unitario>{v_unit:.2f}</valor_unitario>
        <valor_total>{v_tot:.2f}</valor_total>
      </item>""")

    total += acrescimo_fixo

    def _valor_min_para_destino(cep):
        cep8 = so_digitos(cep)
        info = buscar_info_cep(cep8) or {}
        cidade = (info.get("city") or "").strip().upper()
        uf = (info.get("uf") or "").strip().upper()
        for reg in DATA.get("regras_municipio", []):
            if regra_cobre_cep(reg, cep8) and float(reg.get("valor_min", 0) or 0) > 0:
                return float(reg["valor_min"])
        for reg in DATA.get("regras_municipio", []):
            muni = (reg.get("municipio") or "").strip().upper()
            uf_reg = (reg.get("uf") or "").strip().upper()
            if muni and muni == cidade and (not uf_reg or uf_reg == uf):
                vm = float(reg.get("valor_min", 0) or 0)
                if vm > 0: return vm
        return 0.0

    valor_min = _valor_min_para_destino(cep_destino)
    if valor_min > 0 and total < valor_min:
        total = float(valor_min)

    debug_info = (f"<debug "
                  f"cep_origem='{html.escape(cep_origem_param)}' "
                  f"cep_destino='{html.escape(cep_destino)}' "
                  f"km='{km_aplic:.1f}' "
                  f"fonte_km='{html.escape(km_fonte)}' "
                  f"valor_km='{valor_km_aplic}' "
                  f"tam_caminhao='{tam_caminhao}' "
                  f"acrescimo_fixo='{acrescimo_fixo:.2f}' "
                  f"valor_min='{valor_min:.2f}' "
                  f"total_itens='{len(itens)}'/>")

    xml = _monta_xml_ok(total, itens_xml, debug_info)
    return _resp_xml(xml, status=200)

@app.route("/teste-distancia")
def teste_distancia():
    cep_origem = request.args.get("origem", CEP_ORIGEM)
    cep_destino = request.args.get("destino", "")
    if not cep_destino:
        return {"erro": "Informe o parâmetro 'destino'"}

    km, fonte = calcular_distancia_ceps(cep_origem, cep_destino)
    coord_origem = buscar_coordenadas(cep_origem)
    coord_destino = buscar_coordenadas(cep_destino)
    info_dest = buscar_info_cep(cep_destino) or {}

    return {
        "cep_origem": cep_origem,
        "cep_destino": cep_destino,
        "coordenadas_origem": coord_origem,
        "coordenadas_destino": coord_destino,
        "dest_city": info_dest.get("city"),
        "dest_uf": info_dest.get("uf"),
        "distancia_km": km,
        "fonte_calculo": fonte,
    }

# ===== NOVO: ENDPOINT DE ENDEREÇO (GRÁTIS + TOKEN DO SEU APP) =====
@app.route("/endereco")
def endereco():
    """
    Protegido por ?token=TOKEN_SECRETO
    Uso: GET /endereco?token=...&cep=90020100
    """
    token = request.args.get("token", "")
    if token != TOKEN_SECRETO:
        return {"erro": "Token inválido"}, 403

    cep = request.args.get("cep", "")
    cep8 = limpar_cep(cep)
    if len(cep8) != 8:
        return {"erro": "CEP inválido"}, 400

    info = buscar_endereco(cep8)
    if not info:
        return {"erro": "Endereço não encontrado"}, 404
    return info, 200

if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    print("🚀 Iniciando API de Frete (distância real + regras município + endereço)")
    print(f"📍 CEP Origem: {CEP_ORIGEM}")
    print(f"🔑 Token: {TOKEN_SECRETO}")
    print(f"📊 Produtos no catálogo: {len(DATA['catalogo'])}")
    print(f"🧭 Regras de município: {len(DATA.get('regras_municipio', []))}")
    app.run(host="0.0.0.0", port=port, debug=True)
