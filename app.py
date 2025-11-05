# app.py — FRETE com DISTÂNCIA REAL município a município
import os, math, re, requests, time
from typing import Dict, Any, List, Tuple, Optional
import pandas as pd
from flask import Flask, request, Response
from functools import lru_cache

# ==========================
# CONFIG
# ==========================
TOKEN_SECRETO = os.getenv("TOKEN_SECRETO", "teste123")
CEP_ORIGEM = os.getenv("CEP_ORIGEM", "98400000")  # Frederico Westphalen/RS
ARQ_PLANILHA = os.getenv("PLANILHA_FRETE", "tabela de frete atualizada(2)(Recuperado Automaticamente).xlsx")

DEFAULT_VALOR_KM = float(os.getenv("DEFAULT_VALOR_KM", "7.0"))
DEFAULT_TAM_CAMINHAO = float(os.getenv("DEFAULT_TAM_CAMINHAO", "8.5"))
DEFAULT_KM = float(os.getenv("DEFAULT_KM", "450.0"))

PALAVRAS_IGNORAR = {
    "VALOR KM","TAMANHO CAMINHAO","TAMANHO CAMINHÃO",
    "CALCULO DE FRETE POR TAMANHO DE PEÇA","CÁLCULO DE FRETE POR TAMANHO DE PEÇA"
}

# Cache em memória
cache_municipios = {}  # CEP -> (municipio, uf, lat, lon)
cache_coordenadas_municipio = {}  # "MUNICIPIO-UF" -> (lat, lon)

app = Flask(__name__)

# ==========================
# BUSCA DE MUNICÍPIO E COORDENADAS
# ==========================
def limpar_cep(cep: str) -> str:
    """Remove formatação e retorna 8 dígitos"""
    return re.sub(r'\D', '', str(cep or ""))[:8].zfill(8)

def normalizar_municipio(nome: str) -> str:
    """Normaliza nome do município (remove acentos, maiúsculas)"""
    import unicodedata
    if not nome:
        return ""
    # Remove acentos
    nfkd = unicodedata.normalize('NFKD', nome)
    sem_acento = "".join([c for c in nfkd if not unicodedata.combining(c)])
    return sem_acento.upper().strip()

@lru_cache(maxsize=2000)
def buscar_municipio_por_cep(cep: str) -> Optional[Tuple[str, str]]:
    """
    Busca município e UF a partir do CEP
    Retorna: (municipio, uf) ou None
    """
    cep_limpo = limpar_cep(cep)
    
    if len(cep_limpo) != 8:
        return None
    
    # Verifica cache
    if cep_limpo in cache_municipios:
        m, u, _, _ = cache_municipios[cep_limpo]
        return (m, u)
    
    # Tenta ViaCEP (mais confiável para município)
    try:
        url = f"https://viacep.com.br/ws/{cep_limpo}/json/"
        resp = requests.get(url, timeout=5)
        if resp.status_code == 200:
            data = resp.json()
            if not data.get("erro"):
                municipio = normalizar_municipio(data.get("localidade", ""))
                uf = data.get("uf", "").upper()
                if municipio and uf:
                    return (municipio, uf)
    except Exception as e:
        print(f"[WARN] Erro ViaCEP para {cep_limpo}: {e}")
    
    # Fallback: BrasilAPI
    try:
        url = f"https://brasilapi.com.br/api/cep/v2/{cep_limpo}"
        resp = requests.get(url, timeout=5)
        if resp.status_code == 200:
            data = resp.json()
            municipio = normalizar_municipio(data.get("city", ""))
            uf = data.get("state", "").upper()
            if municipio and uf:
                return (municipio, uf)
    except Exception as e:
        print(f"[WARN] Erro BrasilAPI para {cep_limpo}: {e}")
    
    return None

@lru_cache(maxsize=1000)
def buscar_coordenadas_municipio(municipio: str, uf: str) -> Optional[Tuple[float, float]]:
    """
    Busca coordenadas do centro geográfico do município
    Retorna: (latitude, longitude) ou None
    """
    chave = f"{municipio}-{uf}"
    
    # Verifica cache
    if chave in cache_coordenadas_municipio:
        return cache_coordenadas_municipio[chave]
    
    # Método 1: API do IBGE (mais preciso)
    try:
        # Busca código do município
        url = f"https://servicodados.ibge.gov.br/api/v1/localidades/estados/{uf}/municipios"
        resp = requests.get(url, timeout=8)
        if resp.status_code == 200:
            municipios = resp.json()
            for m in municipios:
                nome_ibge = normalizar_municipio(m.get("nome", ""))
                if nome_ibge == municipio:
                    # Pega coordenadas do município
                    codigo_ibge = m.get("id")
                    # Usa API de malhas do IBGE para coordenadas aproximadas
                    # Como alternativa, usa coordenadas médias conhecidas
                    
                    # Fallback: busca em base local conhecida
                    coord = _coordenadas_conhecidas(municipio, uf)
                    if coord:
                        cache_coordenadas_municipio[chave] = coord
                        return coord
                    break
    except Exception as e:
        print(f"[WARN] Erro IBGE para {municipio}/{uf}: {e}")
    
    # Método 2: Nominatim OpenStreetMap
    try:
        query = f"{municipio}, {uf}, Brasil"
        url = f"https://nominatim.openstreetmap.org/search"
        params = {
            "q": query,
            "format": "json",
            "limit": 1
        }
        headers = {"User-Agent": "BakofFreteAPI/1.0"}
        resp = requests.get(url, params=params, headers=headers, timeout=8)
        time.sleep(1)  # Respeita rate limit do Nominatim
        
        if resp.status_code == 200:
            data = resp.json()
            if data and len(data) > 0:
                lat = float(data[0]["lat"])
                lon = float(data[0]["lon"])
                cache_coordenadas_municipio[chave] = (lat, lon)
                return (lat, lon)
    except Exception as e:
        print(f"[WARN] Erro Nominatim para {municipio}/{uf}: {e}")
    
    # Método 3: Base local conhecida (capitais e principais cidades)
    coord = _coordenadas_conhecidas(municipio, uf)
    if coord:
        cache_coordenadas_municipio[chave] = coord
        return coord
    
    return None

def _coordenadas_conhecidas(municipio: str, uf: str) -> Optional[Tuple[float, float]]:
    """Base de coordenadas de municípios conhecidos (backup)"""
    # Coordenadas aproximadas de capitais e principais cidades
    coords = {
        "PORTO ALEGRE-RS": (-30.0346, -51.2177),
        "SAO PAULO-SP": (-23.5505, -46.6333),
        "RIO DE JANEIRO-RJ": (-22.9068, -43.1729),
        "BRASILIA-DF": (-15.8267, -47.9218),
        "BELO HORIZONTE-MG": (-19.9167, -43.9345),
        "CURITIBA-PR": (-25.4284, -49.2733),
        "FORTALEZA-CE": (-3.7172, -38.5433),
        "SALVADOR-BA": (-12.9714, -38.5014),
        "RECIFE-PE": (-8.0476, -34.8770),
        "MANAUS-AM": (-3.1190, -60.0217),
        "FLORIANOPOLIS-SC": (-27.5954, -48.5480),
        "GOIANIA-GO": (-16.6869, -49.2648),
        "VITORIA-ES": (-20.3155, -40.3128),
        "CAMPO GRANDE-MS": (-20.4697, -54.6201),
        "CUIABA-MT": (-15.6014, -56.0979),
        "FREDERICO WESTPHALEN-RS": (-27.3594, -53.3937),
        "PASSO FUNDO-RS": (-28.2620, -52.4083),
        "ERECHIM-RS": (-27.6336, -52.2736),
        "CHAPECO-SC": (-27.0965, -52.6146),
        "CASCAVEL-PR": (-24.9555, -53.4552),
        "FOZ DO IGUACU-PR": (-25.5163, -54.5854),
        "LONDRINA-PR": (-23.3045, -51.1696),
        "MARINGA-PR": (-23.4205, -51.9333),
        "CAMPINAS-SP": (-22.9099, -47.0626),
        "RIBEIRAO PRETO-SP": (-21.1767, -47.8103),
        "SANTOS-SP": (-23.9608, -46.3336),
        "SOROCABA-SP": (-23.5015, -47.4526),
    }
    
    chave = f"{municipio}-{uf}"
    return coords.get(chave)

def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calcula distância em KM entre dois pontos usando fórmula de Haversine
    """
    R = 6371  # Raio da Terra em km
    
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    delta_lat = math.radians(lat2 - lat1)
    delta_lon = math.radians(lon2 - lon1)
    
    a = math.sin(delta_lat/2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    
    return R * c

def calcular_distancia_entre_municipios(cep_origem: str, cep_destino: str) -> Tuple[Optional[float], str, Dict[str, Any]]:
    """
    Calcula distância real entre municípios
    Retorna: (km, fonte, detalhes)
    """
    # Busca municípios
    muni_origem = buscar_municipio_por_cep(cep_origem)
    muni_destino = buscar_municipio_por_cep(cep_destino)
    
    detalhes = {
        "cep_origem": cep_origem,
        "cep_destino": cep_destino,
        "municipio_origem": None,
        "uf_origem": None,
        "municipio_destino": None,
        "uf_destino": None,
        "coord_origem": None,
        "coord_destino": None,
    }
    
    if not muni_origem or not muni_destino:
        return (None, "erro_municipio", detalhes)
    
    municipio_orig, uf_orig = muni_origem
    municipio_dest, uf_dest = muni_destino
    
    detalhes["municipio_origem"] = municipio_orig
    detalhes["uf_origem"] = uf_orig
    detalhes["municipio_destino"] = municipio_dest
    detalhes["uf_destino"] = uf_dest
    
    # Verifica se é mesmo município
    if municipio_orig == municipio_dest and uf_orig == uf_dest:
        return (10.0, "mesmo_municipio", detalhes)  # Frete mínimo local
    
    # Busca coordenadas dos municípios
    coord_origem = buscar_coordenadas_municipio(municipio_orig, uf_orig)
    coord_destino = buscar_coordenadas_municipio(municipio_dest, uf_dest)
    
    detalhes["coord_origem"] = coord_origem
    detalhes["coord_destino"] = coord_destino
    
    if not coord_origem or not coord_destino:
        return (None, "erro_coordenadas", detalhes)
    
    # Calcula distância
    lat1, lon1 = coord_origem
    lat2, lon2 = coord_destino
    km = haversine(lat1, lon1, lat2, lon2)
    
    # Arredonda para múltiplos de 5 (mais realista para rotas terrestres)
    km = round(km / 5) * 5
    km = max(10.0, km)  # Mínimo 10km
    
    return (km, "distancia_municipio", detalhes)

# ==========================
# HELPERS ORIGINAIS
# ==========================
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

# ==========================
# PLANILHA
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

def carregar_tudo() -> Dict[str, Any]:
    try:
        xls = pd.ExcelFile(ARQ_PLANILHA)
    except Exception as e:
        print(f"[WARN] Não foi possível carregar planilha: {e}")
        return {
            "consts": {"VALOR_KM": DEFAULT_VALOR_KM, "TAM_CAMINHAO": DEFAULT_TAM_CAMINHAO},
            "catalogo": {}
        }

    consts = carregar_constantes(xls)
    cadastro = carregar_cadastro_produtos(xls)
    catalogo = montar_catalogo_tamanho(cadastro)
    
    return {"consts": consts, "catalogo": catalogo}

DATA = carregar_tudo()

# ==========================
# CÁLCULO DE FRETE
# ==========================
def calcula_valor_item(tamanho_peca_m: float, km: float, valor_km: float, tam_caminhao: float) -> float:
    """
    Fórmula: (tamanho_peça / tamanho_caminhão) * valor_km * km
    """
    if tamanho_peca_m <= 0 or tam_caminhao <= 0: return 0.0
    ocupacao = float(tamanho_peca_m) / float(tam_caminhao)
    return round(float(valor_km) * float(km) * ocupacao, 2)

def parse_prods(prods_str: str) -> List[Dict[str, Any]]:
    """Parse dos produtos no formato Tray"""
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
            comp, larg, alt, cub, qty, peso, codigo, valor = raw.split(";")
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
# ENDPOINTS
# ==========================
@app.route("/health")
def health():
    return {
        "ok": True,
        "cep_origem": CEP_ORIGEM,
        "valores": DATA["consts"],
        "itens_catalogo": len(DATA["catalogo"]),
        "cache_municipios": len(cache_municipios),
        "cache_coordenadas": len(cache_coordenadas_municipio),
    }

@app.route("/frete")
def frete():
    # Autenticação
    token = request.args.get("token", "")
    if token != TOKEN_SECRETO:
        return Response("Token inválido", status=403)

    # Parâmetros
    cep_origem_param = request.args.get("cep_origem", CEP_ORIGEM)
    cep_destino = request.args.get("cep_destino", "")
    prods = request.args.get("prods", "")

    if not cep_destino or not prods:
        return Response("Parâmetros insuficientes (cep_destino, prods)", status=400)

    # Parse produtos
    itens = parse_prods(prods)
    if not itens:
        return Response("Nenhum item válido em 'prods'", status=400)

    # Constantes base
    valor_km = DATA["consts"].get("VALOR_KM", DEFAULT_VALOR_KM)
    tam_caminhao = DATA["consts"].get("TAM_CAMINHAO", DEFAULT_TAM_CAMINHAO)

    # Permite override via parâmetro (para testes)
    try:
        if request.args.get("valor_km"):
            valor_km = float(str(request.args["valor_km"]).replace(",", "."))
        if request.args.get("tam_caminhao"):
            tam_caminhao = float(str(request.args["tam_caminhao"]).replace(",", "."))
    except: pass

    # ====== CALCULA DISTÂNCIA ENTRE MUNICÍPIOS ======
    km, km_fonte, detalhes = calcular_distancia_entre_municipios(cep_origem_param, cep_destino)
    
    if km is None:
        # Fallback: estimativa por UF
        uf_dest = uf_por_cep(so_digitos(cep_destino))
        KM_APROX_POR_UF = {
            "RS":150,"SC":450,"PR":700,"SP":1100,"RJ":1500,"MG":1600,"ES":1800,
            "MS":1600,"MT":2200,"DF":2000,"GO":2100,"TO":2500,"BA":2600,"SE":2700,
            "AL":2800,"PE":3000,"PB":3100,"RN":3200,"CE":3400,"PI":3300,"MA":3500,
            "PA":3800,"AP":4100,"AM":4200,"RO":4000,"AC":4300,"RR":4500,
        }
        km = KM_APROX_POR_UF.get(uf_dest, DEFAULT_KM)
        km_fonte = f"uf_fallback_{uf_dest}" if uf_dest else "default"

    # ====== CALCULA FRETE POR PRODUTO ======
    total = 0.0
    itens_xml = []
    
    for it in itens:
        nome = it["codigo"] or "Item"
        
        # Busca tamanho no catálogo ou calcula pelas dimensões
        tam_catalogo = DATA["catalogo"].get(nome)
        if tam_catalogo is None:
            tam_catalogo = tamanho_peca_por_nome(nome, it["alt"], it["larg"])
            if tam_catalogo == 0:
                tam_catalogo = max(it["comp"], it["larg"], it["alt"])
        
        # Calcula valor unitário e total
        v_unit = calcula_valor_item(tam_catalogo, km, valor_km, tam_caminhao)
        v_tot = v_unit * max(1, it["qty"])
        total += v_tot
        
        itens_xml.append(f"""
      <item>
        <codigo>{nome}</codigo>
        <quantidade>{it['qty']}</quantidade>
        <diametro_metros>{tam_catalogo:.3f}</diametro_metros>
        <valor_unitario>{v_unit:.2f}</valor_unitario>
        <valor_total>{v_tot:.2f}</valor_total>
      </item>""")

    # Monta resposta XML
    municipio_info = ""
    if detalhes.get("municipio_origem") and detalhes.get("municipio_destino"):
        municipio_info = (f"municipio_origem='{detalhes['municipio_origem']}/{detalhes['uf_origem']}' "
                         f"municipio_destino='{detalhes['municipio_destino']}/{detalhes['uf_destino']}' ")

    debug_info = (f"<debug "
                  f"cep_origem='{cep_origem_param}' "
                  f"cep_destino='{cep_destino}' "
                  f"{municipio_info}"
                  f"km='{km:.1f}' "
                  f"fonte_km='{km_fonte}' "
                  f"valor_km='{valor_km}' "
                  f"tam_caminhao='{tam_caminhao}' "
                  f"total_itens='{len(itens)}'"
                  f"/>")

    xml = f"""<?xml version="1.0"?>
<cotacao>
  <resultado>
    <codigo>BAKOF</codigo>
    <transportadora>Bakof Log</transportadora>
    <servico>Transporte</servico>
    <transporte>TERRESTRE</transporte>
    <valor>{total:.2f}</valor>
    <km_distancia>{km:.1f}</km_distancia>
    <prazo_min>4</prazo_min>
    <prazo_max>7</prazo_max>
    <entrega_domiciliar>1</entrega_domiciliar>
    <detalhes>{"".join(itens_xml)}
    </detalhes>
    {debug_info}
  </resultado>
</cotacao>"""
    
    return Response(xml, mimetype="application/xml")

# ==========================
# ENDPOINT
