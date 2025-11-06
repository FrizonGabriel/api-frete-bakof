# app.py — API de Frete com cálculo de distância LOCAL (sem APIs externas)
import os
import math
import re
from typing import Dict, Any, List, Tuple, Optional
import pandas as pd
from flask import Flask, request, Response

# ==========================
# CONFIGURAÇÕES
# ==========================
TOKEN_SECRETO = os.getenv("TOKEN_SECRETO", "teste123")
CEP_ORIGEM = os.getenv("CEP_ORIGEM", "98400000")  # Frederico Westphalen/RS
ARQ_PLANILHA = os.getenv("PLANILHA_FRETE", "tabela de frete atualizada(2)(Recuperado Automaticamente).xlsx")

DEFAULT_VALOR_KM = float(os.getenv("DEFAULT_VALOR_KM", "7.0"))
DEFAULT_TAM_CAMINHAO = float(os.getenv("DEFAULT_TAM_CAMINHAO", "8.5"))
DEFAULT_KM = float(os.getenv("DEFAULT_KM", "450.0"))

PALAVRAS_IGNORAR = {
    "VALOR KM", "TAMANHO CAMINHAO", "TAMANHO CAMINHÃO",
    "CALCULO DE FRETE POR TAMANHO DE PEÇA", "CÁLCULO DE FRETE POR TAMANHO DE PEÇA"
}

app = Flask(__name__)

# ==========================
# TABELA DE COORDENADAS LOCAL (sem API)
# ==========================
COORDENADAS_MUNICIPIOS = {
    # Rio Grande do Sul
    "FREDERICO WESTPHALEN-RS": (-27.3594, -53.3937),
    "PORTO ALEGRE-RS": (-30.0346, -51.2177),
    "CAXIAS DO SUL-RS": (-29.1634, -51.1797),
    "PELOTAS-RS": (-31.7654, -52.3376),
    "CANOAS-RS": (-29.9177, -51.1844),
    "SANTA MARIA-RS": (-29.6868, -53.8149),
    "GRAVATAI-RS": (-29.9419, -50.9928),
    "VIAMAO-RS": (-30.0811, -51.0233),
    "NOVO HAMBURGO-RS": (-29.6783, -51.1306),
    "SAO LEOPOLDO-RS": (-29.7600, -51.1479),
    "ALVORADA-RS": (-30.0011, -51.0797),
    "PASSO FUNDO-RS": (-28.2620, -52.4083),
    "SAPUCAIA DO SUL-RS": (-29.8389, -51.1447),
    "URUGUAIANA-RS": (-29.7547, -57.0883),
    "SANTA CRUZ DO SUL-RS": (-29.7175, -52.4261),
    "CACHOEIRINHA-RS": (-29.9508, -51.0944),
    "ERECHIM-RS": (-27.6336, -52.2736),
    "GUAIBA-RS": (-30.1139, -51.3253),
    "SANTANA DO LIVRAMENTO-RS": (-30.8908, -55.5322),
    "BAGE-RS": (-31.3286, -54.1072),
    # Santa Catarina
    "FLORIANOPOLIS-SC": (-27.5954, -48.5480),
    "JOINVILLE-SC": (-26.3045, -48.8487),
    "BLUMENAU-SC": (-26.9194, -49.0661),
    "SAO JOSE-SC": (-27.6108, -48.6350),
    "CHAPECO-SC": (-27.0965, -52.6146),
    "CRICIUMA-SC": (-28.6773, -49.3695),
    "ITAJAI-SC": (-26.9075, -48.6614),
    "JARAGUA DO SUL-SC": (-26.4869, -49.0669),
    "LAGES-SC": (-27.8160, -50.3264),
    "PALHOCA-SC": (-27.6450, -48.6700),
    # Paraná
    "CURITIBA-PR": (-25.4284, -49.2733),
    "LONDRINA-PR": (-23.3045, -51.1696),
    "MARINGA-PR": (-23.4205, -51.9333),
    "PONTA GROSSA-PR": (-25.0916, -50.1668),
    "CASCAVEL-PR": (-24.9555, -53.4552),
    "SAO JOSE DOS PINHAIS-PR": (-25.5304, -49.2064),
    "FOZ DO IGUACU-PR": (-25.5163, -54.5854),
    "COLOMBO-PR": (-25.2919, -49.2244),
    "GUARAPUAVA-PR": (-25.3905, -51.4628),
    "PARANAGUA-PR": (-25.5200, -48.5089),
    # São Paulo
    "SAO PAULO-SP": (-23.5505, -46.6333),
    "GUARULHOS-SP": (-23.4538, -46.5333),
    "CAMPINAS-SP": (-22.9099, -47.0626),
    "SAO BERNARDO DO CAMPO-SP": (-23.6914, -46.5647),
    "SANTO ANDRE-SP": (-23.6636, -46.5341),
    "OSASCO-SP": (-23.5329, -46.7919),
    "SAO JOSE DOS CAMPOS-SP": (-23.1791, -45.8872),
    "RIBEIRAO PRETO-SP": (-21.1767, -47.8103),
    "SOROCABA-SP": (-23.5015, -47.4526),
    "SANTOS-SP": (-23.9608, -46.3336),
    # Rio de Janeiro
    "RIO DE JANEIRO-RJ": (-22.9068, -43.1729),
    "SAO GONCALO-RJ": (-22.8268, -43.0534),
    "DUQUE DE CAXIAS-RJ": (-22.7858, -43.3054),
    "NOVA IGUACU-RJ": (-22.7591, -43.4509),
    "NITEROI-RJ": (-22.8839, -43.1039),
    # Minas Gerais
    "BELO HORIZONTE-MG": (-19.9167, -43.9345),
    "UBERLANDIA-MG": (-18.9186, -48.2772),
    "CONTAGEM-MG": (-19.9320, -44.0539),
    "JUIZ DE FORA-MG": (-21.7642, -43.3502),
    # Outras capitais
    "BRASILIA-DF": (-15.8267, -47.9218),
    "SALVADOR-BA": (-12.9714, -38.5014),
    "FORTALEZA-CE": (-3.7172, -38.5433),
    "RECIFE-PE": (-8.0476, -34.8770),
    "MANAUS-AM": (-3.1190, -60.0217),
    "GOIANIA-GO": (-16.6869, -49.2648),
    "VITORIA-ES": (-20.3155, -40.3128),
    "CAMPO GRANDE-MS": (-20.4697, -54.6201),
    "CUIABA-MT": (-15.6014, -56.0979),
}

# Mapeamento CEP -> Município
FAIXAS_CEP_MUNICIPIO = [
    # RS - Principais
    ("98400000", "98419999", "FREDERICO WESTPHALEN-RS"),
    ("90000000", "91999999", "PORTO ALEGRE-RS"),
    ("95000000", "95130999", "CAXIAS DO SUL-RS"),
    ("96000000", "96099999", "PELOTAS-RS"),
    ("92000000", "92999999", "CANOAS-RS"),
    ("97000000", "97119999", "SANTA MARIA-RS"),
    ("99000000", "99099999", "PASSO FUNDO-RS"),
    ("99700000", "99799999", "ERECHIM-RS"),
    # SC - Principais
    ("88000000", "88099999", "FLORIANOPOLIS-SC"),
    ("89200000", "89239999", "JOINVILLE-SC"),
    ("89000000", "89099999", "BLUMENAU-SC"),
    ("89800000", "89879999", "CHAPECO-SC"),
    # PR - Principais
    ("80000000", "82999999", "CURITIBA-PR"),
    ("86000000", "86199999", "LONDRINA-PR"),
    ("87000000", "87099999", "MARINGA-PR"),
    ("85800000", "85879999", "CASCAVEL-PR"),
    ("85850000", "85869999", "FOZ DO IGUACU-PR"),
    # SP - Principais
    ("01000000", "05999999", "SAO PAULO-SP"),
    ("07000000", "07399999", "GUARULHOS-SP"),
    ("13000000", "13149999", "CAMPINAS-SP"),
    ("09700000", "09899999", "SAO BERNARDO DO CAMPO-SP"),
    ("11000000", "11999999", "SANTOS-SP"),
    ("12200000", "12249999", "SAO JOSE DOS CAMPOS-SP"),
    ("14000000", "14109999", "RIBEIRAO PRETO-SP"),
    # RJ - Principais
    ("20000000", "23799999", "RIO DE JANEIRO-RJ"),
    ("24000000", "24999999", "NITEROI-RJ"),
    # MG - Principais
    ("30000000", "31999999", "BELO HORIZONTE-MG"),
    ("32000000", "32999999", "CONTAGEM-MG"),
    # DF
    ("70000000", "72799999", "BRASILIA-DF"),
]

# ==========================
# FUNÇÕES DE CÁLCULO
# ==========================
def limpar_cep(cep: str) -> str:
    """Remove formatação e retorna 8 dígitos"""
    s = re.sub(r'\D', '', str(cep or ""))
    return s[:8].zfill(8) if s else "00000000"

def buscar_municipio_por_cep(cep: str) -> Optional[str]:
    """Busca município baseado em faixas de CEP locais"""
    cep_limpo = limpar_cep(cep)
    cep_num = int(cep_limpo)
    
    for inicio, fim, municipio in FAIXAS_CEP_MUNICIPIO:
        if int(inicio) <= cep_num <= int(fim):
            return municipio
    
    # Fallback: busca por UF
    uf = uf_por_cep(cep_limpo)
    if uf:
        # Retorna capital da UF
        capitais = {
            "RS": "PORTO ALEGRE-RS",
            "SC": "FLORIANOPOLIS-SC",
            "PR": "CURITIBA-PR",
            "SP": "SAO PAULO-SP",
            "RJ": "RIO DE JANEIRO-RJ",
            "MG": "BELO HORIZONTE-MG",
            "DF": "BRASILIA-DF",
        }
        return capitais.get(uf)
    
    return None

def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calcula distância em KM entre dois pontos"""
    R = 6371  # Raio da Terra em km
    
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    delta_lat = math.radians(lat2 - lat1)
    delta_lon = math.radians(lon2 - lon1)
    
    a = math.sin(delta_lat/2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    
    return R * c

def calcular_distancia_real(cep_origem: str, cep_destino: str) -> Tuple[Optional[float], str, Dict[str, Any]]:
    """
    Calcula distância real usando tabela local de coordenadas
    Retorna: (km, fonte, detalhes)
    """
    detalhes = {
        "cep_origem": cep_origem,
        "cep_destino": cep_destino,
        "municipio_origem": None,
        "municipio_destino": None,
    }
    
    # Busca municípios
    muni_origem = buscar_municipio_por_cep(cep_origem)
    muni_destino = buscar_municipio_por_cep(cep_destino)
    
    detalhes["municipio_origem"] = muni_origem
    detalhes["municipio_destino"] = muni_destino
    
    if not muni_origem or not muni_destino:
        return (None, "municipio_nao_encontrado", detalhes)
    
    # Verifica se é mesmo município
    if muni_origem == muni_destino:
        return (10.0, "mesmo_municipio", detalhes)
    
    # Busca coordenadas
    coord_origem = COORDENADAS_MUNICIPIOS.get(muni_origem)
    coord_destino = COORDENADAS_MUNICIPIOS.get(muni_destino)
    
    if not coord_origem or not coord_destino:
        return (None, "coordenadas_nao_encontradas", detalhes)
    
    # Calcula distância
    lat1, lon1 = coord_origem
    lat2, lon2 = coord_destino
    km = haversine(lat1, lon1, lat2, lon2)
    
    # Ajusta distância (rodovias são ~15% mais longas que linha reta)
    km = km * 1.15
    
    # Arredonda para múltiplos de 5
    km = round(km / 5) * 5
    km = max(10.0, km)
    
    return (km, "distancia_calculada", detalhes)

def uf_por_cep(cep8: str) -> Optional[str]:
    """Retorna UF baseado na faixa de CEP"""
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

# ==========================
# FUNÇÕES DA PLANILHA
# ==========================
def limpar_texto(nome: Any) -> str:
    if not isinstance(nome, str):
        return ""
    return " ".join(nome.replace("\n"," ").split()).strip()

def extrai_numero_linha(row) -> Optional[float]:
    for v in row:
        if v is None or pd.isna(v):
            continue
        s = str(v).strip().upper()
        if s in ("", "NAN", "NONE", "NULL"):
            continue
        s = s.replace(",", ".")
        s = re.sub(r'(METROS?|KM|R\$|REAIS|/KM)', '', s, flags=re.IGNORECASE).strip()
        try:
            f = float(s)
            if math.isfinite(f) and f > 0:
                return f
        except:
            pass
    return None

def carregar_constantes(xls: pd.ExcelFile) -> Dict[str, float]:
    valor_km = DEFAULT_VALOR_KM
    tam_caminhao = DEFAULT_TAM_CAMINHAO
    
    for aba in ("BASE_CALCULO", "D", "BASE", "CONSTANTES"):
        if aba not in xls.sheet_names:
            continue
        try:
            raw = pd.read_excel(xls, aba, header=None)
            for _, row in raw.iterrows():
                texto = " ".join([str(v).upper() for v in row if isinstance(v, str)])
                if "VALOR" in texto or "KM" in texto:
                    num = extrai_numero_linha(row)
                    if num and 3 <= num <= 50:
                        valor_km = num
                if "TAMANHO" in texto and "CAMINH" in texto:
                    num = extrai_numero_linha(row)
                    if num and 3 <= num <= 20:
                        tam_caminhao = num
        except Exception as e:
            print(f"[WARN] Erro ao ler aba {aba}: {e}")
    
    return {"VALOR_KM": valor_km, "TAM_CAMINHAO": tam_caminhao}

def carregar_cadastro_produtos(xls: pd.ExcelFile) -> pd.DataFrame:
    for aba in ("CADASTRO_PRODUTO", "CADASTRO", "PRODUTOS"):
        if aba not in xls.sheet_names:
            continue
        try:
            raw = pd.read_excel(xls, aba, header=None)
            nome_col = 2 if raw.shape[1] > 2 else 0
            dim1_col = 3 if raw.shape[1] > 3 else (1 if raw.shape[1] > 1 else 0)
            dim2_col = 4 if raw.shape[1] > 4 else (2 if raw.shape[1] > 2 else 1)
            
            df = raw[[nome_col, dim1_col, dim2_col]].copy()
            df.columns = ["nome", "dim1", "dim2"]
            df["nome"] = df["nome"].apply(limpar_texto)
            df = df[~df["nome"].str.upper().isin(PALAVRAS_IGNORAR)]
            df = df[df["nome"].astype(str).str.len() > 0]
            df["dim1"] = pd.to_numeric(df["dim1"], errors="coerce").fillna(0.0)
            df["dim2"] = pd.to_numeric(df["dim2"], errors="coerce").fillna(0.0)
            df = df.drop_duplicates(subset=["nome"], keep="first").reset_index(drop=True)
            return df[["nome", "dim1", "dim2"]]
        except Exception as e:
            print(f"[WARN] Erro ao ler aba {aba}: {e}")
    
    return pd.DataFrame(columns=["nome", "dim1", "dim2"])

def tipo_produto(nome: str) -> str:
    n = (nome or "").lower()
    if "fossa" in n:
        return "fossa"
    if "vertical" in n:
        return "vertical"
    if "horizontal" in n:
        return "horizontal"
    if "tc" in n and ("10.000" in n or "10000" in n or "10.0" in n):
        return "tc_ate_10k"
    return "auto"

def tamanho_peca_por_nome(nome: str, dim1: float, dim2: float) -> float:
    t = tipo_produto(nome)
    if t in ("fossa", "vertical"):
        return float(dim1 or 0.0)
    if t in ("horizontal", "tc_ate_10k"):
        return float(dim2 or 0.0)
    return float(max(float(dim1 or 0.0), float(dim2 or 0.0)))

def montar_catalogo_tamanho(df: pd.DataFrame) -> Dict[str, float]:
    mapa = {}
    for _, r in df.iterrows():
        try:
            nome = limpar_texto(r["nome"])
            if not nome or nome.upper() in PALAVRAS_IGNORAR:
                continue
            tam = tamanho_peca_por_nome(nome, float(r["dim1"]), float(r["dim2"]))
            if tam > 0:
                mapa[nome] = tam
        except Exception as e:
            print(f"[WARN] Erro ao processar produto: {e}")
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
        return {
            "consts": {"VALOR_KM": DEFAULT_VALOR_KM, "TAM_CAMINHAO": DEFAULT_TAM_CAMINHAO},
            "catalogo": {}
        }

DATA = carregar_tudo()

# ==========================
# CÁLCULO DE FRETE
# ==========================
def calcula_valor_item(tamanho_peca_m: float, km: float, valor_km: float, tam_caminhao: float) -> float:
    """Fórmula: (tamanho_peça / tamanho_caminhão) * valor_km * km"""
    if tamanho_peca_m <= 0 or tam_caminhao <= 0:
        return 0.0
    ocupacao = float(tamanho_peca_m) / float(tam_caminhao)
    return round(float(valor_km) * float(km) * ocupacao, 2)

def parse_prods(prods_str: str) -> List[Dict[str, Any]]:
    """Parse dos produtos no formato Tray"""
    itens = []
    if not prods_str:
        return itens
    
    blocos = []
    for sep in ("/", "|"):
        if sep in prods_str:
            blocos = [b for b in prods_str.split(sep) if b.strip()]
            break
    if not blocos:
        blocos = [prods_str]

    def norm_num(x):
        if x is None:
            return 0.0
        s = str(x).strip().lower()
        if s in ("", "null", "none", "nan"):
            return 0.0
        s = s.replace(",", ".")
        try:
            return float(s)
        except:
            return 0.0

    def cm_to_m(x):
        if not x or x == 0:
            return 0.0
        return x/100.0 if x > 20 else x

    for raw in blocos:
        try:
            partes = raw.split(";")
            if len(partes) < 8:
                continue
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
    
    return itens

# ==========================
# ENDPOINTS
# ==========================
@app.route("/")
def index():
    return {
        "api": "Bakof Frete",
        "versao": "3.0 - Cálculo LOCAL",
        "municipios_disponiveis": len(COORDENADAS_MUNICIPIOS),
        "endpoints": {
            "/health": "Status da API",
            "/frete": "Calcular frete",
            "/teste-distancia": "Testar distância entre CEPs",
            "/municipios": "Listar municípios disponíveis"
        }
    }

@app.route("/health")
def health():
    return {
        "ok": True,
        "cep_origem": CEP_ORIGEM,
        "valores": DATA["consts"],
        "itens_catalogo": len(DATA["catalogo"]),
        "municipios_cadastrados": len(COORDENADAS_MUNICIPIOS),
    }

@app.route("/municipios")
def listar_municipios():
    """Lista todos os municípios disponíveis"""
    return {
        "total": len(COORDENADAS_MUNICIPIOS),
        "municipios": sorted(list(COORDENADAS_MUNICIPIOS.keys()))
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

    # Permite override via parâmetro
    try:
        if request.args.get("valor_km"):
            valor_km = float(str(request.args["valor_km"]).replace(",", "."))
        if request.args.get("tam_caminhao"):
            tam_caminhao = float(str(request.args["tam_caminhao"]).replace(",", "."))
    except:
        pass

    # Calcula distância
    km, km_fonte, detalhes = calcular_distancia_real(cep_origem_param, cep_destino)
    
    if km is None:
        # Fallback por UF
        uf_dest = uf_por_cep(limpar_cep(cep_destino))
        KM_APROX_POR_UF = {
            "RS":150,"SC":450,"PR":700,"SP":1100,"RJ":1500,"MG":1600,"ES":1800,
            "MS":1600,"MT":2200,"DF":2000,"GO":2100,"TO":2500,"BA":2600,"SE":2700,
            "AL":2800,"PE":3000,"PB":3100,"RN":3200,"CE":3400,"PI":3300,"MA":3500,
            "PA":3800,"AP":4100,"AM":4200,"RO":4000,"AC":4300,"RR":4500,
        }
        km = KM_APROX_POR_UF.get(uf_dest, DEFAULT_KM)
        km_fonte = f"uf_fallback_{uf_dest}" if uf_dest else "default"

    # Calcula frete por produto
    total = 0.0
    itens_xml = []
    
    for it in itens:
        nome = it["codigo"] or "Item"
        
        # Busca tamanho no catálogo
        tam_catalogo = DATA["catalogo"].get(nome)
        if tam_catalogo is None:
            tam_catalogo = tamanho_peca_por_nome(nome, it["alt"], it["larg"])
            if tam_catalogo == 0:
                tam_catalogo = max(it["comp"], it["larg"], it["alt"])
        
        # Calcula valores
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
        municipio_info = (f"municipio_origem='{detalhes['municipio_origem']}' "
                         f"municipio_destino
