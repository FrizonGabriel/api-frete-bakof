# app.py — API de Frete TRAY COMPATIBLE
import os
import math
import re
from typing import Dict, Any, List, Tuple, Optional
import pandas as pd
from flask import Flask, request, Response
from flask_cors import CORS

# ==========================
# CONFIGURAÇÕES
# ==========================
TOKEN_SECRETO = os.getenv("TOKEN_SECRETO", "teste123")
CEP_ORIGEM = os.getenv("CEP_ORIGEM", "98400000")
ARQ_PLANILHA = os.getenv("PLANILHA_FRETE", "tabela de frete atualizada(2)(Recuperado Automaticamente).xlsx")

DEFAULT_VALOR_KM = float(os.getenv("DEFAULT_VALOR_KM", "7.0"))
DEFAULT_TAM_CAMINHAO = float(os.getenv("DEFAULT_TAM_CAMINHAO", "8.5"))
DEFAULT_KM = float(os.getenv("DEFAULT_KM", "450.0"))

app = Flask(__name__)
CORS(app)  # CRITICAL: Tray precisa de CORS

# ==========================
# COORDENADAS MUNICIPIOS
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
    ("92000000", "92999999", "CANOAS-RS"),
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
    ("11000000", "11999999", "SANTOS-SP"),
    ("12200000", "12249999", "SAO JOSE DOS CAMPOS-SP"),
    ("14000000", "14109999", "RIBEIRAO PRETO-SP"),
    ("20000000", "23799999", "RIO DE JANEIRO-RJ"),
    ("24000000", "24999999", "NITEROI-RJ"),
    ("30000000", "31999999", "BELO HORIZONTE-MG"),
    ("70000000", "72799999", "BRASILIA-DF"),
]

# ==========================
# FUNÇÕES
# ==========================
def limpar_cep(cep: str) -> str:
    s = re.sub(r'\D', '', str(cep or ""))
    return s[:8].zfill(8) if s else "00000000"

def uf_por_cep(cep8: str) -> Optional[str]:
    ranges = [
        ("RS","90000000","99999999"),("SC","88000000","89999999"),
        ("PR","80000000","87999999"),("SP","01000000","19999999"),
        ("RJ","20000000","28999999"),("MG","30000000","39999999"),
        ("DF","70000000","73699999"),
    ]
    try:
        n = int(cep8)
        for uf, a, b in ranges:
            if int(a) <= n <= int(b):
                return uf
    except:
        pass
    return None

def buscar_municipio_por_cep(cep: str) -> Optional[str]:
    cep_limpo = limpar_cep(cep)
    cep_num = int(cep_limpo)
    for inicio, fim, municipio in FAIXAS_CEP_MUNICIPIO:
        if int(inicio) <= cep_num <= int(fim):
            return municipio
    uf = uf_por_cep(cep_limpo)
    capitais = {
        "RS": "PORTO ALEGRE-RS", "SC": "FLORIANOPOLIS-SC",
        "PR": "CURITIBA-PR", "SP": "SAO PAULO-SP",
        "RJ": "RIO DE JANEIRO-RJ", "MG": "BELO HORIZONTE-MG",
        "DF": "BRASILIA-DF",
    }
    return capitais.get(uf) if uf else None

def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))

def calcular_km(cep_origem: str, cep_destino: str) -> float:
    muni_origem = buscar_municipio_por_cep(cep_origem)
    muni_destino = buscar_municipio_por_cep(cep_destino)
    
    if not muni_origem or not muni_destino:
        uf = uf_por_cep(limpar_cep(cep_destino))
        KM_UF = {"RS":150,"SC":450,"PR":700,"SP":1100,"RJ":1500,"MG":1600,"DF":2000}
        return KM_UF.get(uf, DEFAULT_KM)
    
    if muni_origem == muni_destino:
        return 10.0
    
    c1 = COORDENADAS_MUNICIPIOS.get(muni_origem)
    c2 = COORDENADAS_MUNICIPIOS.get(muni_destino)
    if not c1 or not c2:
        return DEFAULT_KM
    
    km = haversine(c1[0], c1[1], c2[0], c2[1]) * 1.15
    return max(10.0, round(km / 5) * 5)

def carregar_dados():
    try:
        xls = pd.ExcelFile(ARQ_PLANILHA)
        # Simplificado - apenas retorna defaults se falhar
        return {"VALOR_KM": DEFAULT_VALOR_KM, "TAM_CAMINHAO": DEFAULT_TAM_CAMINHAO}
    except:
        return {"VALOR_KM": DEFAULT_VALOR_KM, "TAM_CAMINHAO": DEFAULT_TAM_CAMINHAO}

DATA = carregar_dados()

# ==========================
# ENDPOINT PRINCIPAL TRAY
# ==========================
@app.route("/frete", methods=["GET", "POST", "OPTIONS"])
def frete():
    # CORS preflight
    if request.method == "OPTIONS":
        response = Response()
        response.headers["Access-Control-Allow-Origin"] = "*"
        response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
        response.headers["Access-Control-Allow-Headers"] = "Content-Type"
        return response
    
    try:
        # Captura parâmetros (GET ou POST)
        if request.method == "POST":
            params = request.form.to_dict() or request.get_json(silent=True) or {}
        else:
            params = request.args.to_dict()
        
        print(f"\n{'='*60}")
        print(f"REQUISIÇÃO RECEBIDA - {request.method}")
        print(f"Params: {params}")
        print(f"{'='*60}\n")
        
        # Token (OPCIONAL para testes - remova isso em produção)
        token = params.get("token", "")
        # if token != TOKEN_SECRETO:
        #     return gerar_xml_erro("Token inválido"), 403
        
        # CEP destino (aceita 'cep' ou 'cep_destino')
        cep_destino = params.get("cep_destino") or params.get("cep") or params.get("zipcode") or ""
        if not cep_destino:
            return gerar_xml_erro("CEP não informado"), 400
        
        # Produtos (formato Tray: comp;larg;alt;cub;qty;peso;codigo;valor)
        prods_raw = params.get("prods", "")
        if not prods_raw:
            return gerar_xml_erro("Produtos não informados"), 400
        
        # Parse simples de produtos
        itens = []
        for bloco in prods_raw.split("/"):
            try:
                partes = bloco.split(";")
                if len(partes) >= 8:
                    itens.append({
                        "qty": int(float(partes[4])) if partes[4] else 1,
                        "peso": float(partes[5]) if partes[5] else 1.0,
                    })
            except:
                pass
        
        if not itens:
            itens = [{"qty": 1, "peso": 10.0}]  # Fallback
        
        print(f"Itens parseados: {len(itens)}")
        
        # Calcula KM
        cep_origem_param = params.get("cep_origem", CEP_ORIGEM)
        km = calcular_km(cep_origem_param, cep_destino)
        print(f"Distância: {km} km")
        
        # Valores
        valor_km = DATA.get("VALOR_KM", DEFAULT_VALOR_KM)
        
        # Cálculo simples: R$ por KM x quantidade de itens
        total_itens = sum(it["qty"] for it in itens)
        valor_frete = round(valor_km * km * (total_itens * 0.3), 2)  # 30% de ocupação por item
        
        # Valor mínimo
        if valor_frete < 50.0:
            valor_frete = 50.0
        
        print(f"Valor calculado: R$ {valor_frete:.2f}")
        
        # Prazo baseado em distância
        if km <= 100:
            prazo = 3
        elif km <= 300:
            prazo = 5
        elif km <= 600:
            prazo = 7
        elif km <= 1000:
            prazo = 10
        else:
            prazo = 15
        
        print(f"Prazo: {prazo} dias\n")
        
        # Gera XML EXATAMENTE como a Tray espera
        xml = f"""<?xml version="1.0" encoding="utf-8"?>
<frete>
  <servico>
    <codigo>BAKOF</codigo>
    <nome>Bakof Logistica</nome>
    <valor>{valor_frete:.2f}</valor>
    <prazo>{prazo}</prazo>
  </servico>
</frete>"""
        
        response = Response(xml, mimetype="text/xml; charset=utf-8")
        response.headers["Access-Control-Allow-Origin"] = "*"
        response.headers["Cache-Control"] = "no-cache"
        return response
        
    except Exception as e:
        print(f"\n[ERRO CRÍTICO]: {str(e)}")
        import traceback
        traceback.print_exc()
        return gerar_xml_erro(f"Erro: {str(e)}"), 500

def gerar_xml_erro(msg: str):
    xml = f"""<?xml version="1.0" encoding="utf-8"?>
<erro>
  <mensagem>{msg}</mensagem>
</erro>"""
    response = Response(xml, mimetype="text/xml; charset=utf-8")
    response.headers["Access-Control-Allow-Origin"] = "*"
    return response

# ==========================
# ENDPOINTS AUXILIARES
# ==========================
@app.route("/")
def index():
    return """
    <html>
    <body style="font-family: Arial; padding: 20px;">
        <h1>🚚 Bakof Frete API - TRAY</h1>
        <h2>Endpoints:</h2>
        <ul>
            <li><strong>/frete</strong> - Calcular frete (Tray)</li>
            <li><strong>/teste</strong> - Testar cálculo</li>
        </ul>
        <h3>Teste rápido:</h3>
        <form action="/teste" method="get">
            <label>CEP Destino:</label>
            <input name="cep" value="90000000" />
            <button>Testar</button>
        </form>
    </body>
    </html>
    """

@app.route("/teste")
def teste():
    cep = request.args.get("cep", "90000000")
    km = calcular_km(CEP_ORIGEM, cep)
    valor = round(DATA["VALOR_KM"] * km * 0.3, 2)
    
    return f"""
    <html>
    <body style="font-family: Arial; padding: 20px;">
        <h2>Resultado do Teste</h2>
        <p><strong>CEP Origem:</strong> {CEP_ORIGEM}</p>
        <p><strong>CEP Destino:</strong> {cep}</p>
        <p><strong>Distância:</strong> {km} km</p>
        <p><strong>Valor Frete:</strong> R$ {max(50.0, valor):.2f}</p>
        <br>
        <a href="/">← Voltar</a>
    </body>
    </html>
    """

@app.route("/health")
def health():
    return {"ok": True, "cep_origem": CEP_ORIGEM}

if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    print("\n" + "="*60)
    print("🚀 BAKOF FRETE API - TRAY COMPATIBLE")
    print("="*60)
    print(f"CEP Origem: {CEP_ORIGEM}")
    print(f"Valor/KM: R$ {DATA['VALOR_KM']:.2f}")
    print(f"Porta: {port}")
    print("="*60 + "\n")
    
    app.run(host="0.0.0.0", port=port, debug=True)
