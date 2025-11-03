# app.py
from flask import Flask, request, Response
import pandas as pd
import math
import re

app = Flask(__name__)

# === CONFIG BÁSICA ===
TOKEN_SECRETO = "MINHA_CHAVE_FORTE"  # use o mesmo token que vai cadastrar na Tray
ARQ_PLANILHA = "tabela de frete atualizada(2)(Recuperado Automaticamente).xlsx"

# === CARREGA DADOS DA SUA PLANILHA ===
xls = pd.ExcelFile(ARQ_PLANILHA)

# Sheet D: extrai VALOR_KM e TAMANHO_CAMINHAO
raw_d = pd.read_excel(xls, "D", header=None)
def extrai_constante(df, chave):
    for _, row in df.iterrows():
        txts = [v for v in row if isinstance(v, str)]
        if any(chave in t.upper() for t in txts):
            nums = [v for v in row if isinstance(v, (int, float))]
            if nums:
                return float(nums[0])
    return None

VALOR_KM = extrai_constante(raw_d, "VALOR KM") or 7.0
TAM_CAMINHAO = extrai_constante(raw_d, "TAMANHO CAMINHAO") or 8.5

# Sheet CADASTRO_PRODUTO: nome + duas dimensões
cad = pd.read_excel(xls, "CADASTRO_PRODUTO", header=None)
cad.columns = ["c0","c1","nome","dim1","dim2","c5"]
cad = cad[~cad["nome"].isna()][["nome","dim1","dim2"]].drop_duplicates()

# Heurística de tipo pelo nome do produto
def tipo_produto(nome):
    n = nome.lower()
    if "fossa" in n:
        return "fossa"
    if "vertical" in n:
        return "vertical"
    if "horizontal" in n:
        return "horizontal"
    if "tc" in n and ("10.000" in n or "10000" in n):
        return "tc_ate_10k"
    return "auto"

def tamanho_peca_por_nome(nome, dim1, dim2):
    t = tipo_produto(nome)
    if t == "fossa":       # usar ALTURA
        return float(dim1)
    if t == "vertical":    # usar ALTURA
        return float(dim1)
    if t == "horizontal":  # usar LARGURA/DIÂMETRO
        return float(dim2)
    if t == "tc_ate_10k":  # usar LARGURA MAIOR (dim2)
        return float(dim2)
    # auto: escolhe a maior dimensão “de controle”
    return float(max(dim1, dim2))

# Mapa: nome → tamanho_de_controle
catalogo_tamanho = {}
for _, r in cad.iterrows():
    catalogo_tamanho[r["nome"]] = tamanho_peca_por_nome(r["nome"], r["dim1"], r["dim2"])

# === Distância (km) ===
# Se você ainda não quiser integrar CEP→km, aceite um parâmetro ?km= no teste.
# Depois dá para trocar por uma função que lê suas TABELAS DE CEP→DISTÂNCIA.
def obter_km(cep_origem, cep_destino, km_param):
    if km_param:
        return float(km_param)
    # TODO: plugar sua tabela de faixas de CEP aqui
    # por enquanto, default conservador
    return 100.0

def calcula_valor_por_item(tamanho_peca_m, km):
    ocupacao = max(0.01, tamanho_peca_m / TAM_CAMINHAO)
    valor_km = VALOR_KM * ocupacao
    return round(valor_km * km, 2)

# === PROTOCOLO TRAY (GET) ===
# Parâmetros que a Tray envia: token, cep, cep_destino, prods
# Em prods cada item é "compr;larg;alt;cubagem;qty;peso;codigo;valor"
@app.route("/frete", methods=["GET"])
def frete():
    token = request.args.get("token","")
    if token != TOKEN_SECRETO:
        return Response("Token inválido", status=403)

    cep = request.args.get("cep","")
    cep_dest = request.args.get("cep_destino","")
    prods = request.args.get("prods","")
    km_param = request.args.get("km","")  # opcional p/ teste

    if not cep or not cep_dest or not prods:
        return Response("Parâmetros insuficientes", status=400)

    km = obter_km(cep, cep_dest, km_param)

    total = 0.0
    itens_xml = []
    for raw in prods.split("/"):
        if not raw.strip():
            continue
        try:
            comp, larg, alt, cub, qty, peso, codigo, valor = raw.split(";")
            qty = int(float(qty))
            comp = float(comp); larg = float(larg); alt = float(alt)
            codigo = codigo.strip()

            # tenta achar pelo NOME/SKU no cadastro
            tam_controle = None
            if codigo in catalogo_tamanho:
                tam_controle = catalogo_tamanho[codigo]
            else:
                # fallback: decide por palavras-chave no código
                tam_controle = tamanho_peca_por_nome(codigo, alt, larg)

            valor_item = calcula_valor_por_item(tam_controle, km) * qty
            total += valor_item

            itens_xml.append(f"""
      <item>
        <codigo>{codigo}</codigo>
        <tamanho_controle>{tam_controle:.3f}</tamanho_controle>
        <km>{km:.1f}</km>
        <valor>{valor_item:.2f}</valor>
      </item>""")
        except Exception as e:
            continue

    # Monte 2 serviços (Econômico/Expresso) com multiplicadores/prazos diferentes
    servicos = [
        ("ECON", "Econômico", 1.00, 4, 7),
        ("EXPR", "Expresso", 1.20, 1, 3),
    ]

    resultados = []
    for cod, nome, mult, pmin, pmax in servicos:
        valor = round(total * mult, 2)
        resultados.append(f"""
  <resultado>
    <codigo>{cod}</codigo>
    <transportadora>Bakof Log</transportadora>
    <servico>{nome}</servico>
    <transporte>TERRESTRE</transporte>
    <valor>{valor:.2f}</valor>
    <prazo_min>{pmin}</prazo_min>
    <prazo_max>{pmax}</prazo_max>
    <entrega_domiciliar>1</entrega_domiciliar>
    <detalhes>{"".join(itens_xml)}
    </detalhes>
  </resultado>""")

    xml = f"""<?xml version="1.0"?>
<cotacao>
{''.join(resultados)}
</cotacao>"""
    return Response(xml, mimetype="application/xml")
