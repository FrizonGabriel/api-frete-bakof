# app.py
import os
import math
from typing import Dict, Any, List
import pandas as pd
from flask import Flask, request, Response

# ==========================
# CONFIGURAÇÕES BÁSICAS
# ==========================
# Token: pode vir do ambiente (Render → Settings → Environment Variables)
TOKEN_SECRETO = os.getenv("TOKEN_SECRETO", "MINHA_CHAVE_FORTE")

# Nome EXATO do arquivo da planilha (tem que estar no mesmo diretório do app)
ARQ_PLANILHA = "tabela de frete atualizada(2)(Recuperado Automaticamente).xlsx"

# Chaves a ignorar quando formos varrer as planilhas (títulos, notas, etc.)
PALAVRAS_IGNORAR = {"VALOR KM", "TAMANHO CAMINHAO", "TAMANHO CAMINHÃO",
                    "CALCULO DE FRETE POR TAMANHO DE PEÇA",
                    "CÁLCULO DE FRETE POR TAMANHO DE PEÇA"}

# Defaults caso a planilha não traga os valores
DEFAULT_VALOR_KM = 7.0
DEFAULT_TAM_CAMINHAO = 8.5  # em metros

app = Flask(__name__)

# ==========================
# LEITURA E PRÉ-PROCESSAMENTO DA PLANILHA
# ==========================

def extrai_constante(sheet_raw: pd.DataFrame, chave: str, default: float) -> float:
    """
    Procura por uma linha que contenha 'chave' (case-insensitive) e
    tenta retornar o primeiro número daquela linha.
    """
    chave = chave.upper()
    for _, row in sheet_raw.iterrows():
        textos = [str(v).strip() for v in row if isinstance(v, str)]
        if any(chave in t.upper() for t in textos):
            nums = [v for v in row if isinstance(v, (int, float))]
            if nums:
                try:
                    return float(nums[0])
                except Exception:
                    pass
    return default

def carregar_constantes(xls: pd.ExcelFile) -> Dict[str, float]:
    try:
        raw_d = pd.read_excel(xls, "D", header=None)
    except ValueError:
        # Se não existir a sheet "D", tenta "BASE_CALCULO" e usa defaults se não achar
        raw_d = pd.read_excel(xls, "BASE_CALCULO", header=None)
    valor_km = extrai_constante(raw_d, "VALOR KM", DEFAULT_VALOR_KM)
    tam_caminhao = extrai_constante(raw_d, "TAMANHO CAMINHAO", DEFAULT_TAM_CAMINHAO)
    return {"VALOR_KM": valor_km, "TAM_CAMINHAO": tam_caminhao}

def limpar_texto(nome: Any) -> str:
    if not isinstance(nome, str):
        return ""
    return " ".join(nome.replace("\n", " ").split()).strip()

def carregar_cadastro_produtos(xls: pd.ExcelFile) -> pd.DataFrame:
    """
    Lê a sheet CADASTRO_PRODUTO e retorna DataFrame com colunas:
    ['nome', 'dim1', 'dim2'] (float). Linhas inválidas são descartadas.
    A função é tolerante a variações de colunas e cabeçalhos.
    """
    raw = pd.read_excel(xls, "CADASTRO_PRODUTO", header=None)

    # Heurística: normalmente, a 3ª coluna é o NOME (índice 2),
    # e a 4ª/5ª são as dimensões (índices 3 e 4).
    # Mas deixamos robusto: se não houver colunas suficientes, tentamos reidentificar.
    cols = list(range(raw.shape[1]))
    nome_col = 2 if len(cols) > 2 else 0
    dim1_col = 3 if len(cols) > 3 else (1 if len(cols) > 1 else 0)
    dim2_col = 4 if len(cols) > 4 else (2 if len(cols) > 2 else 1)

    df = raw[[nome_col, dim1_col, dim2_col]].copy()
    df.columns = ["nome", "dim1", "dim2"]

    # Limpa textos e ignora cabeçalhos/linhas com palavras-chave
    df["nome"] = df["nome"].apply(limpar_texto)
    df = df[~df["nome"].str.upper().isin(PALAVRAS_IGNORAR)]
    df = df[df["nome"].astype(str).str.len() > 0]

    # Converte dimensões para número e descarta linhas que não possuem nenhum valor numérico
    df["dim1"] = pd.to_numeric(df["dim1"], errors="coerce")
    df["dim2"] = pd.to_numeric(df["dim2"], errors="coerce")
    df = df.dropna(subset=["dim1", "dim2"], how="all")

    # Para valores faltantes, usa 0.0 (será tratado depois com max())
    df["dim1"] = df["dim1"].fillna(0.0)
    df["dim2"] = df["dim2"].fillna(0.0)

    # Remove duplicados mantendo a primeira ocorrência
    df = df.drop_duplicates(subset=["nome"], keep="first").reset_index(drop=True)
    return df[["nome", "dim1", "dim2"]]

def tipo_produto(nome: str) -> str:
    n = (nome or "").lower()
    if "fossa" in n:
        return "fossa"
    if "vertical" in n:
        return "vertical"
    if "horizontal" in n:
        return "horizontal"
    if "tc" in n and ("10.000" in n or "10000" in n):
        return "tc_ate_10k"
    return "auto"

def tamanho_peca_por_nome(nome: str, dim1: float, dim2: float) -> float:
    """
    Aplica as regras da sua tabela:
      - Vertical -> usar ALTURA (dim1)
      - Horizontal -> usar LARGURA/DIÂMETRO (dim2)
      - Fossa poli -> ALTURA (dim1)
      - TC abaixo de 10.000 L -> LARGURA MAIOR (dim2)
      - auto -> maior das dimensões fornecidas
    """
    t = tipo_produto(nome)
    if t == "fossa":
        return float(dim1)
    if t == "vertical":
        return float(dim1)
    if t == "horizontal":
        return float(dim2)
    if t == "tc_ate_10k":
        return float(dim2)
    return float(max(float(dim1 or 0.0), float(dim2 or 0.0)))

def montar_catalogo_tamanho(df: pd.DataFrame) -> Dict[str, float]:
    mapa: Dict[str, float] = {}
    for _, r in df.iterrows():
        try:
            nome = limpar_texto(r["nome"])
            if not nome or nome.upper() in PALAVRAS_IGNORAR:
                continue
            dim1 = float(r["dim1"] or 0.0)
            dim2 = float(r["dim2"] or 0.0)
            tam = tamanho_peca_por_nome(nome, dim1, dim2)
            if tam > 0:
                mapa[nome] = tam
        except Exception:
            # ignora linhas problemáticas
            continue
    return mapa

def carregar_tudo() -> Dict[str, Any]:
    """
    Carrega planilha, constantes e catálogo de tamanhos.
    Executado no startup do app.
    """
    xls = pd.ExcelFile(ARQ_PLANILHA)
    consts = carregar_constantes(xls)
    cadastro = carregar_cadastro_produtos(xls)
    catalogo = montar_catalogo_tamanho(cadastro)
    return {"consts": consts, "catalogo": catalogo}

# Carrega ao iniciar a app
try:
    DATA = carregar_tudo()
except Exception as e:
    # Se der erro na inicialização, mantenha valores seguros e deixe endpoint explicar
    DATA = {"consts": {"VALOR_KM": DEFAULT_VALOR_KM, "TAM_CAMINHAO": DEFAULT_TAM_CAMINHAO},
            "catalogo": {}}
    print(f"[WARN] Falha ao carregar planilha: {e}")

# ==========================
# LÓGICA DE CÁLCULO
# ==========================

def obter_km(cep_origem: str, cep_destino: str, km_param: str) -> float:
    """
    Enquanto não plugar a tabela de CEP→km, aceita ?km= para testes.
    Se vazio, usa 100 km como fallback.
    """
    if km_param:
        try:
            return max(1.0, float(km_param))
        except Exception:
            pass
    return 100.0

def calcula_valor_item(tamanho_peca_m: float, km: float, valor_km: float, tam_caminhao: float) -> float:
    """
    Fórmula da sua planilha:
      ocupação = tamanho_peca_m / tam_caminhao
      valor_por_km = VALOR_KM * ocupação
      valor_total_item = valor_por_km * km
    """
    ocupacao = max(0.01, float(tamanho_peca_m) / float(tam_caminhao))
    valor_km_item = float(valor_km) * ocupacao
    return round(valor_km_item * float(km), 2)

def parse_prods(prods_str: str) -> List[Dict[str, Any]]:
    """
    Formato “compatível Tray”:
      comp;larg;alt;cub;qty;peso;codigo;valor
    Itens separados por '/' normalmente. Também aceitamos '|'.
    Converte vírgula decimal para ponto e 'null'/'None' para 0.
    """
    itens: List[Dict[str, Any]] = []
    if not prods_str:
        return itens

    # Normaliza separadores de item
    blocos = []
    for sep in ["/", "|"]:
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
        # vírgula -> ponto
        s = s.replace(",", ".")
        try:
            return float(s)
        except Exception:
            return 0.0

    for raw in blocos:
        try:
            comp, larg, alt, cub, qty, peso, codigo, valor = raw.split(";")
            item = {
                "comp": norm_num(comp),
                "larg": norm_num(larg),
                "alt":  norm_num(alt),
                "cub":  norm_num(cub),
                "qty":  int(norm_num(qty)) if norm_num(qty) > 0 else 1,
                "peso": norm_num(peso),
                "codigo": (codigo or "").strip(),
                "valor": norm_num(valor),
            }
            itens.append(item)
        except Exception:
            # ignora linhas zoadas
            continue
    return itens

# ==========================
# ENDPOINTS
# ==========================

@app.route("/health", methods=["GET"])
def health():
    return {"ok": True, "valores": DATA["consts"], "itens_catalogo": len(DATA["catalogo"])}

@app.route("/frete", methods=["GET"])
def frete():
    # 1) valida token
    token = request.args.get("token", "")
    if token != TOKEN_SECRETO:
        return Response("Token inválido", status=403)

    # 2) parâmetros principais
    cep_origem = request.args.get("cep", "")
    cep_dest = request.args.get("cep_destino", "")
    prods = request.args.get("prods", "")
    km_param = request.args.get("km", "")

    if not cep_origem or not cep_dest or not prods:
        return Response("Parâmetros insuficientes", status=400)

    itens = parse_prods(prods)
    if not itens:
        return Response("Nenhum item válido em 'prods'", status=400)

    km = obter_km(cep_origem, cep_dest, km_param)
    valor_km = DATA["consts"].get("VALOR_KM", DEFAULT_VALOR_KM)
    tam_caminhao = DATA["consts"].get("TAM_CAMINHAO", DEFAULT_TAM_CAMINHAO)

    total_base = 0.0
    itens_xml = []

    for it in itens:
        # Estratégia de identificação do produto no catálogo:
        # 1) tenta nome/código exatamente como veio
        # 2) se não achar, usa heurística de dimensões (vertical = alt; horizontal = larg; auto = max)
        nome = it["codigo"]
        tam_catalogo = DATA["catalogo"].get(nome)

        if tam_catalogo is None:
            # Fallback: usa as dimensões do parâmetro para decidir
            tam_catalogo = tamanho_peca_por_nome(nome, it["alt"], it["larg"])

        valor_item = calcula_valor_item(tam_catalogo, km, valor_km, tam_caminhao) * max(1, it["qty"])
        total_base += valor_item

        itens_xml.append(f"""
      <item>
        <codigo>{nome}</codigo>
        <tamanho_controle>{tam_catalogo:.3f}</tamanho_controle>
        <km>{km:.1f}</km>
        <valor>{valor_item:.2f}</valor>
      </item>""")

    # Monte 2 serviços (econômico e expresso) com multiplicadores diferentes
    servicos = [
        ("ECON", "Econômico", 1.00, 4, 7),
        ("EXPR", "Expresso", 1.20, 1, 3),
    ]

    resultados = []
    for cod, nome, mult, pmin, pmax in servicos:
        valor = round(total_base * mult, 2)
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


# ==========================
# RODAR LOCALMENTE (opcional)
# ==========================
if __name__ == "__main__":
    # Para debug local: http://127.0.0.1:8000/health
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8000")), debug=True)

