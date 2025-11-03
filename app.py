# app.py
import os
import math
from typing import Dict, Any, List
import pandas as pd
from flask import Flask, request, Response

# ==========================
# CONFIG
# ==========================
TOKEN_SECRETO = os.getenv("TOKEN_SECRETO", "MINHA_CHAVE_FORTE")
ARQ_PLANILHA = "tabela de frete atualizada(2)(Recuperado Automaticamente).xlsx"

DEFAULT_VALOR_KM = 7.0
DEFAULT_TAM_CAMINHAO = 8.5  # metros

PALAVRAS_IGNORAR = {
    "VALOR KM",
    "TAMANHO CAMINHAO",
    "TAMANHO CAMINHÃO",
    "CALCULO DE FRETE POR TAMANHO DE PEÇA",
    "CÁLCULO DE FRETE POR TAMANHO DE PEÇA",
}

app = Flask(__name__)

# ==========================
# LEITURA DA PLANILHA
# ==========================
def extrai_constante(sheet_raw: pd.DataFrame, chave: str, default: float) -> float:
    chave = chave.upper()
    for _, row in sheet_raw.iterrows():
        textos = [str(v).strip() for v in row if isinstance(v, str)]
        if any(chave in t.upper() for t in textos):
            nums = []
            for v in row:
                if isinstance(v, (int, float)):
                    try:
                        fv = float(v)
                        if math.isfinite(fv):
                            nums.append(fv)
                    except Exception:
                        pass
            if nums:
                val = nums[0]
                if math.isfinite(val) and val > 0:
                    return float(val)
    return float(default)

def carregar_constantes(xls: pd.ExcelFile) -> Dict[str, float]:
    try:
        raw = pd.read_excel(xls, "D", header=None)
    except ValueError:
        raw = pd.read_excel(xls, "BASE_CALCULO", header=None)

    return {
        "VALOR_KM": extrai_constante(raw, "VALOR KM", DEFAULT_VALOR_KM),
        "TAM_CAMINHAO": extrai_constante(raw, "TAMANHO CAMINHAO", DEFAULT_TAM_CAMINHAO),
    }

def limpar_texto(nome: Any) -> str:
    if not isinstance(nome, str):
        return ""
    return " ".join(nome.replace("\n", " ").split()).strip()

def carregar_cadastro_produtos(xls: pd.ExcelFile) -> pd.DataFrame:
    raw = pd.read_excel(xls, "CADASTRO_PRODUTO", header=None)
    cols = list(range(raw.shape[1]))
    nome_col = 2 if len(cols) > 2 else 0
    dim1_col = 3 if len(cols) > 3 else (1 if len(cols) > 1 else 0)
    dim2_col = 4 if len(cols) > 4 else (2 if len(cols) > 2 else 1)

    df = raw[[nome_col, dim1_col, dim2_col]].copy()
    df.columns = ["nome", "dim1", "dim2"]

    df["nome"] = df["nome"].apply(limpar_texto)
    df = df[~df["nome"].str.upper().isin(PALAVRAS_IGNORAR)]
    df = df[df["nome"].astype(str).str.len() > 0]

    df["dim1"] = pd.to_numeric(df["dim1"], errors="coerce").fillna(0.0)
    df["dim2"] = pd.to_numeric(df["dim2"], errors="coerce").fillna(0.0)

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
    t = tipo_produto(nome)
    if t in ("fossa", "vertical"):
        return float(dim1 or 0.0)
    if t in ("horizontal", "tc_ate_10k"):
        return float(dim2 or 0.0)
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
            continue
    return mapa

def carregar_tudo() -> Dict[str, Any]:
    xls = pd.ExcelFile(ARQ_PLANILHA)
    consts = carregar_constantes(xls)
    cadastro = carregar_cadastro_produtos(xls)
    catalogo = montar_catalogo_tamanho(cadastro)
    return {"consts": consts, "catalogo": catalogo}

try:
    DATA = carregar_tudo()
except Exception as e:
    DATA = {"consts": {"VALOR_KM": DEFAULT_VALOR_KM, "TAM_CAMINHAO": DEFAULT_TAM_CAMINHAO}, "catalogo": {}}
    print(f"[WARN] Falha ao carregar planilha: {e}")

# ==========================
# CÁLCULO
# ==========================
def obter_km(cep_origem: str, cep_destino: str, km_param: str) -> float:
    if km_param:
        try:
            return max(1.0, float(str(km_param).replace(",", ".")))
        except Exception:
            pass
    return 100.0

def calcula_valor_item(tamanho_peca_m: float, km: float, valor_km: float, tam_caminhao: float) -> float:
    ocupacao = max(0.01, float(tamanho_peca_m) / float(tam_caminhao))
    valor_km_item = float(valor_km) * ocupacao
    return round(valor_km_item * float(km), 2)

def parse_prods(prods_str: str) -> List[Dict[str, Any]]:
    """
    comp;larg;alt;cub;qty;peso;codigo;valor
    Itens separados por '/' (ou '|'). Aceita vírgula decimal e 'null'.
    """
    itens: List[Dict[str, Any]] = []
    if not prods_str:
        return itens

    # separador dos itens
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
            continue
    return itens

# ==========================
# ENDPOINTS
# ==========================
@app.route("/health", methods=["GET"])
def health():
    return {
        "ok": True,
        "valores": DATA["consts"],
        "itens_catalogo": len(DATA["catalogo"]),
    }

@app.route("/frete", methods=["GET"])
def frete():
    # token
    token = request.args.get("token", "")
    if token != TOKEN_SECRETO:
        return Response("Token inválido", status=403)

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

    # saneia
    if not isinstance(valor_km, (int, float)) or not math.isfinite(valor_km) or valor_km <= 0:
        valor_km = DEFAULT_VALOR_KM
    if not isinstance(tam_caminhao, (int, float)) or not math.isfinite(tam_caminhao) or tam_caminhao <= 0:
        tam_caminhao = DEFAULT_TAM_CAMINHAO

    # overrides p/ teste
    try:
        if request.args.get("valor_km"):
            valor_km = float(str(request.args["valor_km"]).replace(",", "."))
        if request.args.get("tam_caminhao"):
            tam_caminhao = float(str(request.args["tam_caminhao"]).replace(",", "."))
    except Exception:
        pass

    total_base = 0.0
    itens_xml = []

    for it in itens:
        nome = it["codigo"]
        tam_catalogo = DATA["catalogo"].get(nome)
        if tam_catalogo is None:
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
# LOCAL
# ==========================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8000")), debug=True)
