# app.py — WebService de Frete p/ TRAY (comunicação garantida)
# - Rota /frete (principal) e / (delegando)
# - XML no formato Tray (<frete><servicos><servico>...)
# - Content-Type correto (text/xml; charset=utf-8)
# - Nunca retorna vazio; em erro retorna <servico> com erro=1
# - Faixas de km (100 em 100) | modo arredondar ou tabela

import os, math, re
from typing import Dict, Any, List, Tuple, Optional
from flask import Flask, request, Response, jsonify

# ====== .env opcional (dev local) ======
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

app = Flask(__name__)

# ==========================
# CONFIG
# ==========================
TOKEN_SECRETO   = os.getenv("TOKEN_SECRETO", "")   # deixe "" para não exigir token
DEFAULT_VALOR_KM = float(os.getenv("DEFAULT_VALOR_KM", "7.0"))
DEFAULT_TAM_CAM  = float(os.getenv("DEFAULT_TAM_CAMINHAO", "8.5"))
MIN_FRETE        = float(os.getenv("MIN_FRETE", "120.0"))

# Faixas de distância
FAIXA_KM_LARGURA = int(os.getenv("FAIXA_KM_LARGURA", "100"))           # 100, 200, etc.
MODO_FAIXA       = os.getenv("MODO_FAIXA", "arredondar").lower()       # "arredondar" | "tabela"
TABELA_FAIXAS    = os.getenv("TABELA_FAIXAS", "").strip()              # ex: "0-100:180;101-200:260;201-300:350"

# Fallback por UF (quando nada mais definir km)
UF_RANGES = [
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
KM_APROX_POR_UF = {
    "RS":150,"SC":450,"PR":700,"SP":1100,"RJ":1500,"MG":1600,"ES":1800,
    "MS":1600,"MT":2200,"DF":2000,"GO":2100,"TO":2500,"BA":2600,"SE":2700,
    "AL":2800,"PE":3000,"PB":3100,"RN":3200,"CE":3400,"PI":3300,"MA":3500,
    "PA":3800,"AP":4100,"AM":4200,"RO":4000,"AC":4300,"RR":4500,
}
DEFAULT_KM = float(os.getenv("DEFAULT_KM", "450.0"))

# ==========================
# UTILS / PARSE
# ==========================
def digitos8(cep: Any) -> str:
    s = re.sub(r"\D", "", str(cep or ""))
    return (s[:8] if len(s) >= 8 else s.zfill(8)) if s else "00000000"

def uf_por_cep(cep8: str) -> Optional[str]:
    try:
        n = int(cep8)
    except:
        return None
    for uf, a, b in UF_RANGES:
        if int(a) <= n <= int(b):
            return uf
    return None

def arredonda_faixa_km(km: float, largura: int = FAIXA_KM_LARGURA) -> Tuple[int,int,int]:
    """Retorna (km_topo, faixa_ini, faixa_fim). 138 -> (200, 101, 200)"""
    if km <= 0: return (largura, 0 if largura==100 else 1, largura)
    blocos = math.ceil(km / largura)
    topo = blocos * largura
    ini = 0 if topo==largura else (topo - largura + 1)
    fim = topo
    return (topo, ini, fim)

def parse_tabela_faixas(cfg: str) -> List[Tuple[int,int,float]]:
    out: List[Tuple[int,int,float]] = []
    if not cfg: return out
    for bloco in cfg.split(";"):
        bloco = bloco.strip()
        if not bloco: continue
        try:
            rng, preco = bloco.split(":")
            a,b = rng.split("-")
            a = int(a.strip()); b = int(b.strip())
            preco = float(str(preco).replace(",", "."))
            if a >= 0 and b >= a and preco >= 0:
                out.append((a,b,preco))
        except:
            continue
    return sorted(out, key=lambda x: (x[0], x[1]))

TABELA = parse_tabela_faixas(TABELA_FAIXAS)

def preco_faixa_por_tabela(km: float) -> Optional[Tuple[float,Tuple[int,int]]]:
    if MODO_FAIXA != "tabela" or not TABELA:
        return None
    k = int(math.ceil(km))
    for a,b,p in TABELA:
        if a <= k <= b:
            return (p,(a,b))
    return None

def parse_prods(prods: str) -> List[Dict[str,Any]]:
    """
    Formato Tray típico por item: comp;larg;alt;cub;qty;peso;codigo;valor
    Itens separados por "|" ou "/"
    """
    if not prods: return []
    sep = "|" if "|" in prods else ("/" if "/" in prods else None)
    blocos = prods.split(sep) if sep else [prods]

    def n(x):
        s = str(x or "").strip().lower().replace(",", ".")
        try: return float(s)
        except: return 0.0

    def cm_to_m(v):
        return v/100.0 if v and v > 20 else (v or 0.0)

    itens = []
    for raw in blocos:
        partes = raw.split(";")
        if len(partes) < 8:  # protege contra item inválido
            continue
        comp, larg, alt, cub, qty, peso, codigo, valor = partes[:8]
        itens.append({
            "comp": cm_to_m(n(comp)),
            "larg": cm_to_m(n(larg)),
            "alt":  cm_to_m(n(alt)),
            "cub":  n(cub),
            "qty":  int(n(qty)) if n(qty) > 0 else 1,
            "peso": n(peso),
            "codigo": (codigo or "").strip(),
            "valor": n(valor),
        })
    return itens

def money(v: float) -> str:
    return f"{v:.2f}"

# ==========================
# CÁLCULO
# ==========================
def calcula_valor_item(ocup_ref: float, km_ref: float, valor_km: float, tam_cam: float) -> float:
    if ocup_ref <= 0 or tam_cam <= 0: return 0.0
    ocupacao = ocup_ref / tam_cam
    return round(valor_km * km_ref * ocupacao, 2)

def calcular_prazo(km_ref: float) -> int:
    if km_ref <= 100: return 2
    if km_ref <= 250: return 3
    if km_ref <= 500: return 5
    if km_ref <= 900: return 7
    if km_ref <= 1400: return 10
    return 15

def determina_km(cep_dest: str) -> Tuple[float,str]:
    """Aqui você pode plugar regras por município/faixas da planilha. Por enquanto: UF > default."""
    d = digitos8(cep_dest)
    uf = uf_por_cep(d)
    if uf and uf in KM_APROX_POR_UF:
        return float(KM_APROX_POR_UF[uf]), f"uf_{uf}"
    return DEFAULT_KM, "default"

# ==========================
# XML (TRAY)
# ==========================
def xml_ok(nome: str, codigo: str, valor: float, prazo: int, obs: str = "") -> str:
    # Formato esperado pela Tray
    obs = (obs or "").replace("&", "e")
    return f"""<?xml version="1.0" encoding="utf-8"?>
<frete>
  <servicos>
    <servico>
      <nome>{nome}</nome>
      <codigo>{codigo}</codigo>
      <valor>{money(valor)}</valor>
      <prazo>{prazo}</prazo>
      <erro>0</erro>
      <msg_erro></msg_erro>
      <obs>{obs}</obs>
    </servico>
  </servicos>
</frete>"""

def xml_erro(msg: str) -> str:
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
@app.route("/", methods=["GET"])
def raiz():
    # Se a Tray chamar a raiz com params, delega p/ /frete
    if any(k in request.args for k in ("cep", "cep_destino", "prods")):
        return frete()
    return jsonify({
        "api": "Bakof Frete (Tray)",
        "use": "/frete?cep=...&cep_destino=...&prods=comp;larg;alt;cub;qty;peso;codigo;valor|...",
        "modo_faixa": MODO_FAIXA,
        "faixa_km": FAIXA_KM_LARGURA
    })

@app.route("/frete", methods=["GET"])
def frete():
    try:
        # --- LOG básico pra debug ---
        print("[TRAY] params:", dict(request.args))

        # Token (opcional)
        token = request.args.get("token", "")
        if TOKEN_SECRETO and token != TOKEN_SECRETO:
            return Response(xml_erro("Token inválido"), status=403, mimetype="text/xml; charset=utf-8")

        # Parâmetros da Tray
        cep = request.args.get("cep", "")                   # cep de origem (pode vir vazio; não usamos aqui)
        cep_dest = request.args.get("cep_destino", "") or request.args.get("cep", "")
        prods = request.args.get("prods", "")
        # Extras ignorados, mas presentes: peso, envio, num_pedido, etc.

        if not cep_dest:
            return Response(xml_erro("CEP destino não informado"), mimetype="text/xml; charset=utf-8")

        itens = parse_prods(prods) if prods else []
        if not itens and MODO_FAIXA != "tabela":
            # No modo tabela, podemos cotar sem itens (preço fechado por faixa)
            return Response(xml_erro("Produtos não informados"), mimetype="text/xml; charset=utf-8")

        # Constantes
        valor_km = DEFAULT_VALOR_KM
        tam_cam  = DEFAULT_TAM_CAM
        try:
            if request.args.get("valor_km"): valor_km = float(str(request.args["valor_km"]).replace(",", "."))
            if request.args.get("tam_caminhao"): tam_cam = float(str(request.args["tam_caminhao"]).replace(",", "."))
        except: pass

        # KM base (UF / default)
        km, fonte = determina_km(cep_dest)

        # Aplica faixa de KM
        km_topo, faixa_ini, faixa_fim = arredonda_faixa_km(km, FAIXA_KM_LARGURA)

        # MODO TABELA: preço fixo por faixa
        tabela = preco_faixa_por_tabela(km)
        if tabela:
            preco, (fa_i, fa_f) = tabela
            total = max(MIN_FRETE, float(preco))
            prazo = calcular_prazo(km_topo)
            obs = f"modo=tabela; faixa={fa_i}-{fa_f}km; km_ref={km} ({fonte})"
            return Response(xml_ok("BK-EXPRESSO", "BKX", total, prazo, obs), mimetype="text/xml; charset=utf-8")

        # MODO ARREDONDAR: usa km_topo na fórmula
        total = 0.0
        for it in itens:
            # ocupação de referência: usa o maior entre dimensão linear (comp/larg/alt) e cubagem (m³)
            ocup_ref = max(it.get("comp",0.0), it.get("larg",0.0), it.get("alt",0.0), it.get("cub",0.0))
            total += calcula_valor_item(ocup_ref, km_topo, valor_km, tam_cam) * max(1, int(it.get("qty",1)))

        total = max(total, MIN_FRETE)
        prazo = calcular_prazo(km_topo)
        obs = f"modo=arredondar; faixa={faixa_ini}-{faixa_fim}km; km_ref={km} ({fonte}); km_faixa={km_topo}"
        return Response(xml_ok("BK-EXPRESSO", "BKX", total, prazo, obs), mimetype="text/xml; charset=utf-8")

    except Exception as e:
        print("[TRAY][ERRO]", e)
        return Response(xml_erro(f"Falha interna: {e}"), status=500, mimetype="text/xml; charset=utf-8")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT","8000")))
