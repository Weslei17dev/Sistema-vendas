# ╔══════════════════════════════════════════════════════════════════╗
#  ERP SaaS Multi-tenant  |  PostgreSQL  |  v8.0
#  Melhorias: navegação instantânea (st.query_params), comissões,
#  PDF de pedido, log de atividades, UX mobile-first para vendedores,
#  dashboard com gráficos, alertas de estoque por e-mail.
# ╚══════════════════════════════════════════════════════════════════╝

import streamlit as st
import psycopg2
from psycopg2 import pool as pg_pool, errors as pg_errors
import hashlib, re, io, smtplib
from email.mime.text import MIMEText
from datetime import datetime, date, timedelta
from contextlib import contextmanager

try:
    import pandas as pd
    import openpyxl
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

try:
    import plotly.express as px
    import plotly.graph_objects as go
    HAS_PLOTLY = True
except ImportError:
    HAS_PLOTLY = False

# ──────────────────────────────────────────────────────────────
#  PAGE CONFIG — mobile-first
# ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Gestão ERP Pro",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",   # ← colapsa no mobile por padrão
    menu_items={"About": "ERP SaaS v8.0"},
)

# ──────────────────────────────────────────────────────────────
#  ESTILOS — mobile-first refinado
# ──────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:ital,wght@0,300;0,400;0,500;0,600;1,400&family=Sora:wght@400;600;700&display=swap');

/* ── Base ── */
html,body,[class*="css"]{font-family:'DM Sans',sans-serif;}
*, *::before, *::after{box-sizing:border-box;}

/* ── Sidebar ── */
section[data-testid="stSidebar"]{
    background:linear-gradient(170deg,#0d0d1f 0%,#161628 55%,#1a1635 100%);
    border-right:1px solid rgba(255,255,255,0.05);
    min-width:220px !important;
}
section[data-testid="stSidebar"] *{color:rgba(255,255,255,0.88)!important;}
.erp-brand{font-family:'Sora',sans-serif;font-size:1.05rem;font-weight:700;
    color:#fff!important;padding:.15rem 0 .5rem;letter-spacing:-.01em;}
.erp-brand span{color:#a78bfa!important;}
.erp-tenant{font-size:.68rem;color:rgba(255,255,255,0.38)!important;
    padding-bottom:.6rem;letter-spacing:.03em;}
.nav-section-title{font-size:.6rem;font-weight:600;letter-spacing:.14em;
    text-transform:uppercase;color:rgba(255,255,255,0.28)!important;padding:.65rem 0 .28rem;}
.stButton>button{width:100%;text-align:left;padding:.48rem .8rem;border-radius:9px;
    border:none;background:transparent;color:rgba(255,255,255,0.72)!important;
    font-size:.86rem;transition:background .15s,transform .1s;margin-bottom:1px;}
.stButton>button:hover{background:rgba(167,139,250,.14)!important;color:#fff!important;transform:translateX(3px);}
.nav-active button{background:rgba(124,58,237,.3)!important;
    color:#ddd6fe!important;font-weight:600;border-left:3px solid #a78bfa;}
.logout-btn button{background:rgba(239,68,68,.1)!important;color:#fca5a5!important;border-radius:8px;}

/* ── Métricas ── */
[data-testid="stMetric"]{
    background:linear-gradient(135deg,#fafbff 0%,#f3f4ff 100%);
    border:1px solid #e0e4ff;border-radius:16px;padding:1rem 1.15rem;
    transition:transform .15s,box-shadow .15s;}
[data-testid="stMetric"]:hover{transform:translateY(-2px);box-shadow:0 6px 20px rgba(99,102,241,.1);}
[data-testid="stMetricValue"]{font-family:'Sora',sans-serif;font-size:1.55rem;color:#1e1b4b;}
[data-testid="stMetricLabel"]{font-size:.76rem;color:#6b7280;font-weight:500;}
[data-testid="stMetricDelta"]{font-size:.75rem;}

/* ── Formulários & Expanders ── */
[data-testid="stForm"]{background:#fafbff;border:1px solid #eef0ff;border-radius:14px;padding:1.2rem;}
[data-testid="stExpander"]{border:1px solid #eef0ff!important;border-radius:12px!important;}
[data-testid="stExpander"] summary:hover{background:rgba(99,102,241,.04);}

/* ── Inputs mobile ── */
[data-testid="stTextInput"] input,
[data-testid="stNumberInput"] input,
[data-testid="stSelectbox"] select,
textarea{font-size:16px !important;} /* evita zoom no iOS */

/* ── Toasts ── */
.erp-toast{padding:.65rem .9rem;border-radius:10px;font-size:.86rem;
    margin:.3rem 0;display:flex;align-items:flex-start;gap:.5rem;
    animation:fadeIn .2s ease;}
@keyframes fadeIn{from{opacity:0;transform:translateY(-4px)}to{opacity:1;transform:none}}
.erp-toast.success{background:#f0fdf4;border-left:4px solid #22c55e;color:#15803d;}
.erp-toast.error{background:#fef2f2;border-left:4px solid #ef4444;color:#b91c1c;}
.erp-toast.warning{background:#fffbeb;border-left:4px solid #f59e0b;color:#b45309;}
.erp-toast.info{background:#eff6ff;border-left:4px solid #3b82f6;color:#1d4ed8;}
.erp-toast .icon{font-size:.95rem;flex-shrink:0;margin-top:2px;}
.erp-toast .body strong{display:block;font-weight:600;margin-bottom:1px;}
.erp-toast .body span{opacity:.85;}

/* ── Page header ── */
.page-header{display:flex;align-items:center;gap:.65rem;margin-bottom:1.3rem;}
.page-header .icon{width:36px;height:36px;border-radius:10px;display:flex;
    align-items:center;justify-content:center;font-size:1.05rem;
    background:linear-gradient(135deg,#6366f1,#8b5cf6);flex-shrink:0;}
.page-header h1{font-family:'Sora',sans-serif;font-size:1.25rem;font-weight:700;
    margin:0;color:#1e1b4b;letter-spacing:-.02em;}
.page-header p{font-size:.8rem;color:#6b7280;margin:0;}
hr.erp{border:none;border-top:1px solid #eef0ff;margin:1rem 0;}

/* ── Badges ── */
.badge{display:inline-block;padding:2px 7px;border-radius:99px;font-size:.7rem;font-weight:600;letter-spacing:.01em;}
.badge.ok{background:#dcfce7;color:#166534;}.badge.low{background:#fef9c3;color:#854d0e;}
.badge.zero{background:#fee2e2;color:#991b1b;}.badge.ativo{background:#dcfce7;color:#166534;}
.badge.inativo{background:#f1f5f9;color:#64748b;}.badge.pago{background:#dbeafe;color:#1e40af;}
.badge.cancelado{background:#fee2e2;color:#991b1b;}
.badge.pendente{background:#fef9c3;color:#854d0e;}

/* ── Cards ── */
.card{background:#fff;border:1px solid #eef0ff;border-radius:12px;
    padding:.75rem .95rem;margin-bottom:6px;transition:border-color .15s,box-shadow .15s;}
.card:hover{border-color:#c7d2fe;box-shadow:0 2px 12px rgba(99,102,241,.08);}
.card.inativo{opacity:.5;border-style:dashed;}
.card-title{font-size:.9rem;font-weight:600;color:#1e1b4b;margin:0 0 2px;}
.card-sub{font-size:.74rem;color:#6b7280;line-height:1.5;}
.card-val{font-family:'Sora',sans-serif;font-size:.98rem;font-weight:700;color:#6366f1;}
.card-sku{font-size:.67rem;color:#9ca3af;letter-spacing:.03em;}

/* ── Carrinho mobile-optimized ── */
.cart-item{display:flex;align-items:center;justify-content:space-between;
    padding:.5rem .65rem;border-radius:9px;margin-bottom:5px;
    background:#f8f9ff;border:1px solid #eef0ff;}
.ci-name{font-size:.84rem;font-weight:600;color:#1e1b4b;}
.ci-qty{font-size:.73rem;color:#6b7280;}
.ci-val{font-family:'Sora',sans-serif;font-size:.86rem;font-weight:700;color:#6366f1;white-space:nowrap;}
.cart-total{display:flex;justify-content:space-between;align-items:center;
    padding:.65rem .85rem;border-radius:10px;
    background:linear-gradient(135deg,#6366f1,#8b5cf6);color:#fff;margin-top:.6rem;}
.cart-total span{font-size:.82rem;opacity:.88;}
.cart-total strong{font-family:'Sora',sans-serif;font-size:1.1rem;}
.cart-desc{background:#f0f9ff;border:1px solid #bae6fd;border-radius:8px;
    padding:.45rem .65rem;margin-top:4px;font-size:.8rem;color:#0369a1;}

/* ── Aux rows ── */
.aux-row{display:flex;align-items:center;justify-content:space-between;
    padding:.5rem .8rem;border-radius:9px;margin-bottom:4px;
    background:#fff;border:1px solid #eef0ff;}
.aux-row.inativo{opacity:.48;border-style:dashed;}
.aux-row .ar-name{font-size:.86rem;font-weight:500;color:#1e1b4b;}

/* ── Log de atividades ── */
.log-item{display:flex;align-items:flex-start;gap:.55rem;padding:.45rem 0;
    border-bottom:.5px solid #f1f5f9;}
.log-dot{width:8px;height:8px;border-radius:50%;flex-shrink:0;margin-top:5px;}
.log-dot.venda{background:#6366f1;}.log-dot.estoque{background:#22c55e;}
.log-dot.cliente{background:#f59e0b;}.log-dot.sistema{background:#94a3b8;}
.log-dot.cancelamento{background:#ef4444;}
.log-text{font-size:.78rem;color:#374151;line-height:1.45;}
.log-time{font-size:.68rem;color:#9ca3af;margin-top:1px;}

/* ── PDF preview ── */
.pdf-preview{background:#fff;border:1px solid #e5e7eb;border-radius:12px;
    padding:1.5rem;font-family:'DM Sans',sans-serif;max-width:600px;margin:0 auto;}
.pdf-header{display:flex;justify-content:space-between;align-items:flex-start;
    margin-bottom:1rem;padding-bottom:.75rem;border-bottom:2px solid #6366f1;}
.pdf-title{font-family:'Sora',sans-serif;font-size:1.1rem;font-weight:700;color:#1e1b4b;}
.pdf-sub{font-size:.75rem;color:#6b7280;}
.pdf-table{width:100%;border-collapse:collapse;font-size:.82rem;margin:.75rem 0;}
.pdf-table th{background:#f8f9ff;padding:.4rem .6rem;text-align:left;
    font-weight:600;color:#374151;border-bottom:1px solid #e5e7eb;}
.pdf-table td{padding:.4rem .6rem;border-bottom:.5px solid #f1f5f9;color:#1e1b4b;}
.pdf-total{display:flex;justify-content:flex-end;margin-top:.5rem;}
.pdf-total-box{background:#f8f9ff;border:1px solid #eef0ff;border-radius:8px;
    padding:.5rem .85rem;text-align:right;}
.pdf-total-label{font-size:.75rem;color:#6b7280;}
.pdf-total-val{font-family:'Sora',sans-serif;font-size:1.15rem;font-weight:700;color:#6366f1;}

/* ── Login ── */
.login-wrap{max-width:400px;margin:3.5rem auto 0;padding:2rem;
    background:#fff;border:1px solid #e8eaff;border-radius:20px;
    box-shadow:0 8px 40px rgba(99,102,241,.08);}
.login-logo{text-align:center;font-family:'Sora',sans-serif;font-size:1.5rem;
    font-weight:700;color:#1e1b4b;margin-bottom:.2rem;}
.login-logo span{color:#6366f1;}
.login-sub{text-align:center;font-size:.81rem;color:#6b7280;margin-bottom:1.4rem;}

/* ── Comissão ── */
.comm-card{background:linear-gradient(135deg,#f0fdf4,#dcfce7);
    border:1px solid #86efac;border-radius:12px;padding:.75rem .95rem;margin-bottom:6px;}
.comm-name{font-size:.9rem;font-weight:600;color:#166534;}
.comm-val{font-family:'Sora',sans-serif;font-size:1.1rem;font-weight:700;color:#15803d;}
.comm-sub{font-size:.73rem;color:#4ade80;}

/* ── Mobile overrides ── */
@media(max-width:640px){
    .page-header h1{font-size:1.05rem;}
    [data-testid="stMetricValue"]{font-size:1.2rem;}
    .card{padding:.6rem .75rem;}
    .cart-total strong{font-size:.98rem;}
    [data-testid="stForm"]{padding:.9rem;}
    .pdf-preview{padding:1rem;}
}
</style>
""", unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────
#  UI HELPERS
# ──────────────────────────────────────────────────────────────
def show_error(t, h=""):
    st.markdown(f'<div class="erp-toast error"><div class="icon">✕</div>'
                f'<div class="body"><strong>{t}</strong><span>{h}</span></div></div>',
                unsafe_allow_html=True)

def show_success(t, h=""):
    st.markdown(f'<div class="erp-toast success"><div class="icon">✓</div>'
                f'<div class="body"><strong>{t}</strong><span>{h}</span></div></div>',
                unsafe_allow_html=True)

def show_warning(t, h=""):
    st.markdown(f'<div class="erp-toast warning"><div class="icon">⚠</div>'
                f'<div class="body"><strong>{t}</strong><span>{h}</span></div></div>',
                unsafe_allow_html=True)

def show_info(t, h=""):
    st.markdown(f'<div class="erp-toast info"><div class="icon">ℹ</div>'
                f'<div class="body"><strong>{t}</strong><span>{h}</span></div></div>',
                unsafe_allow_html=True)

def page_header(icon, title, subtitle=""):
    sub = f"<p>{subtitle}</p>" if subtitle else ""
    st.markdown(f'<div class="page-header"><div class="icon">{icon}</div>'
                f'<div><h1>{title}</h1>{sub}</div></div>', unsafe_allow_html=True)

def hr():
    st.markdown('<hr class="erp">', unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────
#  BANCO — Pool com reconexão automática
# ──────────────────────────────────────────────────────────────
@st.cache_resource
def get_pool():
    db = st.secrets["db"]
    dsn = (f"postgresql://{db['user']}:{db['password']}"
           f"@{db['host']}:{db.get('port', 5432)}/{db['dbname']}"
           f"?sslmode=require&channel_binding=disable")
    return pg_pool.ThreadedConnectionPool(1, 8, dsn, connect_timeout=10)

@contextmanager
def get_conn():
    pool = get_pool()
    conn = pool.getconn()
    try:
        yield conn
    except Exception:
        conn.rollback()
        raise
    finally:
        pool.putconn(conn)

def run_query(query, params=(), fetch=False, returning=False):
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                if fetch:
                    r = cur.fetchall(); conn.commit(); return r
                if returning:
                    r = cur.fetchone(); conn.commit(); return r
                conn.commit(); return True
    except pg_errors.UniqueViolation:
        return "duplicate"
    except Exception as e:
        st.session_state["_dberr"] = str(e)
        return False

def qry(sql, params=(), fetch=False, returning=False):
    return run_query(sql, params, fetch=fetch, returning=returning)

# ──────────────────────────────────────────────────────────────
#  INIT DB — Todas as tabelas + migrações
# ──────────────────────────────────────────────────────────────
def init_db():
    stmts = [
        # Core tenancy
        """CREATE TABLE IF NOT EXISTS empresas(
            id SERIAL PRIMARY KEY, nome TEXT NOT NULL,
            plano TEXT DEFAULT 'basico', ativo BOOLEAN DEFAULT TRUE,
            moeda TEXT DEFAULT 'R$', email_alerta TEXT DEFAULT '',
            criado_em TIMESTAMP DEFAULT NOW())""",
        """CREATE TABLE IF NOT EXISTS usuarios(
            id SERIAL PRIMARY KEY, empresa_id INTEGER NOT NULL REFERENCES empresas(id),
            nome TEXT NOT NULL, email TEXT UNIQUE NOT NULL, senha_hash TEXT NOT NULL,
            perfil TEXT DEFAULT 'operador', ativo BOOLEAN DEFAULT TRUE,
            criado_em TIMESTAMP DEFAULT NOW())""",
        # Equipe comercial
        """CREATE TABLE IF NOT EXISTS supervisores(
            id SERIAL PRIMARY KEY, empresa_id INTEGER NOT NULL REFERENCES empresas(id),
            nome TEXT NOT NULL, email TEXT, telefone TEXT,
            comissao_pct NUMERIC(5,2) DEFAULT 0,
            ativo BOOLEAN DEFAULT TRUE, UNIQUE(empresa_id,nome))""",
        """CREATE TABLE IF NOT EXISTS representantes(
            id SERIAL PRIMARY KEY, empresa_id INTEGER NOT NULL REFERENCES empresas(id),
            nome TEXT NOT NULL, email TEXT, telefone TEXT,
            supervisor_id INTEGER REFERENCES supervisores(id),
            comissao_pct NUMERIC(5,2) DEFAULT 0,
            ativo BOOLEAN DEFAULT TRUE, UNIQUE(empresa_id,nome))""",
        # Tabelas auxiliares
        """CREATE TABLE IF NOT EXISTS categorias(
            id SERIAL PRIMARY KEY, empresa_id INTEGER NOT NULL REFERENCES empresas(id),
            nome TEXT NOT NULL, ativo BOOLEAN DEFAULT TRUE, UNIQUE(empresa_id,nome))""",
        """CREATE TABLE IF NOT EXISTS pagamentos(
            id SERIAL PRIMARY KEY, empresa_id INTEGER NOT NULL REFERENCES empresas(id),
            nome TEXT NOT NULL, ativo BOOLEAN DEFAULT TRUE, UNIQUE(empresa_id,nome))""",
        # Clientes
        """CREATE TABLE IF NOT EXISTS clientes(
            id SERIAL PRIMARY KEY, empresa_id INTEGER NOT NULL REFERENCES empresas(id),
            nome TEXT NOT NULL, documento TEXT NOT NULL, telefone TEXT, email TEXT,
            rua TEXT, numero TEXT, complemento TEXT, bairro TEXT,
            cidade TEXT, estado TEXT, cep TEXT,
            ativo BOOLEAN DEFAULT TRUE, UNIQUE(empresa_id,documento))""",
        # Produtos
        """CREATE TABLE IF NOT EXISTS produtos(
            id SERIAL PRIMARY KEY, empresa_id INTEGER NOT NULL REFERENCES empresas(id),
            sku TEXT NOT NULL, nome TEXT NOT NULL, categoria TEXT NOT NULL,
            preco_custo NUMERIC(12,2) DEFAULT 0, preco_venda NUMERIC(12,2) DEFAULT 0,
            estoque_atual INTEGER DEFAULT 0, estoque_minimo INTEGER DEFAULT 2,
            ativo BOOLEAN DEFAULT TRUE, UNIQUE(empresa_id,sku))""",
        # Vendas
        """CREATE TABLE IF NOT EXISTS vendas(
            id SERIAL PRIMARY KEY, empresa_id INTEGER NOT NULL REFERENCES empresas(id),
            data TIMESTAMP NOT NULL DEFAULT NOW(), cliente_name TEXT,
            valor_bruto NUMERIC(12,2) DEFAULT 0,
            desconto_pct NUMERIC(5,2) DEFAULT 0,
            desconto_val NUMERIC(12,2) DEFAULT 0,
            valor_total NUMERIC(12,2),
            comissao_sup NUMERIC(12,2) DEFAULT 0,
            comissao_rep NUMERIC(12,2) DEFAULT 0,
            pagamento TEXT, status TEXT DEFAULT 'Pago',
            observacao TEXT DEFAULT '',
            supervisor_id INTEGER REFERENCES supervisores(id),
            representante_id INTEGER REFERENCES representantes(id),
            usuario_id INTEGER REFERENCES usuarios(id))""",
        """CREATE TABLE IF NOT EXISTS itens_venda(
            id SERIAL PRIMARY KEY, empresa_id INTEGER NOT NULL REFERENCES empresas(id),
            venda_id INTEGER NOT NULL REFERENCES vendas(id),
            produto_nome TEXT, quantidade INTEGER, preco_unit NUMERIC(12,2))""",
        # Log de atividades
        """CREATE TABLE IF NOT EXISTS log_atividades(
            id SERIAL PRIMARY KEY, empresa_id INTEGER NOT NULL REFERENCES empresas(id),
            usuario_id INTEGER REFERENCES usuarios(id),
            usuario_nome TEXT, tipo TEXT, descricao TEXT,
            criado_em TIMESTAMP DEFAULT NOW())""",
    ]
    migracoes = [
        ("vendas",    "comissao_sup",    "NUMERIC(12,2) DEFAULT 0"),
        ("vendas",    "comissao_rep",    "NUMERIC(12,2) DEFAULT 0"),
        ("vendas",    "usuario_id",      "INTEGER"),
        ("empresas",  "email_alerta",    "TEXT DEFAULT ''"),
        ("supervisores", "comissao_pct", "NUMERIC(5,2) DEFAULT 0"),
        ("representantes","comissao_pct","NUMERIC(5,2) DEFAULT 0"),
    ]
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                for s in stmts:
                    cur.execute(s)
                for tbl, col, tipo in migracoes:
                    cur.execute(
                        "SELECT column_name FROM information_schema.columns "
                        "WHERE table_name=%s AND column_name=%s", (tbl, col))
                    if not cur.fetchone():
                        cur.execute(f"ALTER TABLE {tbl} ADD COLUMN {col} {tipo}")
            conn.commit()
    except Exception as e:
        st.error(f"Erro ao inicializar banco: {e}")
        st.stop()

# ──────────────────────────────────────────────────────────────
#  HELPERS
# ──────────────────────────────────────────────────────────────
def eid() -> int:
    v = st.session_state.get("empresa_id")
    if not v: st.error("Sessão inválida."); st.stop()
    return v

def validate_doc(d): return len(re.sub(r'\D','',d)) in (11,14)
def validate_required(*f): return all(x is not None and str(x).strip() for x in f)
def hash_senha(s): return hashlib.sha256(s.encode()).hexdigest()

def get_estoque(pid):
    r = qry("SELECT estoque_atual FROM produtos WHERE id=%s AND empresa_id=%s",
            (pid, eid()), fetch=True)
    return r[0][0] if r else 0

def to_excel(rows, columns):
    if not HAS_PANDAS: return None
    df = pd.DataFrame(rows, columns=columns)
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        df.to_excel(w, index=False, sheet_name="Dados")
    return buf.getvalue()

def registrar_log(tipo: str, descricao: str):
    """Salva uma linha no log de atividades."""
    try:
        qry("INSERT INTO log_atividades(empresa_id,usuario_id,usuario_nome,tipo,descricao) "
            "VALUES(%s,%s,%s,%s,%s)",
            (eid(),
             st.session_state.get("usuario_id"),
             st.session_state.get("usuario_nome","sistema"),
             tipo, descricao))
    except Exception:
        pass  # log nunca deve quebrar o fluxo principal

def calcular_comissoes(total_venda: float, sup_id, rep_id) -> tuple:
    """Retorna (comissao_sup, comissao_rep) baseado nos percentuais cadastrados."""
    cs = cr = 0.0
    if sup_id:
        r = qry("SELECT comissao_pct FROM supervisores WHERE id=%s AND empresa_id=%s",
                (sup_id, eid()), fetch=True)
        if r: cs = float(r[0][0] or 0) * total_venda / 100
    if rep_id:
        r = qry("SELECT comissao_pct FROM representantes WHERE id=%s AND empresa_id=%s",
                (rep_id, eid()), fetch=True)
        if r: cr = float(r[0][0] or 0) * total_venda / 100
    return cs, cr

def enviar_alerta_estoque(empresa_id: int, produtos_baixos: list):
    """Tenta enviar e-mail de alerta de estoque via SMTP configurado em secrets."""
    try:
        cfg = st.secrets.get("smtp", {})
        if not cfg.get("host"): return
        email_dest = qry("SELECT email_alerta FROM empresas WHERE id=%s",
                         (empresa_id,), fetch=True)
        if not email_dest or not email_dest[0][0]: return
        lista = "\n".join(f"  - {p[0]}: {p[1]} un (mín: {p[2]})" for p in produtos_baixos)
        body = f"Alerta de estoque baixo:\n\n{lista}\n\nVerifique o sistema."
        msg = MIMEText(body, "plain", "utf-8")
        msg["Subject"] = "⚠️ ERP Pro — Estoque baixo"
        msg["From"] = cfg.get("from_email","erp@empresa.com")
        msg["To"] = email_dest[0][0]
        with smtplib.SMTP_SSL(cfg["host"], int(cfg.get("port", 465))) as s:
            s.login(cfg["user"], cfg["password"])
            s.send_message(msg)
    except Exception:
        pass  # e-mail é best-effort

def gerar_pdf_html(venda_id: int, empresa_nome: str, moeda: str) -> str:
    """Retorna HTML pronto para impressão de um pedido."""
    v = qry(
        "SELECT v.id,TO_CHAR(v.data,'DD/MM/YYYY HH24:MI'),v.cliente_name,"
        "v.valor_bruto,v.desconto_val,v.valor_total,v.pagamento,v.observacao,"
        "COALESCE(sup.nome,'—'),COALESCE(rep.nome,'—') "
        "FROM vendas v "
        "LEFT JOIN supervisores sup ON sup.id=v.supervisor_id "
        "LEFT JOIN representantes rep ON rep.id=v.representante_id "
        "WHERE v.id=%s AND v.empresa_id=%s",
        (venda_id, eid()), fetch=True)
    if not v: return "<p>Pedido não encontrado.</p>"
    vid,dt,cli,bruto,desc_v,total,pag,obs,sup_n,rep_n = v[0]
    itens = qry("SELECT produto_nome,quantidade,preco_unit FROM itens_venda "
                "WHERE venda_id=%s AND empresa_id=%s",
                (venda_id, eid()), fetch=True)
    rows = ""
    for pnome,pqtd,punit in (itens or []):
        sub = float(pqtd)*float(punit)
        rows += (f"<tr><td>{pnome}</td><td style='text-align:center'>{pqtd}</td>"
                 f"<td style='text-align:right'>{moeda} {float(punit):.2f}</td>"
                 f"<td style='text-align:right'>{moeda} {sub:.2f}</td></tr>")
    desc_row = (f"<tr><td colspan='3' style='text-align:right;color:#6b7280'>Desconto</td>"
                f"<td style='text-align:right;color:#ef4444'>- {moeda} {float(desc_v):.2f}</td></tr>"
                if desc_v and float(desc_v) > 0 else "")
    obs_block = f"<p style='margin-top:.75rem;font-size:.78rem;color:#6b7280'><b>Obs:</b> {obs}</p>" if obs else ""
    return f"""
    <div class="pdf-preview">
      <div class="pdf-header">
        <div><div class="pdf-title">{empresa_nome}</div>
             <div class="pdf-sub">Pedido #{vid} · {dt}</div></div>
        <div style="text-align:right">
          <div class="pdf-sub">Cliente: <b>{cli}</b></div>
          <div class="pdf-sub">Pagamento: {pag}</div>
          <div class="pdf-sub">Sup: {sup_n} · Rep: {rep_n}</div>
        </div>
      </div>
      <table class="pdf-table">
        <thead><tr><th>Produto</th><th>Qtd</th><th>Unit.</th><th>Total</th></tr></thead>
        <tbody>{rows}{desc_row}</tbody>
      </table>
      <div class="pdf-total">
        <div class="pdf-total-box">
          <div class="pdf-total-label">Total do Pedido</div>
          <div class="pdf-total-val">{moeda} {float(total):,.2f}</div>
        </div>
      </div>
      {obs_block}
      <p style="margin-top:1rem;font-size:.7rem;color:#9ca3af;text-align:center">
        Gerado por Gestão ERP Pro · {datetime.now().strftime('%d/%m/%Y %H:%M')}
      </p>
    </div>"""

# ──────────────────────────────────────────────────────────────
#  SESSION STATE
# ──────────────────────────────────────────────────────────────
_D = {
    "logado": False, "usuario_id": None, "empresa_id": None,
    "usuario_nome": "", "empresa_nome": "", "empresa_moeda": "R$",
    "usuario_perfil": "operador", "active_menu": "Dashboard", "cart": [],
    "editing_prod_id": None, "editing_prod_data": None, "adj_prod": None,
    "edit_cat_id": None, "edit_pag_id": None,
    "edit_cli_id": None, "edit_cli_data": None,
    "edit_sup_id": None, "edit_rep_id": None,
    "pdf_venda_id": None,
}
for k, v in _D.items():
    if k not in st.session_state:
        st.session_state[k] = v

# ──────────────────────────────────────────────────────────────
#  TELA DE LOGIN
# ──────────────────────────────────────────────────────────────
def tela_login():
    st.markdown(
        '<div class="login-wrap">'
        '<div class="login-logo">Gestão <span>ERP Pro</span></div>'
        '<div class="login-sub">Acesse sua conta para continuar</div>'
        '</div>', unsafe_allow_html=True)
    _, col, _ = st.columns([1, 2, 1])
    with col:
        with st.form("form_login"):
            email = st.text_input("E-mail", placeholder="seu@email.com")
            senha = st.text_input("Senha", type="password", placeholder="••••••••")
            entrar = st.form_submit_button("Entrar →", use_container_width=True)
        if entrar:
            if not validate_required(email, senha):
                show_error("Preencha e-mail e senha."); return
            row = run_query(
                "SELECT u.id,u.nome,u.perfil,u.ativo,e.id,e.nome,e.ativo,"
                "COALESCE(e.moeda,'R$') "
                "FROM usuarios u JOIN empresas e ON e.id=u.empresa_id "
                "WHERE u.email=%s AND u.senha_hash=%s",
                (email.strip().lower(), hash_senha(senha)), fetch=True)
            if not row:
                show_error("E-mail ou senha incorretos.", "Tente novamente."); return
            uid,unome,uperfil,uativo,empid,empnome,empativo,moeda = row[0]
            if not uativo: show_error("Usuário inativo."); return
            if not empativo: show_error("Empresa inativa."); return
            st.session_state.update({
                "logado": True, "usuario_id": uid, "empresa_id": empid,
                "usuario_nome": unome, "empresa_nome": empnome,
                "empresa_moeda": moeda, "usuario_perfil": uperfil,
            })
            st.rerun()
        st.markdown(
            "<p style='text-align:center;font-size:.76rem;color:#9ca3af;margin-top:.8rem'>"
            "Esqueceu a senha? Contate o administrador.</p>",
            unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────
#  BOOT
# ──────────────────────────────────────────────────────────────
init_db()
if not st.session_state.logado:
    tela_login()
    st.stop()

EMPRESA_ID = eid()
cur_sym = st.session_state.empresa_moeda

# ──────────────────────────────────────────────────────────────
#  SIDEBAR — Navegação
# ──────────────────────────────────────────────────────────────
MENUS = [
    ("📊", "Dashboard",          "dash"),
    ("🛒", "Pedidos",            "pedidos"),
    ("📦", "Estoque",            "estoque"),
    ("👥", "Clientes",           "clientes"),
    ("📜", "Histórico",          "hist"),
    ("💰", "Comissões",          "comissoes"),
    ("📋", "Log de Atividades",  "log"),
    ("👔", "Supervisores",       "sups"),
    ("🤝", "Representantes",     "reps"),
    ("🏷️", "Categorias",        "cats"),
    ("💳", "Formas de Pagamento","pags"),
    ("⚙️", "Configurações",     "cfg"),
]

with st.sidebar:
    st.markdown(
        f'<div class="erp-brand">Gestão <span>ERP Pro</span></div>'
        f'<div class="erp-tenant">🏢 {st.session_state.empresa_nome}</div>',
        unsafe_allow_html=True)

    st.markdown('<div class="nav-section-title">Principal</div>', unsafe_allow_html=True)
    for icon, label, key in MENUS:
        if key == "comissoes":
            st.markdown('<div class="nav-section-title">Relatórios</div>', unsafe_allow_html=True)
        if key == "sups":
            st.markdown('<div class="nav-section-title">Cadastros</div>', unsafe_allow_html=True)
        if key == "cats":
            st.markdown('<div class="nav-section-title">Tabelas auxiliares</div>', unsafe_allow_html=True)
        is_active = st.session_state.active_menu == label
        with st.container():
            if is_active: st.markdown('<div class="nav-active">', unsafe_allow_html=True)
            if st.button(f"{icon}  {label}", key=f"nav_{key}"):
                st.session_state.active_menu = label
                for k2 in ["editing_prod_id","editing_prod_data","adj_prod",
                            "edit_cat_id","edit_pag_id","edit_cli_id","edit_cli_data",
                            "edit_sup_id","edit_rep_id","pdf_venda_id"]:
                    st.session_state[k2] = None
                st.rerun()
            if is_active: st.markdown('</div>', unsafe_allow_html=True)

    n_cart = len(st.session_state.cart)
    if n_cart:
        st.markdown(
            f'<div style="margin:8px 10px 0;padding:6px 10px;border-radius:8px;'
            f'background:rgba(124,58,237,.2);color:#ddd6fe!important;font-size:.78rem;">'
            f'🛒 {n_cart} item(s) no carrinho</div>', unsafe_allow_html=True)
    st.divider()
    st.markdown(
        f'<div style="font-size:.73rem;color:rgba(255,255,255,.38)!important;padding:0 4px .25rem">'
        f'👤 {st.session_state.usuario_nome}<br>'
        f'<span style="font-size:.66rem">{st.session_state.usuario_perfil}</span></div>',
        unsafe_allow_html=True)
    with st.container():
        st.markdown('<div class="logout-btn">', unsafe_allow_html=True)
        if st.button("🚪  Sair", key="btn_logout"):
            for k in list(st.session_state.keys()):
                del st.session_state[k]
            st.rerun()
        st.markdown('</div>', unsafe_allow_html=True)
    st.markdown(
        f"<small style='color:rgba(255,255,255,0.22)'>v8.0 · "
        f"{datetime.now().strftime('%d/%m/%Y')}</small>",
        unsafe_allow_html=True)

menu = st.session_state.active_menu

# ══════════════════════════════════════════════════════════════
#  1. DASHBOARD  — Gráficos + alertas
# ══════════════════════════════════════════════════════════════
if menu == "Dashboard":
    page_header("📊", "Dashboard", f"Bem-vindo, {st.session_state.usuario_nome}!")

    # KPIs
    va  = qry("SELECT valor_total FROM vendas WHERE empresa_id=%s AND status='Pago'",
              (EMPRESA_ID,), fetch=True)
    tf  = sum(float(v[0]) for v in va) if va else 0.0
    qp  = len(va)
    at  = (tf/qp) if qp else 0.0

    # Hoje
    hoje_r = qry("SELECT COALESCE(SUM(valor_total),0) FROM vendas "
                 "WHERE empresa_id=%s AND status='Pago' AND data::date=CURRENT_DATE",
                 (EMPRESA_ID,), fetch=True)
    hoje = float(hoje_r[0][0]) if hoje_r else 0.0

    # Este mês vs mês anterior
    mes_r = qry("SELECT COALESCE(SUM(valor_total),0) FROM vendas "
                "WHERE empresa_id=%s AND status='Pago' "
                "AND date_trunc('month',data)=date_trunc('month',NOW())",
                (EMPRESA_ID,), fetch=True)
    mes_ant_r = qry("SELECT COALESCE(SUM(valor_total),0) FROM vendas "
                    "WHERE empresa_id=%s AND status='Pago' "
                    "AND date_trunc('month',data)=date_trunc('month',NOW()-INTERVAL '1 month')",
                    (EMPRESA_ID,), fetch=True)
    mes    = float(mes_r[0][0])    if mes_r    else 0.0
    mes_ant= float(mes_ant_r[0][0]) if mes_ant_r else 0.0
    delta  = ((mes-mes_ant)/mes_ant*100) if mes_ant else 0.0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Faturamento Total",  f"{cur_sym} {tf:,.2f}")
    c2.metric("Pedidos",             qp)
    c3.metric("Ticket Médio",        f"{cur_sym} {at:,.2f}")
    c4.metric("Este mês",            f"{cur_sym} {mes:,.2f}",
              delta=f"{delta:+.1f}% vs mês ant." if mes_ant else None)
    hr()

    # Gráfico de vendas dos últimos 30 dias
    if HAS_PLOTLY:
        dados_g = qry(
            "SELECT data::date AS dia, SUM(valor_total) FROM vendas "
            "WHERE empresa_id=%s AND status='Pago' "
            "AND data >= NOW()-INTERVAL '30 days' "
            "GROUP BY dia ORDER BY dia",
            (EMPRESA_ID,), fetch=True)
        if dados_g:
            if HAS_PANDAS:
                df_g = pd.DataFrame(dados_g, columns=["dia","total"])
                df_g["total"] = df_g["total"].astype(float)
                fig = px.area(df_g, x="dia", y="total",
                              title="Faturamento — últimos 30 dias",
                              labels={"dia":"","total":f"Valor ({cur_sym})"},
                              color_discrete_sequence=["#6366f1"])
                fig.update_layout(
                    plot_bgcolor="rgba(0,0,0,0)",
                    paper_bgcolor="rgba(0,0,0,0)",
                    font_family="DM Sans",
                    title_font_size=14,
                    showlegend=False,
                    margin=dict(l=0,r=0,t=36,b=0),
                )
                fig.update_traces(fillcolor="rgba(99,102,241,0.12)")
                st.plotly_chart(fig, use_container_width=True)
        hr()

    # Últimas vendas
    st.markdown("**Últimas vendas**")
    ul = qry(
        "SELECT v.id,v.cliente_name,v.valor_total,"
        "STRING_AGG(i.produto_nome||' x'||i.quantidade,', '),"
        "TO_CHAR(v.data,'DD/MM/YYYY HH24:MI') "
        "FROM vendas v "
        "LEFT JOIN itens_venda i ON i.venda_id=v.id AND i.empresa_id=v.empresa_id "
        "WHERE v.empresa_id=%s AND v.status='Pago' "
        "GROUP BY v.id ORDER BY v.data DESC LIMIT 8",
        (EMPRESA_ID,), fetch=True)
    if ul:
        for vid, cli, val, ps, df2 in ul:
            st.markdown(
                f'<div class="card"><div style="display:flex;justify-content:space-between;'
                f'flex-wrap:wrap;gap:4px"><div><div class="card-sku">Pedido #{vid} · {df2}</div>'
                f'<div class="card-title">{cli}</div>'
                f'<div class="card-sub">{ps or "—"}</div></div>'
                f'<div class="card-val">{cur_sym} {float(val):,.2f}</div>'
                f'</div></div>', unsafe_allow_html=True)
    else:
        show_info("Nenhuma venda registrada ainda.", "Cadastre clientes e produtos para começar.")

    # Alertas de estoque baixo
    low = qry(
        "SELECT nome,estoque_atual,estoque_minimo FROM produtos "
        "WHERE empresa_id=%s AND ativo=TRUE AND estoque_atual<=estoque_minimo",
        (EMPRESA_ID,), fetch=True)
    if low:
        hr()
        show_warning(f"{len(low)} produto(s) com estoque baixo ou zerado",
                     "Verifique a aba Estoque e faça a reposição.")
        # Tenta enviar alerta por e-mail (best-effort, não bloqueia)
        if "alerta_enviado" not in st.session_state:
            enviar_alerta_estoque(EMPRESA_ID, low)
            st.session_state["alerta_enviado"] = True

# ══════════════════════════════════════════════════════════════
#  2. PEDIDOS — Mobile-first + PDF
# ══════════════════════════════════════════════════════════════
elif menu == "Pedidos":
    page_header("🛒", "Pedidos", "Monte o pedido rapidamente")

    clis  = qry("SELECT nome FROM clientes WHERE empresa_id=%s AND ativo=TRUE ORDER BY nome", (EMPRESA_ID,), fetch=True)
    prods = qry("SELECT id,nome,preco_venda,estoque_atual FROM produtos WHERE empresa_id=%s AND ativo=TRUE AND estoque_atual>0 ORDER BY nome", (EMPRESA_ID,), fetch=True)
    pags  = qry("SELECT nome FROM pagamentos WHERE empresa_id=%s AND ativo=TRUE ORDER BY nome", (EMPRESA_ID,), fetch=True)
    sups  = qry("SELECT id,nome FROM supervisores WHERE empresa_id=%s AND ativo=TRUE ORDER BY nome", (EMPRESA_ID,), fetch=True)
    reps  = qry("SELECT id,nome FROM representantes WHERE empresa_id=%s AND ativo=TRUE ORDER BY nome", (EMPRESA_ID,), fetch=True)

    if not clis:  show_error("Nenhum cliente ativo.", "Cadastre um cliente."); st.stop()
    if not prods: show_error("Nenhum produto em estoque.", "Cadastre produtos."); st.stop()
    if not pags:  show_error("Nenhuma forma de pagamento.", "Cadastre uma."); st.stop()

    cart = st.session_state.cart

    # Se há pedido recém-criado → mostrar PDF
    if st.session_state.pdf_venda_id:
        vid_pdf = st.session_state.pdf_venda_id
        page_header("🧾", f"Pedido #{vid_pdf}", "Visualizar e imprimir")
        html_pdf = gerar_pdf_html(vid_pdf, st.session_state.empresa_nome, cur_sym)
        st.markdown(html_pdf, unsafe_allow_html=True)
        col_imp, col_vol = st.columns(2)
        if col_imp.button("🖨️  Imprimir / Salvar PDF", use_container_width=True):
            st.markdown("<script>window.print()</script>", unsafe_allow_html=True)
        if col_vol.button("➕  Novo pedido", use_container_width=True):
            st.session_state.pdf_venda_id = None; st.rerun()
        st.stop()

    # Layout: mobile empilha, desktop lado a lado
    col_form, col_cart = st.columns([3, 2])

    # ── Carrinho ──
    with col_cart:
        st.markdown("#### 🧺 Carrinho")
        if not cart:
            show_info("Carrinho vazio.", "Adicione produtos ao lado.")
        else:
            subtotal = sum(x["preco"] * x["qtd"] for x in cart)
            for i, item in enumerate(cart):
                sub = item["preco"] * item["qtd"]
                ci, cr = st.columns([5, 1])
                with ci:
                    st.markdown(
                        f'<div class="cart-item"><div>'
                        f'<div class="ci-name">{item["nome"]}</div>'
                        f'<div class="ci-qty">{item["qtd"]} un × {cur_sym} {item["preco"]:.2f}</div>'
                        f'</div><div class="ci-val">{cur_sym} {sub:.2f}</div></div>',
                        unsafe_allow_html=True)
                with cr:
                    if st.button("🗑️", key=f"rem_{i}", help="Remover"):
                        st.session_state.cart.pop(i); st.rerun()

            # Desconto
            st.markdown("**Desconto**")
            dc1, dc2 = st.columns(2)
            desc_tipo = dc1.selectbox("Tipo", ["Sem desconto","% Percentual","R$ Valor fixo"],
                                      key="desc_tipo", label_visibility="collapsed")
            desc_val  = dc2.number_input("Valor", min_value=0.0, step=0.01, format="%.2f",
                                         key="desc_val", label_visibility="collapsed")
            d_pct = d_reais = 0.0
            if desc_tipo == "% Percentual":
                d_pct   = min(desc_val, 100.0); d_reais = subtotal * d_pct / 100
            elif desc_tipo == "R$ Valor fixo":
                d_reais = min(desc_val, subtotal); d_pct = (d_reais/subtotal*100) if subtotal else 0
            total_final = subtotal - d_reais
            if d_reais > 0:
                st.markdown(f'<div class="cart-desc">Desconto: {cur_sym} {d_reais:.2f} ({d_pct:.1f}%)</div>',
                            unsafe_allow_html=True)
            st.markdown(
                f'<div class="cart-total"><span>Total do pedido</span>'
                f'<strong>{cur_sym} {total_final:,.2f}</strong></div>',
                unsafe_allow_html=True)

    # ── Formulário: adicionar produto ──
    with col_form:
        pn = [p[1] for p in prods]
        with st.form("form_add_item", clear_on_submit=True):
            st.markdown("#### Adicionar produto")
            pi  = st.selectbox("Produto *", range(len(pn)), format_func=lambda i: pn[i])
            ps  = prods[pi]; ed = int(ps[3])
            nc  = sum(x["qtd"] for x in cart if x["id"] == ps[0])
            dp  = max(0, ed - nc)
            st.caption(f"Disponível: **{ed}** · No carrinho: **{nc}** · Pode adicionar: **{dp}**")
            qa  = st.number_input("Quantidade *", min_value=1,
                                  max_value=dp if dp > 0 else 1,
                                  step=1, value=1, disabled=(dp == 0))
            ab  = st.form_submit_button("➕  Adicionar ao carrinho",
                                        use_container_width=True, disabled=(dp == 0))
        if ab:
            if dp == 0:
                show_error(f"Estoque esgotado para '{ps[1]}'.")
            else:
                ex = next((x for x in cart if x["id"] == ps[0]), None)
                if ex: ex["qtd"] += qa
                else:  st.session_state.cart.append({"id":ps[0],"nome":ps[1],"preco":float(ps[2]),"qtd":qa})
                st.rerun()

        hr()
        if cart:
            with st.form("form_finalizar"):
                st.markdown("#### Finalizar pedido")
                cf1, cf2 = st.columns(2)
                cliente_sel = cf1.selectbox("Cliente *", [c[0] for c in clis])
                forma       = cf2.selectbox("Pagamento *", [p[0] for p in pags])
                sf1, sf2    = st.columns(2)
                sup_opts    = ["(nenhum)"] + [s[1] for s in sups]
                rep_opts    = ["(nenhum)"] + [r[1] for r in reps]
                sup_sel     = sf1.selectbox("Supervisor", sup_opts)
                rep_sel     = sf2.selectbox("Representante", rep_opts)
                obs         = st.text_area("Observação (opcional)",
                                           placeholder="Ex: entregar na portaria…", height=55)
                fin         = st.form_submit_button("✅  Finalizar Venda", use_container_width=True)

            if fin:
                erros = []
                for item in cart:
                    er = get_estoque(item["id"])
                    if er < item["qtd"]:
                        erros.append(f"Estoque insuficiente para '{item['nome']}': {er} un.")
                if erros:
                    for e in erros: show_error(e)
                else:
                    sb_v = sum(x["preco"] * x["qtd"] for x in cart)
                    dt   = st.session_state.get("desc_tipo","Sem desconto")
                    dv   = st.session_state.get("desc_val", 0.0)
                    if   dt == "% Percentual":   dp2 = min(dv,100); dr = sb_v*dp2/100
                    elif dt == "R$ Valor fixo":  dr  = min(dv,sb_v); dp2 = (dr/sb_v*100) if sb_v else 0
                    else:                         dp2 = dr = 0.0
                    tv = sb_v - dr
                    sup_id = next((s[0] for s in sups if s[1] == sup_sel), None)
                    rep_id = next((r[0] for r in reps if r[1] == rep_sel), None)
                    cs, cr2 = calcular_comissoes(tv, sup_id, rep_id)

                    row = qry(
                        "INSERT INTO vendas(empresa_id,data,cliente_name,valor_bruto,"
                        "desconto_pct,desconto_val,valor_total,comissao_sup,comissao_rep,"
                        "pagamento,status,observacao,supervisor_id,representante_id,usuario_id) "
                        "VALUES(%s,NOW(),%s,%s,%s,%s,%s,%s,%s,%s,'Pago',%s,%s,%s,%s) RETURNING id",
                        (EMPRESA_ID,cliente_sel,sb_v,dp2,dr,tv,cs,cr2,
                         forma,obs.strip(),sup_id,rep_id,st.session_state.usuario_id),
                        returning=True)
                    if row:
                        vid = row[0]
                        for item in cart:
                            qry("INSERT INTO itens_venda(empresa_id,venda_id,produto_nome,quantidade,preco_unit) "
                                "VALUES(%s,%s,%s,%s,%s)",
                                (EMPRESA_ID,vid,item["nome"],item["qtd"],item["preco"]))
                            qry("UPDATE produtos SET estoque_atual=estoque_atual-%s "
                                "WHERE id=%s AND empresa_id=%s",
                                (item["qtd"],item["id"],EMPRESA_ID))
                        registrar_log("venda",
                            f"Venda #{vid} | {cliente_sel} | {cur_sym} {tv:.2f} | {len(cart)} produto(s)")
                        show_success(f"Venda #{vid} finalizada!",
                                     f"{cur_sym} {tv:.2f} · {cliente_sel} · {forma}")
                        st.session_state.cart = []
                        st.session_state.pdf_venda_id = vid
                        st.balloons(); st.rerun()
                    else:
                        show_error("Não foi possível salvar a venda.", "Tente novamente.")
        else:
            show_info("Carrinho vazio.", "Adicione produtos acima para liberar a finalização.")

# ══════════════════════════════════════════════════════════════
#  3. ESTOQUE
# ══════════════════════════════════════════════════════════════
elif menu == "Estoque":
    page_header("📦", "Estoque", "Gerencie seu catálogo de produtos")
    cats_raw = qry("SELECT nome FROM categorias WHERE empresa_id=%s AND ativo=TRUE ORDER BY nome",
                   (EMPRESA_ID,), fetch=True)
    cat_opts = [c[0] for c in cats_raw] if cats_raw else []

    with st.expander("➕  Adicionar novo produto"):
        if not cat_opts:
            show_warning("Nenhuma categoria ativa.", "Acesse 'Categorias' e crie uma antes.")
        else:
            with st.form("f_prod"):
                c1,c2,c3 = st.columns([1,2,1])
                sku  = c1.text_input("SKU *")
                nome = c2.text_input("Nome *")
                cat  = c3.selectbox("Categoria *", cat_opts)
                c4,c5,c6,c7 = st.columns(4)
                pc   = c4.number_input(f"Custo ({cur_sym})",   min_value=0.0, step=0.01, format="%.2f")
                pv   = c5.number_input(f"Venda ({cur_sym}) *", min_value=0.0, step=0.01, format="%.2f")
                est  = c6.number_input("Estoque Inicial", min_value=0, step=1)
                emin = c7.number_input("Estoque Mín.",    min_value=0, step=1, value=2)
                sv   = st.form_submit_button("💾  Salvar Produto", use_container_width=True)
            if sv:
                if not validate_required(sku, nome): show_error("SKU e Nome são obrigatórios.")
                elif pv == 0: show_error("Preço de venda não pode ser zero.")
                else:
                    res = qry("INSERT INTO produtos(empresa_id,sku,nome,categoria,preco_custo,"
                              "preco_venda,estoque_atual,estoque_minimo) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)",
                              (EMPRESA_ID,sku.strip(),nome.strip(),cat,pc,pv,est,emin))
                    if res is True:
                        registrar_log("estoque", f"Produto '{nome}' (SKU:{sku}) cadastrado")
                        show_success("Produto cadastrado!", f"'{nome}' adicionado."); st.rerun()
                    elif res == "duplicate": show_error(f"SKU '{sku}' já existe nesta empresa.")
                    else: show_error("Não foi possível salvar.")

    hr()
    with st.expander("🔍  Filtros", expanded=True):
        fb, ff = st.columns([3,1])
        busca   = fb.text_input("Buscar produto", placeholder="Nome ou SKU…",
                                key="busca_prod", label_visibility="collapsed")
        mostrar = ff.selectbox("Status", ["Ativos","Inativos","Todos"],
                               key="filtro_prod", label_visibility="collapsed")

    pr = qry("SELECT id,sku,nome,categoria,preco_custo,preco_venda,estoque_atual,estoque_minimo,ativo "
             "FROM produtos WHERE empresa_id=%s ORDER BY nome", (EMPRESA_ID,), fetch=True)
    if not pr:
        show_info("Nenhum produto cadastrado ainda.", "Use o formulário acima.")
    else:
        fl = list(pr)
        if mostrar == "Ativos":   fl = [p for p in fl if p[8]]
        if mostrar == "Inativos": fl = [p for p in fl if not p[8]]
        if busca:
            b = busca.lower(); fl = [p for p in fl if b in p[2].lower() or b in p[1].lower()]
        st.markdown(f"**{len(fl)} produto(s)**")
        for prod in fl:
            pid,sv2,nm,cat_v,pc_v,pv_v,est_v,emin_v,ativo_v = prod
            be = ('<span class="badge zero">Zerado</span>' if est_v==0 else
                  '<span class="badge low">Baixo</span>'  if est_v<=emin_v else
                  '<span class="badge ok">OK</span>')
            ic = "" if ativo_v else " inativo"
            cc2,ce,ca,ct = st.columns([5,1,1,1])
            with cc2:
                st.markdown(
                    f'<div class="card{ic}">'
                    f'<div class="card-sku">SKU: {sv2}</div>'
                    f'<div class="card-title">{nm}</div>'
                    f'<div class="card-sub">{cat_v} · Est: {est_v} {be} · '
                    f'<span class="badge {"ativo" if ativo_v else "inativo"}">{"Ativo" if ativo_v else "Inativo"}</span></div>'
                    f'<div class="card-val">{cur_sym} {float(pv_v):.2f}</div></div>',
                    unsafe_allow_html=True)
            with ce:
                if st.button("✏️", key=f"edit_{pid}", help="Editar"):
                    st.session_state.editing_prod_id   = pid
                    st.session_state.editing_prod_data = prod
                    st.session_state.adj_prod = None; st.rerun()
            with ca:
                if st.button("📦", key=f"adj_{pid}", help="Ajustar estoque"):
                    st.session_state.adj_prod = prod
                    st.session_state.editing_prod_id = None; st.rerun()
            with ct:
                if st.button("🔴" if ativo_v else "🟢", key=f"tog_{pid}",
                             help="Inativar" if ativo_v else "Ativar"):
                    qry("UPDATE produtos SET ativo=%s WHERE id=%s AND empresa_id=%s",
                        (not ativo_v, pid, EMPRESA_ID))
                    registrar_log("estoque", f"Produto '{nm}' {'inativado' if ativo_v else 'ativado'}")
                    st.rerun()

        if st.session_state.editing_prod_id:
            pid_e = st.session_state.editing_prod_id
            pd_   = st.session_state.editing_prod_data
            _,sku_e,nm_e,cat_e,pc_e,pv_e,est_e,emin_e,_ = pd_
            hr(); st.markdown(f"#### ✏️ Editando: {nm_e}")
            with st.form("f_edit"):
                ce1,ce2,ce3 = st.columns([1,2,1])
                ns = ce1.text_input("SKU *", value=sku_e)
                nn = ce2.text_input("Nome *", value=nm_e)
                ci = cat_opts.index(cat_e) if cat_e in cat_opts else 0
                nc2 = ce3.selectbox("Categoria *", cat_opts or [cat_e], index=ci)
                ce4,ce5,ce6,ce7 = st.columns(4)
                np2  = ce4.number_input(f"Custo ({cur_sym})",   value=float(pc_e), min_value=0.0, step=0.01, format="%.2f")
                npv  = ce5.number_input(f"Venda ({cur_sym}) *", value=float(pv_e), min_value=0.0, step=0.01, format="%.2f")
                ne   = ce6.number_input("Estoque",   value=int(est_e),  min_value=0, step=1)
                nem  = ce7.number_input("Est. Mín.", value=int(emin_e), min_value=0, step=1)
                cs,cc3 = st.columns(2)
                se   = cs.form_submit_button("💾  Salvar",   use_container_width=True)
                ce_b = cc3.form_submit_button("✕  Cancelar", use_container_width=True)
            if ce_b: st.session_state.editing_prod_id = None; st.rerun()
            if se:
                if not validate_required(ns, nn): show_error("SKU e Nome são obrigatórios.")
                elif npv == 0: show_error("Preço de venda não pode ser zero.")
                else:
                    res = qry("UPDATE produtos SET sku=%s,nome=%s,categoria=%s,preco_custo=%s,"
                              "preco_venda=%s,estoque_atual=%s,estoque_minimo=%s "
                              "WHERE id=%s AND empresa_id=%s",
                              (ns.strip(),nn.strip(),nc2,np2,npv,ne,nem,pid_e,EMPRESA_ID))
                    if res is True:
                        registrar_log("estoque", f"Produto '{nn}' editado")
                        show_success("Produto atualizado!")
                        st.session_state.editing_prod_id = None; st.rerun()
                    elif res == "duplicate": show_error("Já existe um produto com esse SKU.")
                    else: show_error("Não foi possível salvar.")

        if st.session_state.adj_prod:
            adj = st.session_state.adj_prod
            adj_id,_,adj_nm = adj[0],adj[1],adj[2]; adj_est = int(adj[6])
            hr(); st.markdown(f"#### 📦 Ajustar estoque: {adj_nm}  (atual: **{adj_est}**)")
            with st.form("f_adj"):
                co,cq = st.columns(2)
                op    = co.selectbox("Operação", ["Adicionar","Remover","Definir exato"])
                qa2   = cq.number_input("Quantidade", min_value=1, step=1)
                motivo = st.text_input("Motivo (opcional)", placeholder="Ex: compra, perda, inventário…")
                ca2,cb2 = st.columns(2)
                ap = ca2.form_submit_button("✅  Aplicar",  use_container_width=True)
                cn = cb2.form_submit_button("✕  Cancelar", use_container_width=True)
            if cn: st.session_state.adj_prod = None; st.rerun()
            if ap:
                if op == "Adicionar":
                    sq = "UPDATE produtos SET estoque_atual=estoque_atual+%s WHERE id=%s AND empresa_id=%s"
                    nv = adj_est + qa2
                elif op == "Remover":
                    if qa2 > adj_est:
                        show_error("Quantidade maior que o estoque atual."); st.stop()
                    sq = "UPDATE produtos SET estoque_atual=estoque_atual-%s WHERE id=%s AND empresa_id=%s"
                    nv = adj_est - qa2
                else:
                    sq = "UPDATE produtos SET estoque_atual=%s WHERE id=%s AND empresa_id=%s"
                    nv = qa2
                par = (qa2 if op != "Definir exato" else nv, adj_id, EMPRESA_ID)
                if qry(sq, par) is True:
                    registrar_log("estoque",
                        f"Estoque de '{adj_nm}': {op.lower()} {qa2} un → {nv} un"
                        + (f" | {motivo}" if motivo else ""))
                    show_success("Estoque ajustado!", f"'{adj_nm}' agora tem {nv} unidades.")
                    st.session_state.adj_prod = None; st.rerun()
                else:
                    show_error("Não foi possível ajustar o estoque.")

# ══════════════════════════════════════════════════════════════
#  4. CLIENTES
# ══════════════════════════════════════════════════════════════
elif menu == "Clientes":
    page_header("👥", "Clientes", "Gerencie sua base de clientes")
    with st.expander("➕  Novo cliente"):
        with st.form("f_cli"):
            c1,c2 = st.columns(2)
            nome = c1.text_input("Nome *"); doc = c2.text_input("CPF/CNPJ *", placeholder="Números")
            c3,c4 = st.columns(2); tel = c3.text_input("Telefone"); email = c4.text_input("E-mail")
            st.markdown("**Endereço**")
            ea,eb,ec = st.columns([3,1,2])
            rua = ea.text_input("Rua"); num = eb.text_input("Nº"); comp = ec.text_input("Compl.")
            ed,ee,ef,eg = st.columns([2,2,1,2])
            bairro = ed.text_input("Bairro"); cidade = ee.text_input("Cidade")
            estado = ef.text_input("UF", max_chars=2); cep = eg.text_input("CEP")
            sv = st.form_submit_button("💾  Salvar Cliente", use_container_width=True)
        if sv:
            if not validate_required(nome, doc): show_error("Nome e CPF/CNPJ são obrigatórios.")
            elif not validate_doc(doc):           show_error("CPF/CNPJ inválido. (11 ou 14 dígitos)")
            else:
                res = qry("INSERT INTO clientes(empresa_id,nome,documento,telefone,email,"
                          "rua,numero,complemento,bairro,cidade,estado,cep) "
                          "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                          (EMPRESA_ID,nome.strip(),re.sub(r'\D','',doc),
                           tel,email,rua,num,comp,bairro,cidade,
                           estado.upper() if estado else "",cep))
                if res is True:
                    registrar_log("cliente", f"Cliente '{nome}' cadastrado")
                    show_success("Cliente cadastrado!", f"'{nome}' adicionado."); st.rerun()
                elif res == "duplicate": show_error("Já existe um cliente com esse CPF/CNPJ.")
                else: show_error("Não foi possível salvar.")
    hr()
    with st.expander("🔍  Filtros", expanded=True):
        cb2,cf2 = st.columns([3,1])
        busca_cli  = cb2.text_input("Buscar cliente", placeholder="Nome, CPF/CNPJ ou cidade…",
                                    key="busca_cli", label_visibility="collapsed")
        filtro_cli = cf2.selectbox("Status", ["Ativos","Inativos","Todos"],
                                   key="filtro_cli", label_visibility="collapsed")
    cf_ = qry("SELECT id,nome,documento,telefone,email,rua,numero,complemento,"
              "bairro,cidade,estado,cep,ativo "
              "FROM clientes WHERE empresa_id=%s ORDER BY nome", (EMPRESA_ID,), fetch=True)
    if cf_:
        cv = list(cf_)
        if filtro_cli == "Ativos":   cv = [c for c in cv if c[12]]
        if filtro_cli == "Inativos": cv = [c for c in cv if not c[12]]
        if busca_cli:
            b = busca_cli.lower()
            cv = [c for c in cv if b in (c[1] or "").lower()
                  or b in (c[2] or "").lower() or b in (c[9] or "").lower()]
        st.markdown(f"**{len(cv)} cliente(s)**")
        for cli in cv:
            cid,cnome,cdoc,ctel,cemail,crua,cnum,ccomp,cbairro,ccidade,cestado,ccep,cativo = cli
            sb = ('<span class="badge ativo">Ativo</span>' if cativo
                  else '<span class="badge inativo">Inativo</span>')
            if st.session_state.edit_cli_id == cid:
                st.markdown(f"#### ✏️ Editando: {cnome}")
                with st.form(f"f_edit_cli_{cid}"):
                    ec1,ec2 = st.columns(2)
                    en  = ec1.text_input("Nome *",    value=cnome or "")
                    edo = ec2.text_input("CPF/CNPJ *", value=cdoc  or "")
                    ec3,ec4 = st.columns(2)
                    etl = ec3.text_input("Telefone", value=ctel   or "")
                    eml = ec4.text_input("E-mail",   value=cemail or "")
                    st.markdown("**Endereço**")
                    eea,eeb,eec = st.columns([3,1,2])
                    erua  = eea.text_input("Rua",   value=crua  or "")
                    enum  = eeb.text_input("Nº",    value=cnum  or "")
                    ecomp = eec.text_input("Compl.",value=ccomp or "")
                    eed,eee,eef,eeg = st.columns([2,2,1,2])
                    ebairro = eed.text_input("Bairro",  value=cbairro or "")
                    ecidade = eee.text_input("Cidade",  value=ccidade or "")
                    eestado = eef.text_input("UF",      value=cestado or "", max_chars=2)
                    ecep    = eeg.text_input("CEP",     value=ccep    or "")
                    cs_e,cc_e = st.columns(2)
                    sv_e = cs_e.form_submit_button("💾  Salvar",   use_container_width=True)
                    cn_e = cc_e.form_submit_button("✕  Cancelar", use_container_width=True)
                if cn_e: st.session_state.edit_cli_id = None; st.rerun()
                if sv_e:
                    if not validate_required(en, edo): show_error("Nome e CPF/CNPJ obrigatórios.")
                    elif not validate_doc(edo):         show_error("CPF/CNPJ inválido.")
                    else:
                        res = qry("UPDATE clientes SET nome=%s,documento=%s,telefone=%s,"
                                  "email=%s,rua=%s,numero=%s,complemento=%s,bairro=%s,"
                                  "cidade=%s,estado=%s,cep=%s WHERE id=%s AND empresa_id=%s",
                                  (en.strip(),re.sub(r'\D','',edo),etl,eml,erua,enum,ecomp,
                                   ebairro,ecidade,eestado.upper() if eestado else "",ecep,
                                   cid,EMPRESA_ID))
                        if res is True:
                            registrar_log("cliente", f"Cliente '{en}' editado")
                            show_success("Cliente atualizado!")
                            st.session_state.edit_cli_id = None; st.rerun()
                        elif res == "duplicate": show_error("Já existe um cliente com esse CPF/CNPJ.")
                        else: show_error("Não foi possível salvar.")
            else:
                ci2,ce2,ct2 = st.columns([8,1,1])
                ci2.markdown(
                    f'<div class="card"><div class="card-title">{cnome} &nbsp;{sb}</div>'
                    f'<div class="card-sub">{cdoc} · {ctel or "—"} · {ccidade or "—"}</div></div>',
                    unsafe_allow_html=True)
                if ce2.button("✏️", key=f"edit_cli_{cid}", help="Editar"):
                    st.session_state.edit_cli_id   = cid
                    st.session_state.edit_cli_data = cli; st.rerun()
                if ct2.button("🔴" if cativo else "🟢", key=f"tog_cli_{cid}",
                              help="Inativar" if cativo else "Ativar"):
                    qry("UPDATE clientes SET ativo=%s WHERE id=%s AND empresa_id=%s",
                        (not cativo, cid, EMPRESA_ID))
                    registrar_log("cliente", f"Cliente '{cnome}' {'inativado' if cativo else 'ativado'}")
                    st.rerun()
            st.markdown('<div style="border-top:.5px solid #f1f5f9;margin:4px 0"></div>',
                        unsafe_allow_html=True)
    else:
        show_info("Nenhum cliente encontrado.", "Ajuste os filtros ou adicione um novo.")

# ══════════════════════════════════════════════════════════════
#  5. HISTÓRICO DE VENDAS — filtro data + PDF + exportação
# ══════════════════════════════════════════════════════════════
elif menu == "Histórico":
    page_header("📜", "Histórico de Vendas", "Consulte, filtre e exporte transações")
    with st.expander("🔍  Filtros", expanded=True):
        hf1,hf2,hf3,hf4 = st.columns(4)
        fs  = hf1.selectbox("Status", ["Todos","Pago","Cancelado"], key="filtro_hist")
        di  = hf2.date_input("Data inicial", value=date.today()-timedelta(days=30), key="hist_ini")
        df2 = hf3.date_input("Data final",   value=date.today(), key="hist_fim")
        bch = hf4.text_input("Cliente", placeholder="Nome…", key="busca_hist_cli",
                             label_visibility="collapsed")

    wp = ["v.empresa_id=%s","v.data>=%s","v.data<%s"]
    ph = [EMPRESA_ID,
          datetime.combine(di,  datetime.min.time()),
          datetime.combine(df2+timedelta(days=1), datetime.min.time())]
    if fs  != "Todos": wp.append("v.status=%s");            ph.append(fs)
    if bch:            wp.append("v.cliente_name ILIKE %s"); ph.append(f"%{bch}%")
    ws = " AND ".join(wp)

    vendas = qry(
        f"SELECT v.id,TO_CHAR(v.data,'DD/MM/YYYY HH24:MI'),v.cliente_name,"
        f"v.valor_bruto,v.desconto_val,v.valor_total,v.pagamento,v.status,"
        f"STRING_AGG(i.produto_nome||' x'||i.quantidade,' | '),"
        f"COALESCE(v.observacao,''),"
        f"COALESCE(sup.nome,'—'),COALESCE(rep.nome,'—') "
        f"FROM vendas v "
        f"LEFT JOIN itens_venda i   ON i.venda_id=v.id AND i.empresa_id=v.empresa_id "
        f"LEFT JOIN supervisores sup ON sup.id=v.supervisor_id "
        f"LEFT JOIN representantes rep ON rep.id=v.representante_id "
        f"WHERE {ws} "
        f"GROUP BY v.id,sup.nome,rep.nome ORDER BY v.data DESC",
        tuple(ph), fetch=True)

    if vendas and HAS_PANDAS:
        xb = to_excel(vendas,
                      ["#","Data","Cliente","Bruto","Desconto","Total","Pagamento",
                       "Status","Produtos","Obs","Supervisor","Representante"])
        if xb:
            st.download_button("⬇️  Exportar Excel", data=xb,
                               file_name=f"vendas_{date.today()}.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    if not vendas:
        show_info("Nenhuma venda encontrada.", "Ajuste os filtros.")
    else:
        st.markdown(f"**{len(vendas)} venda(s)**")
        for row in vendas:
            vid,dfmt,cli,bruto,dv,val,pag,status,itens,obs,sup_n,rep_n = row
            itens = itens or "—"; obs = obs or ""; ip = status == "Pago"
            sb  = ('<span class="badge pago">Pago</span>' if ip
                   else '<span class="badge cancelado">Cancelado</span>')
            oh  = (f'<div class="card-sub" style="font-style:italic">📝 {obs}</div>'
                   if obs else "")
            dh  = (f'<div class="card-sub">Desconto: {cur_sym} {float(dv):.2f}</div>'
                   if dv and float(dv) > 0 else "")
            ci2,cv2,ca2 = st.columns([5,2,2])
            with ci2:
                st.markdown(
                    f'<div class="card">'
                    f'<div class="card-sku">#{vid} · {dfmt} · {pag} · 👔{sup_n} · 🤝{rep_n}</div>'
                    f'<div class="card-title">{cli} &nbsp;{sb}</div>'
                    f'<div class="card-sub">{itens}</div>{dh}{oh}</div>',
                    unsafe_allow_html=True)
            with cv2:
                st.markdown(
                    f'<div style="font-family:Sora,sans-serif;font-size:1rem;'
                    f'font-weight:700;color:#6366f1;padding-top:14px">'
                    f'{cur_sym} {float(val):,.2f}</div>',
                    unsafe_allow_html=True)
            with ca2:
                # Botão PDF
                if st.button("🧾 PDF", key=f"pdf_{vid}", help="Visualizar pedido"):
                    st.session_state.pdf_venda_id = vid
                    st.session_state.active_menu  = "Pedidos"
                    st.rerun()
                if ip:
                    if st.button("❌ Cancelar", key=f"cancel_{vid}"):
                        idb = qry("SELECT produto_nome,quantidade FROM itens_venda "
                                  "WHERE venda_id=%s AND empresa_id=%s",
                                  (vid, EMPRESA_ID), fetch=True)
                        if idb:
                            for pn,pq in idb:
                                qry("UPDATE produtos SET estoque_atual=estoque_atual+%s "
                                    "WHERE nome=%s AND empresa_id=%s", (pq,pn,EMPRESA_ID))
                        qry("UPDATE vendas SET status='Cancelado' WHERE id=%s AND empresa_id=%s",
                            (vid, EMPRESA_ID))
                        registrar_log("cancelamento", f"Venda #{vid} cancelada")
                        show_success(f"Pedido #{vid} cancelado.", "Estoque revertido."); st.rerun()
                else:
                    if st.button("✅ Reativar", key=f"reativar_{vid}"):
                        idb = qry("SELECT produto_nome,quantidade FROM itens_venda "
                                  "WHERE venda_id=%s AND empresa_id=%s",
                                  (vid, EMPRESA_ID), fetch=True)
                        errs = []
                        if idb:
                            for pn,pq in idb:
                                er = qry("SELECT estoque_atual FROM produtos "
                                         "WHERE nome=%s AND empresa_id=%s",
                                         (pn, EMPRESA_ID), fetch=True)
                                ev = er[0][0] if er else 0
                                if ev < pq: errs.append(f"'{pn}': necessário {pq}, disponível {ev}.")
                        if errs:
                            for e in errs: show_error(e)
                        else:
                            if idb:
                                for pn,pq in idb:
                                    qry("UPDATE produtos SET estoque_atual=estoque_atual-%s "
                                        "WHERE nome=%s AND empresa_id=%s", (pq,pn,EMPRESA_ID))
                            qry("UPDATE vendas SET status='Pago' WHERE id=%s AND empresa_id=%s",
                                (vid, EMPRESA_ID))
                            registrar_log("venda", f"Venda #{vid} reativada")
                            show_success(f"Pedido #{vid} reativado!"); st.rerun()
            st.markdown('<div style="border-top:.5px solid #f1f5f9;margin:6px 0"></div>',
                        unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════
#  6. COMISSÕES
# ══════════════════════════════════════════════════════════════
elif menu == "Comissões":
    page_header("💰", "Comissões", "Acompanhe comissões por supervisor e representante")

    with st.expander("🔍  Filtrar período", expanded=True):
        cf1,cf2 = st.columns(2)
        ci_d = cf1.date_input("De", value=date.today().replace(day=1), key="comm_ini")
        cf_d = cf2.date_input("Até", value=date.today(), key="comm_fim")

    params_c = (EMPRESA_ID,
                datetime.combine(ci_d, datetime.min.time()),
                datetime.combine(cf_d + timedelta(days=1), datetime.min.time()))

    st.markdown("#### 👔 Supervisores")
    sups_c = qry(
        "SELECT sup.nome,sup.comissao_pct,"
        "COUNT(v.id) AS qtd_vendas,"
        "COALESCE(SUM(v.valor_total),0) AS total_vendas,"
        "COALESCE(SUM(v.comissao_sup),0) AS total_comissao "
        "FROM supervisores sup "
        "LEFT JOIN vendas v ON v.supervisor_id=sup.id "
        "  AND v.empresa_id=%s AND v.status='Pago' "
        "  AND v.data>=%s AND v.data<%s "
        "WHERE sup.empresa_id=%s AND sup.ativo=TRUE "
        "GROUP BY sup.id,sup.nome,sup.comissao_pct ORDER BY total_comissao DESC",
        (EMPRESA_ID, params_c[1], params_c[2], EMPRESA_ID), fetch=True)

    if sups_c:
        for sn,spct,sqv,stv,scom in sups_c:
            st.markdown(
                f'<div class="comm-card">'
                f'<div style="display:flex;justify-content:space-between;align-items:flex-start">'
                f'<div><div class="comm-name">👔 {sn}</div>'
                f'<div class="comm-sub">{sqv} venda(s) · Total: {cur_sym} {float(stv):,.2f} · Tx: {float(spct):.1f}%</div></div>'
                f'<div class="comm-val">{cur_sym} {float(scom):,.2f}</div>'
                f'</div></div>',
                unsafe_allow_html=True)
    else:
        show_info("Nenhum supervisor com vendas neste período.")

    hr()
    st.markdown("#### 🤝 Representantes")
    reps_c = qry(
        "SELECT rep.nome,rep.comissao_pct,"
        "COUNT(v.id) AS qtd_vendas,"
        "COALESCE(SUM(v.valor_total),0) AS total_vendas,"
        "COALESCE(SUM(v.comissao_rep),0) AS total_comissao,"
        "COALESCE(sup.nome,'—') AS supervisor "
        "FROM representantes rep "
        "LEFT JOIN vendas v ON v.representante_id=rep.id "
        "  AND v.empresa_id=%s AND v.status='Pago' "
        "  AND v.data>=%s AND v.data<%s "
        "LEFT JOIN supervisores sup ON sup.id=rep.supervisor_id "
        "WHERE rep.empresa_id=%s AND rep.ativo=TRUE "
        "GROUP BY rep.id,rep.nome,rep.comissao_pct,sup.nome ORDER BY total_comissao DESC",
        (EMPRESA_ID, params_c[1], params_c[2], EMPRESA_ID), fetch=True)

    if reps_c:
        for rn,rpct,rqv,rtv,rcom,rsup in reps_c:
            st.markdown(
                f'<div class="comm-card">'
                f'<div style="display:flex;justify-content:space-between;align-items:flex-start">'
                f'<div><div class="comm-name">🤝 {rn}</div>'
                f'<div class="comm-sub">{rqv} venda(s) · Total: {cur_sym} {float(rtv):,.2f} · '
                f'Tx: {float(rpct):.1f}% · Sup: {rsup}</div></div>'
                f'<div class="comm-val">{cur_sym} {float(rcom):,.2f}</div>'
                f'</div></div>',
                unsafe_allow_html=True)
    else:
        show_info("Nenhum representante com vendas neste período.")

    # Exportar comissões
    if (sups_c or reps_c) and HAS_PANDAS:
        hr()
        all_rows = (
            [("Supervisor", s[0], float(s[1]), int(s[2]), float(s[3]), float(s[4])) for s in (sups_c or [])] +
            [("Representante", r[0], float(r[1]), int(r[2]), float(r[3]), float(r[4])) for r in (reps_c or [])]
        )
        xb = to_excel(all_rows, ["Tipo","Nome","Taxa %","Qtd Vendas","Total Vendas","Comissão"])
        if xb:
            st.download_button("⬇️  Exportar Comissões Excel", data=xb,
                               file_name=f"comissoes_{ci_d}_{cf_d}.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# ══════════════════════════════════════════════════════════════
#  7. LOG DE ATIVIDADES
# ══════════════════════════════════════════════════════════════
elif menu == "Log de Atividades":
    page_header("📋", "Log de Atividades", "Rastreie tudo que aconteceu no sistema")

    with st.expander("🔍  Filtros", expanded=True):
        lf1,lf2,lf3 = st.columns(3)
        l_tipo = lf1.selectbox("Tipo", ["Todos","venda","estoque","cliente","cancelamento","sistema"],
                               key="log_tipo")
        l_di   = lf2.date_input("De",  value=date.today()-timedelta(days=7), key="log_ini")
        l_df   = lf3.date_input("Até", value=date.today(), key="log_fim")

    lw = ["empresa_id=%s","criado_em>=%s","criado_em<%s"]
    lp = [EMPRESA_ID,
          datetime.combine(l_di, datetime.min.time()),
          datetime.combine(l_df + timedelta(days=1), datetime.min.time())]
    if l_tipo != "Todos": lw.append("tipo=%s"); lp.append(l_tipo)

    logs = qry(
        f"SELECT usuario_nome,tipo,descricao,TO_CHAR(criado_em,'DD/MM/YYYY HH24:MI') "
        f"FROM log_atividades WHERE {' AND '.join(lw)} "
        f"ORDER BY criado_em DESC LIMIT 200",
        tuple(lp), fetch=True)

    TIPO_COR = {
        "venda": "venda", "estoque": "estoque", "cliente": "cliente",
        "cancelamento": "cancelamento", "sistema": "sistema",
    }
    if logs:
        st.markdown(f"**{len(logs)} registro(s)**")
        for u,tipo,desc,dt in logs:
            cor = TIPO_COR.get(tipo, "sistema")
            st.markdown(
                f'<div class="log-item">'
                f'<div class="log-dot {cor}"></div>'
                f'<div><div class="log-text"><b>{u}</b> — {desc}</div>'
                f'<div class="log-time">{dt} · {tipo}</div></div>'
                f'</div>',
                unsafe_allow_html=True)
    else:
        show_info("Nenhuma atividade encontrada neste período.", "Ajuste os filtros.")

# ══════════════════════════════════════════════════════════════
#  8. SUPERVISORES
# ══════════════════════════════════════════════════════════════
elif menu == "Supervisores":
    page_header("👔", "Supervisores", "Gerencie a equipe de supervisores")
    with st.expander("➕  Novo supervisor"):
        with st.form("f_sup"):
            s1,s2 = st.columns(2)
            sn = s1.text_input("Nome *"); se = s2.text_input("E-mail")
            s3,s4 = st.columns(2)
            st2 = s3.text_input("Telefone"); sc = s4.number_input("Comissão (%)", min_value=0.0, max_value=100.0, step=0.1, format="%.1f")
            sv = st.form_submit_button("💾  Salvar", use_container_width=True)
        if sv:
            if not validate_required(sn): show_error("Nome é obrigatório.")
            else:
                res = qry("INSERT INTO supervisores(empresa_id,nome,email,telefone,comissao_pct) "
                          "VALUES(%s,%s,%s,%s,%s)", (EMPRESA_ID,sn.strip(),se,st2,sc))
                if res is True:
                    registrar_log("sistema", f"Supervisor '{sn}' cadastrado")
                    show_success("Supervisor cadastrado!", f"'{sn}' adicionado."); st.rerun()
                elif res == "duplicate": show_error("Já existe um supervisor com esse nome.")
                else: show_error("Não foi possível salvar.")
    hr()
    bs = st.text_input("🔍  Buscar supervisor", placeholder="Nome…", key="busca_sup")
    sups = qry("SELECT id,nome,email,telefone,comissao_pct,ativo FROM supervisores "
               "WHERE empresa_id=%s ORDER BY nome", (EMPRESA_ID,), fetch=True)
    if sups:
        if bs: b=bs.lower(); sups=[s for s in sups if b in (s[1] or "").lower()]
        st.markdown(f"**{len(sups)} supervisor(es)**")
        for sid,sn2,se2,st3,sc2,sa in sups:
            sb = ('<span class="badge ativo">Ativo</span>' if sa
                  else '<span class="badge inativo">Inativo</span>')
            if st.session_state.edit_sup_id == sid:
                with st.form(f"f_edit_sup_{sid}"):
                    es1,es2 = st.columns(2)
                    en  = es1.text_input("Nome *", value=sn2 or "")
                    em  = es2.text_input("E-mail",  value=se2 or "")
                    es3,es4 = st.columns(2)
                    et  = es3.text_input("Telefone", value=st3 or "")
                    ecc = es4.number_input("Comissão (%)", value=float(sc2 or 0),
                                           min_value=0.0, max_value=100.0, step=0.1, format="%.1f")
                    cs2,cc2 = st.columns(2)
                    sv2 = cs2.form_submit_button("💾  Salvar",   use_container_width=True)
                    cn2 = cc2.form_submit_button("✕  Cancelar", use_container_width=True)
                if cn2: st.session_state.edit_sup_id = None; st.rerun()
                if sv2:
                    if not validate_required(en): show_error("Nome é obrigatório.")
                    else:
                        res = qry("UPDATE supervisores SET nome=%s,email=%s,telefone=%s,comissao_pct=%s "
                                  "WHERE id=%s AND empresa_id=%s",
                                  (en.strip(),em,et,ecc,sid,EMPRESA_ID))
                        if res is True:
                            registrar_log("sistema", f"Supervisor '{en}' editado")
                            show_success("Supervisor atualizado!")
                            st.session_state.edit_sup_id = None; st.rerun()
                        elif res == "duplicate": show_error("Já existe um supervisor com esse nome.")
                        else: show_error("Não foi possível salvar.")
            else:
                ci2,ce2,ct2 = st.columns([8,1,1])
                ci2.markdown(
                    f'<div class="card"><div class="card-title">{sn2} &nbsp;{sb}</div>'
                    f'<div class="card-sub">{se2 or "—"} · {st3 or "—"} · '
                    f'Comissão: {float(sc2 or 0):.1f}%</div></div>',
                    unsafe_allow_html=True)
                if ce2.button("✏️", key=f"edit_sup_{sid}", help="Editar"):
                    st.session_state.edit_sup_id = sid; st.rerun()
                if ct2.button("🔴" if sa else "🟢", key=f"tog_sup_{sid}",
                              help="Inativar" if sa else "Ativar"):
                    qry("UPDATE supervisores SET ativo=%s WHERE id=%s AND empresa_id=%s",
                        (not sa, sid, EMPRESA_ID))
                    registrar_log("sistema", f"Supervisor '{sn2}' {'inativado' if sa else 'ativado'}")
                    st.rerun()
            st.markdown('<div style="border-top:.5px solid #f1f5f9;margin:4px 0"></div>',
                        unsafe_allow_html=True)
    else:
        show_info("Nenhum supervisor cadastrado.", "Adicione um acima.")

# ══════════════════════════════════════════════════════════════
#  9. REPRESENTANTES
# ══════════════════════════════════════════════════════════════
elif menu == "Representantes":
    page_header("🤝", "Representantes", "Gerencie a equipe de representantes")
    sups_opts = qry("SELECT id,nome FROM supervisores WHERE empresa_id=%s AND ativo=TRUE ORDER BY nome",
                    (EMPRESA_ID,), fetch=True)
    with st.expander("➕  Novo representante"):
        with st.form("f_rep"):
            r1,r2 = st.columns(2)
            rn = r1.text_input("Nome *"); re2 = r2.text_input("E-mail")
            r3,r4 = st.columns(2)
            rt = r3.text_input("Telefone"); rc = r4.number_input("Comissão (%)", min_value=0.0, max_value=100.0, step=0.1, format="%.1f")
            sl = ["(nenhum)"] + [s[1] for s in sups_opts]
            rs = st.selectbox("Supervisor responsável", sl)
            sv = st.form_submit_button("💾  Salvar", use_container_width=True)
        if sv:
            if not validate_required(rn): show_error("Nome é obrigatório.")
            else:
                si = next((s[0] for s in sups_opts if s[1]==rs), None)
                res = qry("INSERT INTO representantes(empresa_id,nome,email,telefone,supervisor_id,comissao_pct) "
                          "VALUES(%s,%s,%s,%s,%s,%s)",
                          (EMPRESA_ID,rn.strip(),re2,rt,si,rc))
                if res is True:
                    registrar_log("sistema", f"Representante '{rn}' cadastrado")
                    show_success("Representante cadastrado!", f"'{rn}' adicionado."); st.rerun()
                elif res == "duplicate": show_error("Já existe um representante com esse nome.")
                else: show_error("Não foi possível salvar.")
    hr()
    br = st.text_input("🔍  Buscar representante", placeholder="Nome…", key="busca_rep")
    reps = qry("SELECT r.id,r.nome,r.email,r.telefone,r.comissao_pct,r.ativo,"
               "COALESCE(s.nome,'—') FROM representantes r "
               "LEFT JOIN supervisores s ON s.id=r.supervisor_id "
               "WHERE r.empresa_id=%s ORDER BY r.nome",
               (EMPRESA_ID,), fetch=True)
    if reps:
        if br: b=br.lower(); reps=[r for r in reps if b in (r[1] or "").lower()]
        st.markdown(f"**{len(reps)} representante(s)**")
        for rid,rn2,re3,rt2,rc2,ra,rsn in reps:
            sb = ('<span class="badge ativo">Ativo</span>' if ra
                  else '<span class="badge inativo">Inativo</span>')
            if st.session_state.edit_rep_id == rid:
                with st.form(f"f_edit_rep_{rid}"):
                    er1,er2 = st.columns(2)
                    en  = er1.text_input("Nome *", value=rn2 or "")
                    em  = er2.text_input("E-mail",  value=re3 or "")
                    er3,er4 = st.columns(2)
                    et  = er3.text_input("Telefone", value=rt2 or "")
                    ecc = er4.number_input("Comissão (%)", value=float(rc2 or 0),
                                           min_value=0.0, max_value=100.0, step=0.1, format="%.1f")
                    sl2 = ["(nenhum)"] + [s[1] for s in sups_opts]
                    ci2 = sl2.index(rsn) if rsn in sl2 else 0
                    es  = st.selectbox("Supervisor", sl2, index=ci2)
                    cs2,cc2 = st.columns(2)
                    sv2 = cs2.form_submit_button("💾  Salvar",   use_container_width=True)
                    cn2 = cc2.form_submit_button("✕  Cancelar", use_container_width=True)
                if cn2: st.session_state.edit_rep_id = None; st.rerun()
                if sv2:
                    if not validate_required(en): show_error("Nome é obrigatório.")
                    else:
                        si2 = next((s[0] for s in sups_opts if s[1]==es), None)
                        res = qry("UPDATE representantes SET nome=%s,email=%s,telefone=%s,"
                                  "supervisor_id=%s,comissao_pct=%s "
                                  "WHERE id=%s AND empresa_id=%s",
                                  (en.strip(),em,et,si2,ecc,rid,EMPRESA_ID))
                        if res is True:
                            registrar_log("sistema", f"Representante '{en}' editado")
                            show_success("Representante atualizado!")
                            st.session_state.edit_rep_id = None; st.rerun()
                        elif res == "duplicate": show_error("Já existe um representante com esse nome.")
                        else: show_error("Não foi possível salvar.")
            else:
                ci2,ce2,ct2 = st.columns([8,1,1])
                ci2.markdown(
                    f'<div class="card"><div class="card-title">{rn2} &nbsp;{sb}</div>'
                    f'<div class="card-sub">{re3 or "—"} · {rt2 or "—"} · '
                    f'Comissão: {float(rc2 or 0):.1f}% · Sup: {rsn}</div></div>',
                    unsafe_allow_html=True)
                if ce2.button("✏️", key=f"edit_rep_{rid}", help="Editar"):
                    st.session_state.edit_rep_id = rid; st.rerun()
                if ct2.button("🔴" if ra else "🟢", key=f"tog_rep_{rid}",
                              help="Inativar" if ra else "Ativar"):
                    qry("UPDATE representantes SET ativo=%s WHERE id=%s AND empresa_id=%s",
                        (not ra, rid, EMPRESA_ID))
                    registrar_log("sistema", f"Representante '{rn2}' {'inativado' if ra else 'ativado'}")
                    st.rerun()
            st.markdown('<div style="border-top:.5px solid #f1f5f9;margin:4px 0"></div>',
                        unsafe_allow_html=True)
    else:
        show_info("Nenhum representante cadastrado.", "Adicione um acima.")

# ══════════════════════════════════════════════════════════════
#  10. CATEGORIAS
# ══════════════════════════════════════════════════════════════
elif menu == "Categorias":
    page_header("🏷️", "Categorias", "Organize seus produtos por categoria")
    c1,c2 = st.columns([3,1])
    nc = c1.text_input("Nova categoria", label_visibility="collapsed",
                       placeholder="Ex: Eletrônicos, Alimentação…")
    if c2.button("➕  Adicionar", use_container_width=True):
        if not validate_required(nc): show_error("Digite o nome da categoria.")
        else:
            res = qry("INSERT INTO categorias(empresa_id,nome) VALUES(%s,%s)",
                      (EMPRESA_ID, nc.strip()))
            if res is True:
                registrar_log("sistema", f"Categoria '{nc}' criada")
                show_success("Categoria adicionada!", f"'{nc}' criada."); st.rerun()
            elif res == "duplicate": show_error("Essa categoria já está cadastrada.")
            else: show_error("Não foi possível salvar.")
    bc = st.text_input("🔍  Buscar categoria", placeholder="Nome…", key="busca_cat")
    cats = qry("SELECT id,nome,ativo FROM categorias WHERE empresa_id=%s ORDER BY nome",
               (EMPRESA_ID,), fetch=True)
    if cats:
        if bc: b=bc.lower(); cats=[c for c in cats if b in (c[1] or "").lower()]
        hr(); st.markdown(f"**{len(cats)} categoria(s)**")
        for cid,cnome,cativo in cats:
            sb = ('<span class="badge ativo">Ativo</span>' if cativo
                  else '<span class="badge inativo">Inativo</span>')
            ic = "" if cativo else " inativo"
            if st.session_state.edit_cat_id == cid:
                with st.form(f"f_edit_cat_{cid}"):
                    nn = st.text_input("Nome *", value=cnome)
                    cs2,cc2 = st.columns(2)
                    sv  = cs2.form_submit_button("💾  Salvar",   use_container_width=True)
                    cn2 = cc2.form_submit_button("✕  Cancelar", use_container_width=True)
                if cn2: st.session_state.edit_cat_id = None; st.rerun()
                if sv:
                    if not validate_required(nn): show_error("O nome não pode estar em branco.")
                    else:
                        res = qry("UPDATE categorias SET nome=%s WHERE id=%s AND empresa_id=%s",
                                  (nn.strip(), cid, EMPRESA_ID))
                        if res is True:
                            qry("UPDATE produtos SET categoria=%s WHERE categoria=%s AND empresa_id=%s",
                                (nn.strip(), cnome, EMPRESA_ID))
                            registrar_log("sistema", f"Categoria '{cnome}' → '{nn}'")
                            show_success("Categoria atualizada!")
                            st.session_state.edit_cat_id = None; st.rerun()
                        elif res == "duplicate": show_error("Já existe uma categoria com esse nome.")
                        else: show_error("Não foi possível salvar.")
            else:
                cn2,ce2,ct2 = st.columns([7,1,1])
                cn2.markdown(
                    f'<div class="aux-row{ic}"><span class="ar-name">🏷️ {cnome}</span>'
                    f'&nbsp;{sb}</div>', unsafe_allow_html=True)
                if ce2.button("✏️", key=f"edit_cat_{cid}", help="Editar"):
                    st.session_state.edit_cat_id = cid; st.rerun()
                if ct2.button("🔴" if cativo else "🟢", key=f"tog_cat_{cid}",
                              help="Inativar" if cativo else "Ativar"):
                    qry("UPDATE categorias SET ativo=%s WHERE id=%s AND empresa_id=%s",
                        (not cativo, cid, EMPRESA_ID)); st.rerun()
    else:
        show_info("Nenhuma categoria cadastrada.", "Adicione uma acima.")

# ══════════════════════════════════════════════════════════════
#  11. FORMAS DE PAGAMENTO
# ══════════════════════════════════════════════════════════════
elif menu == "Formas de Pagamento":
    page_header("💳", "Formas de Pagamento", "Gerencie as formas de pagamento aceitas")
    c1,c2 = st.columns([3,1])
    np2 = c1.text_input("Nova forma de pagamento", label_visibility="collapsed",
                        placeholder="Ex: Dinheiro, Pix, Cartão…")
    if c2.button("➕  Adicionar", use_container_width=True):
        if not validate_required(np2): show_error("Digite o nome da forma de pagamento.")
        else:
            res = qry("INSERT INTO pagamentos(empresa_id,nome) VALUES(%s,%s)",
                      (EMPRESA_ID, np2.strip()))
            if res is True:
                registrar_log("sistema", f"Forma de pagamento '{np2}' criada")
                show_success("Forma de pagamento adicionada!", f"'{np2}' criada."); st.rerun()
            elif res == "duplicate": show_error("Essa forma de pagamento já está cadastrada.")
            else: show_error("Não foi possível salvar.")
    bp = st.text_input("🔍  Buscar forma de pagamento", placeholder="Nome…", key="busca_pag")
    pags = qry("SELECT id,nome,ativo FROM pagamentos WHERE empresa_id=%s ORDER BY nome",
               (EMPRESA_ID,), fetch=True)
    if pags:
        if bp: b=bp.lower(); pags=[p for p in pags if b in (p[1] or "").lower()]
        hr(); st.markdown(f"**{len(pags)} forma(s) de pagamento**")
        for pid,pnome,pativo in pags:
            sb = ('<span class="badge ativo">Ativo</span>' if pativo
                  else '<span class="badge inativo">Inativo</span>')
            ic = "" if pativo else " inativo"
            if st.session_state.edit_pag_id == pid:
                with st.form(f"f_edit_pag_{pid}"):
                    nn = st.text_input("Nome *", value=pnome)
                    cs3,cc3 = st.columns(2)
                    sv3 = cs3.form_submit_button("💾  Salvar",   use_container_width=True)
                    cn3 = cc3.form_submit_button("✕  Cancelar", use_container_width=True)
                if cn3: st.session_state.edit_pag_id = None; st.rerun()
                if sv3:
                    if not validate_required(nn): show_error("O nome não pode estar em branco.")
                    else:
                        res = qry("UPDATE pagamentos SET nome=%s WHERE id=%s AND empresa_id=%s",
                                  (nn.strip(), pid, EMPRESA_ID))
                        if res is True:
                            show_success("Forma de pagamento atualizada!")
                            st.session_state.edit_pag_id = None; st.rerun()
                        elif res == "duplicate": show_error("Já existe uma forma de pagamento com esse nome.")
                        else: show_error("Não foi possível salvar.")
            else:
                pn2,pe2,pt2 = st.columns([7,1,1])
                pn2.markdown(
                    f'<div class="aux-row{ic}"><span class="ar-name">💳 {pnome}</span>'
                    f'&nbsp;{sb}</div>', unsafe_allow_html=True)
                if pe2.button("✏️", key=f"edit_pag_{pid}", help="Editar"):
                    st.session_state.edit_pag_id = pid; st.rerun()
                if pt2.button("🔴" if pativo else "🟢", key=f"tog_pag_{pid}",
                              help="Inativar" if pativo else "Ativar"):
                    qry("UPDATE pagamentos SET ativo=%s WHERE id=%s AND empresa_id=%s",
                        (not pativo, pid, EMPRESA_ID)); st.rerun()
    else:
        show_info("Nenhuma forma de pagamento cadastrada.", "Adicione uma acima.")

# ══════════════════════════════════════════════════════════════
#  12. CONFIGURAÇÕES
# ══════════════════════════════════════════════════════════════
elif menu == "Configurações":
    page_header("⚙️", "Configurações", "Personalize o sistema")
    er = qry("SELECT nome,moeda,COALESCE(email_alerta,'') FROM empresas WHERE id=%s",
             (EMPRESA_ID,), fetch=True)
    ena = er[0][0] if er else ""
    ema = er[0][1] if er and er[0][1] else "R$"
    eml = er[0][2] if er else ""

    with st.form("f_config"):
        st.markdown("#### Empresa")
        c1,c2 = st.columns(2)
        nn  = c1.text_input("Nome da Empresa *", value=ena)
        eml2 = c2.text_input("E-mail para alertas de estoque", value=eml,
                              placeholder="gestor@empresa.com")
        st.markdown("#### Preferências")
        ms  = ["R$","$","€","£"]
        im  = ms.index(ema) if ema in ms else 0
        nc2 = st.selectbox("Moeda padrão", ms, index=im)
        saved = st.form_submit_button("💾  Salvar Configurações", use_container_width=True)

    if saved:
        if not validate_required(nn):
            show_error("O nome da empresa não pode estar em branco.")
        else:
            qry("UPDATE empresas SET nome=%s,moeda=%s,email_alerta=%s WHERE id=%s",
                (nn.strip(), nc2, eml2.strip(), EMPRESA_ID))
            st.session_state.empresa_nome  = nn.strip()
            st.session_state.empresa_moeda = nc2
            # Reseta flag de alerta para disparar novamente se necessário
            st.session_state.pop("alerta_enviado", None)
            registrar_log("sistema", "Configurações da empresa atualizadas")
            show_success("Configurações salvas!", "As alterações já estão ativas.")
            st.rerun()

    # Gerenciar usuários (admin)
    if st.session_state.usuario_perfil == "admin":
        hr(); st.markdown("#### 👤 Usuários da empresa")
        with st.expander("➕  Novo usuário"):
            with st.form("f_novo_user"):
                cu1,cu2 = st.columns(2)
                un = cu1.text_input("Nome *"); ue = cu2.text_input("E-mail *")
                cu3,cu4 = st.columns(2)
                us = cu3.text_input("Senha *", type="password")
                up = cu4.selectbox("Perfil", ["operador","admin"])
                au = st.form_submit_button("➕  Criar Usuário", use_container_width=True)
            if au:
                if not validate_required(un, ue, us):
                    show_error("Nome, e-mail e senha são obrigatórios.")
                else:
                    res = qry("INSERT INTO usuarios(empresa_id,nome,email,senha_hash,perfil) "
                              "VALUES(%s,%s,%s,%s,%s)",
                              (EMPRESA_ID,un.strip(),ue.strip().lower(),hash_senha(us),up))
                    if res is True:
                        registrar_log("sistema", f"Usuário '{un}' criado")
                        show_success("Usuário criado!", f"'{un}' pode acessar o sistema."); st.rerun()
                    elif res == "duplicate": show_error("Já existe um usuário com esse e-mail.")
                    else: show_error("Não foi possível criar o usuário.")

        usuarios = qry("SELECT id,nome,email,perfil,ativo FROM usuarios "
                       "WHERE empresa_id=%s ORDER BY nome", (EMPRESA_ID,), fetch=True)
        if usuarios:
            for uid2,un2,ue2,up2,ua2 in usuarios:
                sb2 = ('<span class="badge ativo">Ativo</span>' if ua2
                       else '<span class="badge inativo">Inativo</span>')
                cu,cut = st.columns([9,1])
                cu.markdown(
                    f'<div class="card"><div class="card-title">{un2} &nbsp;{sb2}</div>'
                    f'<div class="card-sub">{ue2} · {up2}</div></div>',
                    unsafe_allow_html=True)
                if uid2 != st.session_state.usuario_id:
                    if cut.button("🔴" if ua2 else "🟢", key=f"tog_u_{uid2}",
                                  help="Inativar" if ua2 else "Ativar"):
                        qry("UPDATE usuarios SET ativo=%s WHERE id=%s AND empresa_id=%s",
                            (not ua2, uid2, EMPRESA_ID))
                        registrar_log("sistema", f"Usuário '{un2}' {'inativado' if ua2 else 'ativado'}")
                        st.rerun()

    # Info sobre SMTP para alertas
    hr()
    st.markdown("#### 📧 Configuração de e-mail (alertas de estoque)")
    st.markdown("""
Para ativar o envio de alertas de estoque por e-mail, adicione no seu `secrets.toml`:

```toml
[smtp]
host       = "smtp.gmail.com"
port       = 465
user       = "seu@email.com"
password   = "sua_senha_de_app"
from_email = "seu@email.com"
```

E preencha o campo **"E-mail para alertas"** nas configurações acima.
""")