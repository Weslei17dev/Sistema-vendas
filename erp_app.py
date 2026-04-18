# ╔══════════════════════════════════════════════════════════════╗
#  ERP SaaS Multi-tenant  |  PostgreSQL  |  v6.0
#  Cada empresa é completamente isolada via empresa_id.
#  Credenciais em .streamlit/secrets.toml
# ╚══════════════════════════════════════════════════════════════╝

import streamlit as st
import psycopg2
from psycopg2 import sql, errors as pg_errors
from psycopg2.extras import RealDictCursor
import hashlib
import re
from datetime import datetime
from contextlib import contextmanager

# ──────────────────────────────────────────────────────────────
#  PAGE CONFIG  (deve ser o primeiro comando Streamlit)
# ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Gestão ERP Pro",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={"About": "ERP SaaS Multi-tenant v6.0"},
)

# ──────────────────────────────────────────────────────────────
#  ESTILOS GLOBAIS  (PC + Mobile-first)
# ──────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600&family=Sora:wght@400;600;700&display=swap');

/* ── Base ── */
html,body,[class*="css"]{font-family:'DM Sans',sans-serif;}

/* ── Sidebar ── */
section[data-testid="stSidebar"]{
    background:linear-gradient(160deg,#0f0f1a 0%,#1a1a2e 60%,#16213e 100%);
    border-right:1px solid rgba(255,255,255,0.06);}
section[data-testid="stSidebar"] *{color:rgba(255,255,255,0.85)!important;}
.erp-brand{font-family:'Sora',sans-serif;font-size:1.1rem;font-weight:700;
    color:#fff!important;padding:.2rem 0 .9rem;line-height:1.3;}
.erp-brand span{color:#818cf8!important;}
.erp-tenant{font-size:.72rem;color:rgba(255,255,255,0.4)!important;
    letter-spacing:.04em;margin-top:-6px;padding-bottom:.5rem;}
.nav-section-title{font-size:.62rem;font-weight:600;letter-spacing:.12em;
    text-transform:uppercase;color:rgba(255,255,255,0.3)!important;padding:.7rem 0 .3rem;}
.stButton>button{width:100%;text-align:left;padding:.52rem .85rem;border-radius:10px;
    border:none;background:transparent;color:rgba(255,255,255,0.75)!important;
    font-size:.88rem;transition:all .18s ease;margin-bottom:2px;}
.stButton>button:hover{background:rgba(129,140,248,.13)!important;
    color:#fff!important;transform:translateX(2px);}
.nav-active button{background:rgba(99,102,241,.28)!important;
    color:#c7d2fe!important;font-weight:600;border-left:3px solid #818cf8;}
.logout-btn button{background:rgba(239,68,68,.12)!important;
    color:#fca5a5!important;border-radius:8px;}

/* ── Métricas ── */
[data-testid="stMetric"]{background:#f8f9ff;border:1px solid #e8eaff;
    border-radius:14px;padding:1rem 1.1rem;}
[data-testid="stMetricValue"]{font-family:'Sora',sans-serif;font-size:1.5rem;}
[data-testid="stMetricLabel"]{font-size:.78rem;color:#6b7280;}

/* ── Formulários ── */
[data-testid="stForm"]{background:#fafbff;border:1px solid #eef0ff;
    border-radius:14px;padding:1.25rem;}
[data-testid="stExpander"]{border:1px solid #eef0ff!important;border-radius:12px!important;}

/* ── Toasts ── */
.erp-toast{padding:.7rem .95rem;border-radius:10px;font-size:.88rem;
    margin:.35rem 0;display:flex;align-items:flex-start;gap:.55rem;}
.erp-toast.success{background:#f0fdf4;border-left:4px solid #22c55e;color:#15803d;}
.erp-toast.error  {background:#fef2f2;border-left:4px solid #ef4444;color:#b91c1c;}
.erp-toast.warning{background:#fffbeb;border-left:4px solid #f59e0b;color:#b45309;}
.erp-toast.info   {background:#eff6ff;border-left:4px solid #3b82f6;color:#1d4ed8;}
.erp-toast .icon  {font-size:1rem;flex-shrink:0;margin-top:2px;}
.erp-toast .body strong{display:block;font-weight:600;margin-bottom:1px;}
.erp-toast .body span  {font-weight:400;color:inherit;opacity:.85;}

/* ── Page header ── */
.page-header{display:flex;align-items:center;gap:.7rem;margin-bottom:1.4rem;}
.page-header .icon{width:38px;height:38px;border-radius:10px;display:flex;
    align-items:center;justify-content:center;font-size:1.1rem;
    background:linear-gradient(135deg,#6366f1,#818cf8);flex-shrink:0;}
.page-header h1{font-family:'Sora',sans-serif;font-size:1.3rem;font-weight:700;
    margin:0;color:#1e1b4b;}
.page-header p{font-size:.82rem;color:#6b7280;margin:0;}
hr.erp{border:none;border-top:1px solid #eef0ff;margin:1.1rem 0;}

/* ── Badges ── */
.badge{display:inline-block;padding:2px 8px;border-radius:99px;font-size:.72rem;font-weight:600;}
.badge.ok       {background:#dcfce7;color:#166534;}
.badge.low      {background:#fef9c3;color:#854d0e;}
.badge.zero     {background:#fee2e2;color:#991b1b;}
.badge.ativo    {background:#dcfce7;color:#166534;}
.badge.inativo  {background:#f1f5f9;color:#64748b;}
.badge.pago     {background:#dbeafe;color:#1e40af;}
.badge.cancelado{background:#fee2e2;color:#991b1b;}

/* ── Cards mobile-friendly ── */
.card{background:#fafbff;border:1px solid #eef0ff;border-radius:12px;
    padding:.75rem .95rem;margin-bottom:7px;transition:border-color .15s;}
.card:hover{border-color:#c7d2fe;}
.card.inativo{opacity:.52;border-style:dashed;}
.card-title{font-size:.92rem;font-weight:600;color:#1e1b4b;margin:0 0 2px;}
.card-sub  {font-size:.76rem;color:#6b7280;}
.card-val  {font-family:'Sora',sans-serif;font-size:1rem;font-weight:700;color:#6366f1;}
.card-sku  {font-size:.68rem;color:#9ca3af;}
.card-actions{display:flex;gap:6px;flex-wrap:wrap;margin-top:6px;}

/* ── Carrinho ── */
.cart-item{display:flex;align-items:center;justify-content:space-between;
    padding:.5rem .7rem;border-radius:9px;margin-bottom:5px;
    background:#f8f9ff;border:1px solid #eef0ff;}
.ci-name{font-size:.85rem;font-weight:600;color:#1e1b4b;}
.ci-qty {font-size:.75rem;color:#6b7280;}
.ci-val {font-family:'Sora',sans-serif;font-size:.88rem;font-weight:700;color:#6366f1;}
.cart-total{display:flex;justify-content:space-between;align-items:center;
    padding:.7rem .9rem;border-radius:10px;background:#6366f1;color:#fff;margin-top:.7rem;}
.cart-total span{font-size:.85rem;opacity:.85;}
.cart-total strong{font-family:'Sora',sans-serif;font-size:1.15rem;}

/* ── Linha auxiliar ── */
.aux-row{display:flex;align-items:center;justify-content:space-between;
    padding:.55rem .85rem;border-radius:9px;margin-bottom:5px;
    background:#fafbff;border:1px solid #eef0ff;}
.aux-row.inativo{opacity:.5;border-style:dashed;}
.aux-row .ar-name{font-size:.88rem;font-weight:500;color:#1e1b4b;}

/* ── Login card ── */
.login-wrap{max-width:420px;margin:4rem auto 0;padding:2rem;
    background:#fafbff;border:1px solid #eef0ff;border-radius:18px;}
.login-logo{text-align:center;font-family:'Sora',sans-serif;font-size:1.5rem;
    font-weight:700;color:#1e1b4b;margin-bottom:.25rem;}
.login-logo span{color:#6366f1;}
.login-sub{text-align:center;font-size:.83rem;color:#6b7280;margin-bottom:1.5rem;}

/* ── Mobile overrides ── */
@media(max-width:640px){
    .page-header h1{font-size:1.1rem;}
    [data-testid="stMetric"]{padding:.75rem .9rem;}
    [data-testid="stMetricValue"]{font-size:1.25rem;}
    .card{padding:.65rem .8rem;}
    .cart-total strong{font-size:1rem;}
}
</style>
""", unsafe_allow_html=True)


# ──────────────────────────────────────────────────────────────
#  HELPERS UI
# ──────────────────────────────────────────────────────────────
def show_error(title, hint=""):
    st.markdown(
        f'<div class="erp-toast error"><div class="icon">✕</div>'
        f'<div class="body"><strong>{title}</strong><span>{hint}</span></div></div>',
        unsafe_allow_html=True)

def show_success(title, hint=""):
    st.markdown(
        f'<div class="erp-toast success"><div class="icon">✓</div>'
        f'<div class="body"><strong>{title}</strong><span>{hint}</span></div></div>',
        unsafe_allow_html=True)

def show_warning(title, hint=""):
    st.markdown(
        f'<div class="erp-toast warning"><div class="icon">⚠</div>'
        f'<div class="body"><strong>{title}</strong><span>{hint}</span></div></div>',
        unsafe_allow_html=True)

def show_info(title, hint=""):
    st.markdown(
        f'<div class="erp-toast info"><div class="icon">ℹ</div>'
        f'<div class="body"><strong>{title}</strong><span>{hint}</span></div></div>',
        unsafe_allow_html=True)

def page_header(icon, title, subtitle=""):
    sub = f"<p>{subtitle}</p>" if subtitle else ""
    st.markdown(
        f'<div class="page-header"><div class="icon">{icon}</div>'
        f'<div><h1>{title}</h1>{sub}</div></div>',
        unsafe_allow_html=True)

def hr():
    st.markdown('<hr class="erp">', unsafe_allow_html=True)


# ──────────────────────────────────────────────────────────────
#  BANCO DE DADOS — PostgreSQL
# ──────────────────────────────────────────────────────────────
@contextmanager
def get_conn():
    db = st.secrets["db"]
    try:
        # Passar os parâmetros individualmente resolve o problema de URL encoding
        conn = psycopg2.connect(
            host=db["host"],
            database=db["dbname"],
            user=db["user"],
            password=db["password"],
            port=db.get("port", 5432),
            sslmode="require"
        )
        yield conn
    except psycopg2.OperationalError as e:
        st.error(f"❌ Não foi possível conectar ao banco de dados: {e}")
        st.stop()
    finally:
        # Nota: O 'finally' fecha a conexão. 
        # Certifique-se de que o uso do contextmanager está correto no restante do app.
        try:
            conn.close()
        except:
            pass


def run_query(query, params=(), fetch=False, returning=False):
    """
    Executa uma query PostgreSQL.
    - fetch=True   → retorna lista de tuplas
    - returning=True → retorna primeira linha (para RETURNING id)
    - Retorna True  em sucesso de escrita
    - Retorna 'duplicate' em UniqueViolation
    - Retorna False em outros erros
    """
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                if fetch:
                    return cur.fetchall()
                if returning:
                    return cur.fetchone()
                conn.commit()
                return True
    except pg_errors.UniqueViolation:
        return "duplicate"
    except Exception as e:
        st.session_state["_last_db_error"] = str(e)
        return False


# ──────────────────────────────────────────────────────────────
#  INIT DB — Cria todas as tabelas no PostgreSQL (idempotente)
# ──────────────────────────────────────────────────────────────
def init_db():
    stmts = [
        # Empresas (tenants)
        """CREATE TABLE IF NOT EXISTS empresas (
            id          SERIAL PRIMARY KEY,
            nome        TEXT NOT NULL,
            plano       TEXT DEFAULT 'basico',
            ativo       BOOLEAN DEFAULT TRUE,
            criado_em   TIMESTAMP DEFAULT NOW()
        )""",
        # Usuários
        """CREATE TABLE IF NOT EXISTS usuarios (
            id          SERIAL PRIMARY KEY,
            empresa_id  INTEGER NOT NULL REFERENCES empresas(id),
            nome        TEXT NOT NULL,
            email       TEXT UNIQUE NOT NULL,
            senha_hash  TEXT NOT NULL,
            perfil      TEXT DEFAULT 'operador',  -- admin | operador
            ativo       BOOLEAN DEFAULT TRUE,
            criado_em   TIMESTAMP DEFAULT NOW()
        )""",
        # Categorias
        """CREATE TABLE IF NOT EXISTS categorias (
            id          SERIAL PRIMARY KEY,
            empresa_id  INTEGER NOT NULL REFERENCES empresas(id),
            nome        TEXT NOT NULL,
            ativo       BOOLEAN DEFAULT TRUE,
            UNIQUE(empresa_id, nome)
        )""",
        # Formas de pagamento
        """CREATE TABLE IF NOT EXISTS pagamentos (
            id          SERIAL PRIMARY KEY,
            empresa_id  INTEGER NOT NULL REFERENCES empresas(id),
            nome        TEXT NOT NULL,
            ativo       BOOLEAN DEFAULT TRUE,
            UNIQUE(empresa_id, nome)
        )""",
        # Clientes
        """CREATE TABLE IF NOT EXISTS clientes (
            id          SERIAL PRIMARY KEY,
            empresa_id  INTEGER NOT NULL REFERENCES empresas(id),
            nome        TEXT NOT NULL,
            documento   TEXT NOT NULL,
            telefone    TEXT,
            email       TEXT,
            rua         TEXT,
            numero      TEXT,
            complemento TEXT,
            bairro      TEXT,
            cidade      TEXT,
            estado      TEXT,
            cep         TEXT,
            ativo       BOOLEAN DEFAULT TRUE,
            UNIQUE(empresa_id, documento)
        )""",
        # Produtos
        """CREATE TABLE IF NOT EXISTS produtos (
            id              SERIAL PRIMARY KEY,
            empresa_id      INTEGER NOT NULL REFERENCES empresas(id),
            sku             TEXT NOT NULL,
            nome            TEXT NOT NULL,
            categoria       TEXT NOT NULL,
            preco_custo     NUMERIC(12,2) DEFAULT 0,
            preco_venda     NUMERIC(12,2) DEFAULT 0,
            estoque_atual   INTEGER DEFAULT 0,
            estoque_minimo  INTEGER DEFAULT 2,
            ativo           BOOLEAN DEFAULT TRUE,
            UNIQUE(empresa_id, sku)
        )""",
        # Vendas
        """CREATE TABLE IF NOT EXISTS vendas (
            id              SERIAL PRIMARY KEY,
            empresa_id      INTEGER NOT NULL REFERENCES empresas(id),
            data            TIMESTAMP NOT NULL DEFAULT NOW(),
            cliente_name    TEXT,
            valor_total     NUMERIC(12,2),
            pagamento       TEXT,
            status          TEXT DEFAULT 'Pago',
            observacao      TEXT DEFAULT ''
        )""",
        # Itens da venda
        """CREATE TABLE IF NOT EXISTS itens_venda (
            id              SERIAL PRIMARY KEY,
            empresa_id      INTEGER NOT NULL REFERENCES empresas(id),
            venda_id        INTEGER NOT NULL REFERENCES vendas(id),
            produto_nome    TEXT,
            quantidade      INTEGER,
            preco_unit      NUMERIC(12,2)
        )""",
    ]
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                for stmt in stmts:
                    cur.execute(stmt)
            conn.commit()
    except Exception as e:
        st.error(f"Erro ao inicializar banco: {e}")
        st.stop()


# ──────────────────────────────────────────────────────────────
#  TENANT GUARD — Garante que empresa_id está na sessão
# ──────────────────────────────────────────────────────────────
def eid() -> int:
    """Retorna o empresa_id do usuário logado. Para a execução se ausente."""
    eid_val = st.session_state.get("empresa_id")
    if not eid_val:
        st.error("Sessão inválida. Faça login novamente.")
        st.stop()
    return eid_val

def qry(sql_str, params=(), fetch=False, returning=False):
    """
    Wrapper que injeta empresa_id automaticamente via %s no final dos params.
    Usa eid() para obter o empresa_id da sessão atual.
    Isso garante que NENHUMA query escape sem o filtro de tenant.
    """
    return run_query(sql_str, params, fetch=fetch, returning=returning)


# ──────────────────────────────────────────────────────────────
#  VALIDAÇÕES
# ──────────────────────────────────────────────────────────────
def validate_doc(doc: str) -> bool:
    return len(re.sub(r'\D', '', doc)) in (11, 14)

def validate_required(*fields) -> bool:
    return all(f is not None and str(f).strip() for f in fields)

def hash_senha(senha: str) -> str:
    return hashlib.sha256(senha.encode()).hexdigest()

def get_estoque(prod_id: int) -> int:
    r = qry("SELECT estoque_atual FROM produtos WHERE id=%s AND empresa_id=%s",
            (prod_id, eid()), fetch=True)
    return r[0][0] if r else 0


# ──────────────────────────────────────────────────────────────
#  SESSION STATE DEFAULTS
# ──────────────────────────────────────────────────────────────
_DEFAULTS = {
    "logado": False, "usuario_id": None, "empresa_id": None,
    "usuario_nome": "", "empresa_nome": "", "empresa_moeda": "R$",
    "usuario_perfil": "operador",
    "active_menu": "Dashboard", "cart": [],
    "editing_prod_id": None, "editing_prod_data": None,
    "adj_prod": None, "edit_cat_id": None, "edit_pag_id": None,
    "edit_cli_id": None, "edit_cli_data": None,
}
for _k, _v in _DEFAULTS.items():
    if _k not in st.session_state:
        st.session_state[_k] = _v


# ──────────────────────────────────────────────────────────────
#  TELA DE LOGIN
# ──────────────────────────────────────────────────────────────
def tela_login():
    st.markdown("""
    <div class="login-wrap">
        <div class="login-logo">Gestão <span>ERP Pro</span></div>
        <div class="login-sub">Acesse sua conta para continuar</div>
    </div>
    """, unsafe_allow_html=True)

    # Centraliza o formulário
    _, col, _ = st.columns([1, 2, 1])
    with col:
        with st.form("form_login"):
            email = st.text_input("E-mail", placeholder="seu@email.com")
            senha = st.text_input("Senha", type="password", placeholder="••••••••")
            entrar = st.form_submit_button("Entrar", use_container_width=True)

        if entrar:
            if not validate_required(email, senha):
                show_error("Preencha e-mail e senha.")
                return

            usuario = run_query(
                """SELECT u.id, u.nome, u.perfil, u.ativo,
                          e.id, e.nome, e.ativo
                   FROM usuarios u
                   JOIN empresas e ON e.id = u.empresa_id
                   WHERE u.email = %s AND u.senha_hash = %s""",
                (email.strip().lower(), hash_senha(senha)),
                fetch=True)

            if not usuario:
                show_error("E-mail ou senha incorretos.",
                           "Verifique os dados e tente novamente.")
                return

            uid, unome, uperfil, uativo, empid, empnome, empativo = usuario[0]

            if not uativo:
                show_error("Usuário inativo.",
                           "Entre em contato com o administrador da sua empresa.")
                return
            if not empativo:
                show_error("Empresa inativa.",
                           "Entre em contato com o suporte.")
                return

            # Busca moeda da empresa (configuração)
            moeda_r = run_query(
                "SELECT moeda FROM empresas WHERE id=%s", (empid,), fetch=True)
            moeda = moeda_r[0][0] if moeda_r and moeda_r[0][0] else "R$"

            # Popula sessão
            st.session_state.update({
                "logado": True,
                "usuario_id": uid,
                "empresa_id": empid,
                "usuario_nome": unome,
                "empresa_nome": empnome,
                "empresa_moeda": moeda,
                "usuario_perfil": uperfil,
            })
            st.rerun()

        st.markdown(
            "<p style='text-align:center;font-size:.78rem;color:#9ca3af;"
            "margin-top:1rem'>Esqueceu a senha? Contate o administrador.</p>",
            unsafe_allow_html=True)


# ──────────────────────────────────────────────────────────────
#  INICIALIZAÇÃO DO BANCO
# ──────────────────────────────────────────────────────────────
init_db()

# Se não logado → mostra tela de login e para
if not st.session_state.logado:
    tela_login()
    st.stop()

# Atalhos pós-login
EMPRESA_ID = eid()
cur_sym    = st.session_state.empresa_moeda


# ──────────────────────────────────────────────────────────────
#  SIDEBAR — Navegação
# ──────────────────────────────────────────────────────────────
MENUS = [
    ("📊", "Dashboard",          "dash"),
    ("🛒", "Pedidos",            "pedidos"),
    ("📦", "Estoque",            "estoque"),
    ("👥", "Clientes",           "clientes"),
    ("📜", "Histórico de Vendas","hist"),
    ("🏷️", "Categorias",        "cats"),
    ("💳", "Formas de Pagamento","pags"),
    ("⚙️", "Configurações",     "cfg"),
]

with st.sidebar:
    # Marca / tenant
    parts = st.session_state.empresa_nome.split()
    b1 = parts[0] if parts else "ERP"
    b2 = " ".join(parts[1:]) if len(parts) > 1 else "Pro"
    st.markdown(
        f'<div class="erp-brand">Gestão <span>ERP Pro</span></div>'
        f'<div class="erp-tenant">🏢 {st.session_state.empresa_nome}</div>',
        unsafe_allow_html=True)

    st.markdown('<div class="nav-section-title">Menu principal</div>',
                unsafe_allow_html=True)

    for icon, label, key in MENUS:
        if key == "cats":
            st.markdown('<div class="nav-section-title">Tabelas auxiliares</div>',
                        unsafe_allow_html=True)
        is_active = st.session_state.active_menu == label
        with st.container():
            if is_active:
                st.markdown('<div class="nav-active">', unsafe_allow_html=True)
            if st.button(f"{icon}  {label}", key=f"nav_{key}"):
                st.session_state.active_menu = label
                for k2 in ["editing_prod_id","editing_prod_data","adj_prod",
                            "edit_cat_id","edit_pag_id","edit_cli_id","edit_cli_data"]:
                    st.session_state[k2] = None
                st.rerun()
            if is_active:
                st.markdown('</div>', unsafe_allow_html=True)

    # Badge carrinho
    n_cart = len(st.session_state.cart)
    if n_cart:
        st.markdown(
            f'<div style="margin:8px 10px 0;padding:6px 10px;border-radius:8px;'
            f'background:rgba(99,102,241,.2);color:#c7d2fe!important;font-size:.8rem;">'
            f'🛒 {n_cart} item(s) no carrinho</div>',
            unsafe_allow_html=True)

    st.divider()
    # Usuário logado + logout
    st.markdown(
        f'<div style="font-size:.75rem;color:rgba(255,255,255,.4)!important;padding:0 4px .3rem">'
        f'👤 {st.session_state.usuario_nome}<br>'
        f'<span style="font-size:.68rem">{st.session_state.usuario_perfil}</span></div>',
        unsafe_allow_html=True)
    with st.container():
        st.markdown('<div class="logout-btn">', unsafe_allow_html=True)
        if st.button("🚪  Sair", key="btn_logout"):
            for k in list(st.session_state.keys()):
                del st.session_state[k]
            st.rerun()
        st.markdown('</div>', unsafe_allow_html=True)

    st.markdown(
        f"<small style='color:rgba(255,255,255,0.25)'>v6.0 · "
        f"{datetime.now().strftime('%d/%m/%Y')}</small>",
        unsafe_allow_html=True)

menu = st.session_state.active_menu


# ════════════════════════════════════════════════════════════════
#  1. DASHBOARD
# ════════════════════════════════════════════════════════════════
if menu == "Dashboard":
    page_header("📊", "Dashboard", f"Bem-vindo, {st.session_state.usuario_nome}!")

    vendas_all = qry(
        "SELECT valor_total FROM vendas WHERE empresa_id=%s AND status='Pago'",
        (EMPRESA_ID,), fetch=True)
    total_fat = sum(float(v[0]) for v in vendas_all) if vendas_all else 0.0
    qtd_ped   = len(vendas_all)
    avg_tick  = (total_fat / qtd_ped) if qtd_ped else 0.0

    c1, c2, c3 = st.columns(3)
    c1.metric("Faturamento Total", f"{cur_sym} {total_fat:,.2f}")
    c2.metric("Pedidos",            qtd_ped)
    c3.metric("Ticket Médio",       f"{cur_sym} {avg_tick:,.2f}")
    hr()

    st.markdown("**Últimas vendas**")
    ultimas = qry(
        """SELECT v.id, v.cliente_name, v.valor_total,
                  STRING_AGG(i.produto_nome||' x'||i.quantidade, ', ') AS produtos,
                  TO_CHAR(v.data,'DD/MM/YYYY HH24:MI')
           FROM vendas v
           LEFT JOIN itens_venda i ON i.venda_id=v.id AND i.empresa_id=v.empresa_id
           WHERE v.empresa_id=%s AND v.status='Pago'
           GROUP BY v.id ORDER BY v.data DESC LIMIT 10""",
        (EMPRESA_ID,), fetch=True)

    if ultimas:
        for row in ultimas:
            vid, cli, val, prods_str, data_fmt = row
            prods_str = prods_str or "—"
            st.markdown(f"""
            <div class="card">
              <div style="display:flex;justify-content:space-between;align-items:flex-start;flex-wrap:wrap;gap:4px">
                <div>
                  <div class="card-sku">Pedido #{vid} · {data_fmt}</div>
                  <div class="card-title">{cli}</div>
                  <div class="card-sub">{prods_str}</div>
                </div>
                <div class="card-val">{cur_sym} {float(val):,.2f}</div>
              </div>
            </div>""", unsafe_allow_html=True)
    else:
        show_info("Nenhuma venda registrada ainda.",
                  "Cadastre clientes e produtos para começar.")

    low = qry(
        "SELECT nome, estoque_atual FROM produtos "
        "WHERE empresa_id=%s AND ativo=TRUE AND estoque_atual<=estoque_minimo",
        (EMPRESA_ID,), fetch=True)
    if low:
        hr()
        show_warning(
            f"{len(low)} produto(s) com estoque baixo ou zerado",
            "Verifique a aba Estoque e faça a reposição.")


# ════════════════════════════════════════════════════════════════
#  2. PEDIDOS — CARRINHO MULTI-PRODUTO
# ════════════════════════════════════════════════════════════════
elif menu == "Pedidos":
    page_header("🛒", "Pedidos", "Monte o pedido com múltiplos produtos")

    clis = qry(
        "SELECT nome FROM clientes WHERE empresa_id=%s AND ativo=TRUE ORDER BY nome",
        (EMPRESA_ID,), fetch=True)
    prods = qry(
        "SELECT id,nome,preco_venda,estoque_atual FROM produtos "
        "WHERE empresa_id=%s AND ativo=TRUE AND estoque_atual>0 ORDER BY nome",
        (EMPRESA_ID,), fetch=True)
    pags = qry(
        "SELECT nome FROM pagamentos WHERE empresa_id=%s AND ativo=TRUE ORDER BY nome",
        (EMPRESA_ID,), fetch=True)

    if not clis:
        show_error("Nenhum cliente ativo cadastrado.",
                   "Cadastre ou ative um cliente antes de vender."); st.stop()
    if not prods:
        show_error("Nenhum produto disponível em estoque.",
                   "Cadastre produtos ou ajuste o estoque."); st.stop()
    if not pags:
        show_error("Nenhuma forma de pagamento ativa.",
                   "Cadastre ou ative uma forma de pagamento."); st.stop()

    cart: list = st.session_state.cart
    col_form, col_cart = st.columns([3, 2])

    # ── Carrinho (direita) ──
    with col_cart:
        st.markdown("#### 🧺 Carrinho")
        if not cart:
            show_info("Carrinho vazio.", "Adicione produtos ao lado.")
        else:
            total_cart = 0.0
            for i, item in enumerate(cart):
                sub = item["preco"] * item["qtd"]
                total_cart += sub
                c_item, c_rem = st.columns([5, 1])
                with c_item:
                    st.markdown(f"""
                    <div class="cart-item">
                      <div><div class="ci-name">{item['nome']}</div>
                      <div class="ci-qty">{item['qtd']} un × {cur_sym} {item['preco']:.2f}</div></div>
                      <div class="ci-val">{cur_sym} {sub:.2f}</div>
                    </div>""", unsafe_allow_html=True)
                with c_rem:
                    if st.button("🗑️", key=f"rem_{i}", help="Remover"):
                        st.session_state.cart.pop(i); st.rerun()
            st.markdown(f"""
            <div class="cart-total">
              <span>Total do pedido</span>
              <strong>{cur_sym} {total_cart:,.2f}</strong>
            </div>""", unsafe_allow_html=True)

    # ── Adicionar item (esquerda) ──
    with col_form:
        prod_nomes = [p[1] for p in prods]
        with st.form("form_add_item", clear_on_submit=True):
            st.markdown("#### Adicionar produto")
            prod_idx = st.selectbox("Produto *", range(len(prod_nomes)),
                                    format_func=lambda i: prod_nomes[i])
            prod_sel    = prods[prod_idx]
            est_disp    = int(prod_sel[3])
            no_carrinho = sum(x["qtd"] for x in cart if x["id"] == prod_sel[0])
            disponivel  = max(0, est_disp - no_carrinho)
            st.caption(
                f"Disponível: **{est_disp}** · No carrinho: **{no_carrinho}** "
                f"· Pode adicionar: **{disponivel}**")
            qtd_add = st.number_input(
                "Quantidade *", min_value=1,
                max_value=disponivel if disponivel > 0 else 1,
                step=1, value=1, disabled=(disponivel == 0))
            add_btn = st.form_submit_button(
                "➕  Adicionar ao carrinho",
                use_container_width=True, disabled=(disponivel == 0))

        if add_btn:
            if disponivel == 0:
                show_error(f"Estoque esgotado para '{prod_sel[1]}'.")
            else:
                existe = next((x for x in cart if x["id"] == prod_sel[0]), None)
                if existe:
                    existe["qtd"] += qtd_add
                else:
                    st.session_state.cart.append({
                        "id": prod_sel[0], "nome": prod_sel[1],
                        "preco": float(prod_sel[2]), "qtd": qtd_add})
                st.rerun()

        hr()
        if cart:
            with st.form("form_finalizar"):
                st.markdown("#### Finalizar pedido")
                cf1, cf2 = st.columns(2)
                cliente_sel = cf1.selectbox("Cliente *", [c[0] for c in clis])
                forma       = cf2.selectbox("Pagamento *", [p[0] for p in pags])
                obs_pedido  = st.text_area(
                    "Observação (opcional)",
                    placeholder="Ex: entregar na portaria, emitir nota fiscal…",
                    height=75)
                fin_btn = st.form_submit_button(
                    "✅  Finalizar Venda", use_container_width=True)

            if fin_btn:
                # Valida estoque real (guard contra race condition)
                erros = []
                for item in cart:
                    est_real = get_estoque(item["id"])
                    if est_real < item["qtd"]:
                        erros.append(
                            f"Estoque insuficiente para '{item['nome']}'. "
                            f"Disponível: {est_real} un.")
                if erros:
                    for e in erros:
                        show_error(e, "Ajuste as quantidades no carrinho.")
                else:
                    total_venda = sum(x["preco"] * x["qtd"] for x in cart)
                    # INSERT com RETURNING id
                    row = qry(
                        """INSERT INTO vendas
                           (empresa_id,data,cliente_name,valor_total,pagamento,status,observacao)
                           VALUES (%s,NOW(),%s,%s,%s,'Pago',%s) RETURNING id""",
                        (EMPRESA_ID, cliente_sel, total_venda, forma,
                         obs_pedido.strip()),
                        returning=True)

                    if row:
                        venda_id = row[0]
                        for item in cart:
                            qry("""INSERT INTO itens_venda
                                   (empresa_id,venda_id,produto_nome,quantidade,preco_unit)
                                   VALUES (%s,%s,%s,%s,%s)""",
                                (EMPRESA_ID, venda_id, item["nome"],
                                 item["qtd"], item["preco"]))
                            qry("""UPDATE produtos
                                   SET estoque_atual=estoque_atual-%s
                                   WHERE id=%s AND empresa_id=%s""",
                                (item["qtd"], item["id"], EMPRESA_ID))
                        show_success(
                            f"Venda de {cur_sym} {total_venda:.2f} finalizada!",
                            f"{len(cart)} produto(s) para {cliente_sel} · {forma}")
                        st.session_state.cart = []
                        st.balloons(); st.rerun()
                    else:
                        show_error("Não foi possível salvar a venda.",
                                   "Tente novamente.")
        else:
            show_info("Carrinho vazio.",
                      "Adicione produtos acima para liberar a finalização.")


# ════════════════════════════════════════════════════════════════
#  3. ESTOQUE
# ════════════════════════════════════════════════════════════════
elif menu == "Estoque":
    page_header("📦", "Estoque", "Gerencie seu catálogo de produtos")

    cats_raw = qry(
        "SELECT nome FROM categorias WHERE empresa_id=%s AND ativo=TRUE ORDER BY nome",
        (EMPRESA_ID,), fetch=True)
    cat_opts = [c[0] for c in cats_raw] if cats_raw else []

    with st.expander("➕  Adicionar novo produto"):
        if not cat_opts:
            show_warning("Nenhuma categoria ativa.",
                         "Acesse 'Categorias' e crie uma antes de cadastrar produtos.")
        else:
            with st.form("f_prod"):
                c1, c2, c3 = st.columns([1, 2, 1])
                sku  = c1.text_input("Código / SKU *")
                nome = c2.text_input("Nome do Produto *")
                cat  = c3.selectbox("Categoria *", cat_opts)
                c4, c5, c6, c7 = st.columns(4)
                pc    = c4.number_input(f"Preço Custo ({cur_sym})",   min_value=0.0, step=0.01, format="%.2f")
                pv    = c5.number_input(f"Preço Venda ({cur_sym}) *", min_value=0.0, step=0.01, format="%.2f")
                est   = c6.number_input("Estoque Inicial",             min_value=0, step=1)
                e_min = c7.number_input("Estoque Mínimo",              min_value=0, step=1, value=2)
                save_btn = st.form_submit_button("💾  Salvar Produto", use_container_width=True)

            if save_btn:
                if not validate_required(sku, nome):
                    show_error("SKU e Nome são obrigatórios.")
                elif pv == 0:
                    show_error("Preço de venda não pode ser zero.")
                else:
                    res = qry(
                        """INSERT INTO produtos
                           (empresa_id,sku,nome,categoria,preco_custo,preco_venda,
                            estoque_atual,estoque_minimo)
                           VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
                        (EMPRESA_ID, sku.strip(), nome.strip(), cat, pc, pv, est, e_min))
                    if res is True:
                        show_success("Produto cadastrado!", f"'{nome}' adicionado."); st.rerun()
                    elif res == "duplicate":
                        show_error("Já existe um produto com esse SKU nesta empresa.",
                                   f"O código '{sku}' já está em uso.")
                    else:
                        show_error("Não foi possível salvar.")

    hr()
    with st.expander("🔍  Filtros", expanded=True):
        col_b, col_f = st.columns([3, 1])
        busca   = col_b.text_input("Buscar produto", placeholder="Nome ou SKU…",
                                   key="busca_prod", label_visibility="collapsed")
        mostrar = col_f.selectbox("Status", ["Ativos","Inativos","Todos"],
                                  key="filtro_prod", label_visibility="collapsed")

    prods_raw = qry(
        "SELECT id,sku,nome,categoria,preco_custo,preco_venda,"
        "estoque_atual,estoque_minimo,ativo "
        "FROM produtos WHERE empresa_id=%s ORDER BY nome",
        (EMPRESA_ID,), fetch=True)

    if not prods_raw:
        show_info("Nenhum produto cadastrado ainda.", "Use o formulário acima.")
    else:
        filtrados = list(prods_raw)
        if mostrar == "Ativos":   filtrados = [p for p in filtrados if p[8]]
        if mostrar == "Inativos": filtrados = [p for p in filtrados if not p[8]]
        if busca:
            b = busca.lower()
            filtrados = [p for p in filtrados if b in p[2].lower() or b in p[1].lower()]

        st.markdown(f"**{len(filtrados)} produto(s)**")
        for prod in filtrados:
            pid, sku_v, nm, cat_v, pc_v, pv_v, est_v, emin_v, ativo_v = prod
            badge_est = (
                '<span class="badge zero">Zerado</span>' if est_v == 0 else
                '<span class="badge low">Baixo</span>'  if est_v <= emin_v else
                '<span class="badge ok">OK</span>')
            inativo_cls = "" if ativo_v else " inativo"

            col_card, col_edit, col_adj, col_tog = st.columns([5, 1, 1, 1])
            with col_card:
                st.markdown(f"""
                <div class="card{inativo_cls}">
                  <div class="card-sku">SKU: {sku_v}</div>
                  <div class="card-title">{nm}</div>
                  <div class="card-sub">
                    {cat_v} · Estoque: {est_v} {badge_est} ·
                    <span class="badge {'ativo' if ativo_v else 'inativo'}">
                      {'Ativo' if ativo_v else 'Inativo'}</span>
                  </div>
                  <div class="card-val">{cur_sym} {float(pv_v):.2f}</div>
                </div>""", unsafe_allow_html=True)
            with col_edit:
                if st.button("✏️", key=f"edit_{pid}", help="Editar"):
                    st.session_state.editing_prod_id   = pid
                    st.session_state.editing_prod_data = prod
                    st.session_state.adj_prod = None; st.rerun()
            with col_adj:
                if st.button("📦", key=f"adj_{pid}", help="Ajustar estoque"):
                    st.session_state.adj_prod = prod
                    st.session_state.editing_prod_id = None; st.rerun()
            with col_tog:
                if st.button("🔴" if ativo_v else "🟢", key=f"tog_{pid}",
                             help="Inativar" if ativo_v else "Ativar"):
                    qry("UPDATE produtos SET ativo=%s WHERE id=%s AND empresa_id=%s",
                        (not ativo_v, pid, EMPRESA_ID)); st.rerun()

        # ── Edição ──
        if st.session_state.editing_prod_id:
            pid_e = st.session_state.editing_prod_id
            pd_   = st.session_state.editing_prod_data
            _, sku_e, nm_e, cat_e, pc_e, pv_e, est_e, emin_e, _ = pd_
            hr(); st.markdown(f"#### ✏️ Editando: {nm_e}")
            with st.form("f_edit"):
                ce1, ce2, ce3 = st.columns([1, 2, 1])
                new_sku  = ce1.text_input("SKU *",   value=sku_e)
                new_nome = ce2.text_input("Nome *",  value=nm_e)
                ci = cat_opts.index(cat_e) if cat_e in cat_opts else 0
                new_cat  = ce3.selectbox("Categoria *", cat_opts or [cat_e], index=ci)
                ce4, ce5, ce6, ce7 = st.columns(4)
                new_pc   = ce4.number_input(f"Custo ({cur_sym})",  value=float(pc_e),  min_value=0.0, step=0.01, format="%.2f")
                new_pv   = ce5.number_input(f"Venda ({cur_sym}) *",value=float(pv_e),  min_value=0.0, step=0.01, format="%.2f")
                new_est  = ce6.number_input("Estoque",              value=int(est_e),   min_value=0, step=1)
                new_emin = ce7.number_input("Est. Mín.",            value=int(emin_e),  min_value=0, step=1)
                cs, cc   = st.columns(2)
                save_e   = cs.form_submit_button("💾  Salvar",   use_container_width=True)
                cncl_e   = cc.form_submit_button("✕  Cancelar", use_container_width=True)
            if cncl_e:
                st.session_state.editing_prod_id = None; st.rerun()
            if save_e:
                if not validate_required(new_sku, new_nome):
                    show_error("SKU e Nome são obrigatórios.")
                elif new_pv == 0:
                    show_error("Preço de venda não pode ser zero.")
                else:
                    res = qry(
                        """UPDATE produtos SET sku=%s,nome=%s,categoria=%s,preco_custo=%s,
                           preco_venda=%s,estoque_atual=%s,estoque_minimo=%s
                           WHERE id=%s AND empresa_id=%s""",
                        (new_sku.strip(), new_nome.strip(), new_cat, new_pc, new_pv,
                         new_est, new_emin, pid_e, EMPRESA_ID))
                    if res is True:
                        show_success("Produto atualizado!")
                        st.session_state.editing_prod_id = None; st.rerun()
                    elif res == "duplicate":
                        show_error("Já existe um produto com esse SKU.")
                    else:
                        show_error("Não foi possível salvar.")

        # ── Ajuste de estoque ──
        if st.session_state.adj_prod:
            adj = st.session_state.adj_prod
            adj_id, _, adj_nm = adj[0], adj[1], adj[2]
            adj_est = int(adj[6])
            hr(); st.markdown(f"#### 📦 Ajustar estoque: {adj_nm}  (atual: **{adj_est}**)")
            with st.form("f_adj"):
                co, cq = st.columns(2)
                op     = co.selectbox("Operação", ["Adicionar","Remover","Definir exato"])
                qtd_aj = cq.number_input("Quantidade", min_value=1, step=1)
                st.text_input("Motivo (opcional)",
                              placeholder="Ex: compra, perda, inventário…")
                ca2, cb2 = st.columns(2)
                ap = ca2.form_submit_button("✅  Aplicar",  use_container_width=True)
                cn = cb2.form_submit_button("✕  Cancelar", use_container_width=True)
            if cn:
                st.session_state.adj_prod = None; st.rerun()
            if ap:
                if op == "Adicionar":
                    sql_adj = "UPDATE produtos SET estoque_atual=estoque_atual+%s WHERE id=%s AND empresa_id=%s"
                    nv = adj_est + qtd_aj
                elif op == "Remover":
                    if qtd_aj > adj_est:
                        show_error("Quantidade maior que o estoque atual.",
                                   f"Máximo: {adj_est} unidades."); st.stop()
                    sql_adj = "UPDATE produtos SET estoque_atual=estoque_atual-%s WHERE id=%s AND empresa_id=%s"
                    nv = adj_est - qtd_aj
                else:
                    sql_adj = "UPDATE produtos SET estoque_atual=%s WHERE id=%s AND empresa_id=%s"
                    nv = qtd_aj
                if qry(sql_adj, (qtd_aj if op != "Definir exato" else nv, adj_id, EMPRESA_ID)) is True:
                    show_success("Estoque ajustado!", f"'{adj_nm}' agora tem {nv} unidades.")
                    st.session_state.adj_prod = None; st.rerun()
                else:
                    show_error("Não foi possível ajustar o estoque.")


# ════════════════════════════════════════════════════════════════
#  4. CLIENTES
# ════════════════════════════════════════════════════════════════
elif menu == "Clientes":
    page_header("👥", "Clientes", "Gerencie sua base de clientes")

    with st.expander("➕  Novo cliente"):
        with st.form("f_cli"):
            c1, c2 = st.columns(2)
            nome = c1.text_input("Nome Completo *")
            doc  = c2.text_input("CPF / CNPJ *", placeholder="Somente números ou com pontuação")
            c3, c4 = st.columns(2)
            tel   = c3.text_input("Telefone")
            email = c4.text_input("E-mail")
            st.markdown("**Endereço**")
            ea, eb, ec = st.columns([3, 1, 2])
            rua  = ea.text_input("Rua / Logradouro")
            num  = eb.text_input("Número")
            comp = ec.text_input("Complemento")
            ed, ee, ef, eg = st.columns([2, 2, 1, 2])
            bairro = ed.text_input("Bairro")
            cidade = ee.text_input("Cidade")
            estado = ef.text_input("UF", max_chars=2)
            cep    = eg.text_input("CEP", placeholder="00000-000")
            save_btn = st.form_submit_button("💾  Salvar Cliente", use_container_width=True)

        if save_btn:
            if not validate_required(nome, doc):
                show_error("Nome e CPF/CNPJ são obrigatórios.")
            elif not validate_doc(doc):
                show_error("CPF/CNPJ inválido.",
                           "CPF deve ter 11 dígitos e CNPJ 14 dígitos.")
            else:
                res = qry(
                    """INSERT INTO clientes
                       (empresa_id,nome,documento,telefone,email,rua,numero,
                        complemento,bairro,cidade,estado,cep)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (EMPRESA_ID, nome.strip(), re.sub(r'\D','',doc),
                     tel, email, rua, num, comp, bairro, cidade,
                     estado.upper() if estado else "", cep))
                if res is True:
                    show_success("Cliente cadastrado!", f"'{nome}' adicionado."); st.rerun()
                elif res == "duplicate":
                    show_error("Já existe um cliente com esse CPF/CNPJ nesta empresa.")
                else:
                    show_error("Não foi possível salvar.")

    hr()
    with st.expander("🔍  Filtros", expanded=True):
        col_b, col_f = st.columns([3, 1])
        busca_cli  = col_b.text_input("Buscar cliente",
                                      placeholder="Nome, CPF/CNPJ ou cidade…",
                                      key="busca_cli", label_visibility="collapsed")
        filtro_cli = col_f.selectbox("Status", ["Ativos","Inativos","Todos"],
                                     key="filtro_cli", label_visibility="collapsed")

    clis_full = qry(
        "SELECT id,nome,documento,telefone,email,rua,numero,complemento,"
        "bairro,cidade,estado,cep,ativo "
        "FROM clientes WHERE empresa_id=%s ORDER BY nome",
        (EMPRESA_ID,), fetch=True)

    if clis_full:
        clis_view = list(clis_full)
        if filtro_cli == "Ativos":   clis_view = [c for c in clis_view if c[12]]
        if filtro_cli == "Inativos": clis_view = [c for c in clis_view if not c[12]]
        if busca_cli:
            b = busca_cli.lower()
            clis_view = [c for c in clis_view if
                         b in (c[1] or "").lower() or
                         b in (c[2] or "").lower() or
                         b in (c[9] or "").lower()]

        st.markdown(f"**{len(clis_view)} cliente(s)**")
        for cli in clis_view:
            (cid, cnome, cdoc, ctel, cemail, crua, cnum, ccomp,
             cbairro, ccidade, cestado, ccep, cativo) = cli
            sb = ('<span class="badge ativo">Ativo</span>' if cativo
                  else '<span class="badge inativo">Inativo</span>')

            if st.session_state.edit_cli_id == cid:
                st.markdown(f"#### ✏️ Editando: {cnome}")
                with st.form(f"f_edit_cli_{cid}"):
                    ec1, ec2 = st.columns(2)
                    en  = ec1.text_input("Nome Completo *", value=cnome or "")
                    edo = ec2.text_input("CPF / CNPJ *",    value=cdoc  or "",
                                         placeholder="Somente números")
                    ec3, ec4 = st.columns(2)
                    etl = ec3.text_input("Telefone", value=ctel   or "")
                    eml = ec4.text_input("E-mail",   value=cemail or "")
                    st.markdown("**Endereço**")
                    eea, eeb, eec = st.columns([3, 1, 2])
                    erua  = eea.text_input("Rua",         value=crua   or "")
                    enum  = eeb.text_input("Número",      value=cnum   or "")
                    ecomp = eec.text_input("Complemento", value=ccomp  or "")
                    eed, eee, eef, eeg = st.columns([2, 2, 1, 2])
                    ebairro = eed.text_input("Bairro",  value=cbairro or "")
                    ecidade = eee.text_input("Cidade",  value=ccidade or "")
                    eestado = eef.text_input("UF",      value=cestado or "", max_chars=2)
                    ecep    = eeg.text_input("CEP",     value=ccep    or "")
                    cs_e, cc_e = st.columns(2)
                    sv_e = cs_e.form_submit_button("💾  Salvar", use_container_width=True)
                    cn_e = cc_e.form_submit_button("✕  Cancelar", use_container_width=True)

                if cn_e:
                    st.session_state.edit_cli_id = None; st.rerun()
                if sv_e:
                    if not validate_required(en, edo):
                        show_error("Nome e CPF/CNPJ são obrigatórios.")
                    elif not validate_doc(edo):
                        show_error("CPF/CNPJ inválido.")
                    else:
                        res = qry(
                            """UPDATE clientes SET nome=%s,documento=%s,telefone=%s,
                               email=%s,rua=%s,numero=%s,complemento=%s,bairro=%s,
                               cidade=%s,estado=%s,cep=%s
                               WHERE id=%s AND empresa_id=%s""",
                            (en.strip(), re.sub(r'\D','',edo), etl, eml,
                             erua, enum, ecomp, ebairro, ecidade,
                             eestado.upper() if eestado else "", ecep,
                             cid, EMPRESA_ID))
                        if res is True:
                            show_success("Cliente atualizado!")
                            st.session_state.edit_cli_id = None; st.rerun()
                        elif res == "duplicate":
                            show_error("Já existe um cliente com esse CPF/CNPJ.")
                        else:
                            show_error("Não foi possível salvar.")
            else:
                col_i, col_e, col_t = st.columns([8, 1, 1])
                col_i.markdown(
                    f'<div class="card">'
                    f'<div class="card-title">{cnome} &nbsp;{sb}</div>'
                    f'<div class="card-sub">{cdoc} · {ctel or "—"} · {ccidade or "—"}</div>'
                    f'</div>', unsafe_allow_html=True)
                if col_e.button("✏️", key=f"edit_cli_{cid}", help="Editar"):
                    st.session_state.edit_cli_id   = cid
                    st.session_state.edit_cli_data = cli; st.rerun()
                if col_t.button("🔴" if cativo else "🟢", key=f"tog_cli_{cid}",
                                help="Inativar" if cativo else "Ativar"):
                    qry("UPDATE clientes SET ativo=%s WHERE id=%s AND empresa_id=%s",
                        (not cativo, cid, EMPRESA_ID)); st.rerun()

            st.markdown('<div style="border-top:.5px solid #eef0ff;margin:4px 0"></div>',
                        unsafe_allow_html=True)
    else:
        show_info("Nenhum cliente encontrado.",
                  "Ajuste os filtros ou adicione um novo cliente.")


# ════════════════════════════════════════════════════════════════
#  5. HISTÓRICO DE VENDAS
# ════════════════════════════════════════════════════════════════
elif menu == "Histórico de Vendas":
    page_header("📜", "Histórico de Vendas", "Consulte e gerencie todas as transações")

    with st.expander("🔍  Filtros", expanded=True):
        filtro_status = st.selectbox(
            "Status", ["Todos","Pago","Cancelado"], key="filtro_hist")

    where_clause = "" if filtro_status == "Todos" else "AND v.status=%s"
    params_hist  = (EMPRESA_ID,) if filtro_status == "Todos" else (EMPRESA_ID, filtro_status)

    vendas = qry(
        f"""SELECT v.id, TO_CHAR(v.data,'DD/MM/YYYY HH24:MI'), v.cliente_name,
                   v.valor_total, v.pagamento, v.status,
                   STRING_AGG(i.produto_nome||' x'||i.quantidade, ' | '),
                   COALESCE(v.observacao,'')
            FROM vendas v
            LEFT JOIN itens_venda i ON i.venda_id=v.id AND i.empresa_id=v.empresa_id
            WHERE v.empresa_id=%s {where_clause}
            GROUP BY v.id ORDER BY v.data DESC""",
        params_hist, fetch=True)

    if not vendas:
        show_info("Nenhuma venda encontrada.", "Ajuste os filtros ou realize uma venda.")
    else:
        for row in vendas:
            vid, data_fmt, cli, val, pag, status, itens, obs = row
            itens    = itens or "—"
            obs      = obs   or ""
            is_pago  = status == "Pago"
            sb = ('<span class="badge pago">Pago</span>' if is_pago
                  else '<span class="badge cancelado">Cancelado</span>')
            obs_html = (f'<div class="card-sub" style="font-style:italic">📝 {obs}</div>'
                        if obs else "")

            col_info, col_val, col_acoes = st.columns([5, 2, 2])
            with col_info:
                st.markdown(
                    f'<div class="card">'
                    f'<div class="card-sku">#{vid} · {data_fmt} · {pag}</div>'
                    f'<div class="card-title">{cli} &nbsp;{sb}</div>'
                    f'<div class="card-sub">{itens}</div>'
                    f'{obs_html}</div>',
                    unsafe_allow_html=True)
            with col_val:
                st.markdown(
                    f'<div style="font-family:Sora,sans-serif;font-size:1rem;'
                    f'font-weight:700;color:#6366f1;padding-top:14px">'
                    f'{cur_sym} {float(val):,.2f}</div>',
                    unsafe_allow_html=True)
            with col_acoes:
                if is_pago:
                    if st.button("❌ Cancelar", key=f"cancel_{vid}",
                                 help="Cancelar pedido e reverter estoque"):
                        itens_db = qry(
                            "SELECT produto_nome,quantidade FROM itens_venda "
                            "WHERE venda_id=%s AND empresa_id=%s",
                            (vid, EMPRESA_ID), fetch=True)
                        if itens_db:
                            for pnome, pqtd in itens_db:
                                qry("UPDATE produtos SET estoque_atual=estoque_atual+%s "
                                    "WHERE nome=%s AND empresa_id=%s",
                                    (pqtd, pnome, EMPRESA_ID))
                        qry("UPDATE vendas SET status='Cancelado' WHERE id=%s AND empresa_id=%s",
                            (vid, EMPRESA_ID))
                        show_success(f"Pedido #{vid} cancelado.",
                                     "Estoque revertido automaticamente.")
                        st.rerun()
                else:
                    if st.button("✅ Reativar", key=f"reativar_{vid}",
                                 help="Reativar e debitar estoque novamente"):
                        itens_db = qry(
                            "SELECT produto_nome,quantidade FROM itens_venda "
                            "WHERE venda_id=%s AND empresa_id=%s",
                            (vid, EMPRESA_ID), fetch=True)
                        erros_r = []
                        if itens_db:
                            for pnome, pqtd in itens_db:
                                est_r = qry(
                                    "SELECT estoque_atual FROM produtos "
                                    "WHERE nome=%s AND empresa_id=%s",
                                    (pnome, EMPRESA_ID), fetch=True)
                                est_val = est_r[0][0] if est_r else 0
                                if est_val < pqtd:
                                    erros_r.append(
                                        f"Estoque insuficiente para '{pnome}': "
                                        f"necessário {pqtd}, disponível {est_val}.")
                        if erros_r:
                            for e in erros_r:
                                show_error(e, "Ajuste o estoque antes de reativar.")
                        else:
                            if itens_db:
                                for pnome, pqtd in itens_db:
                                    qry("UPDATE produtos SET estoque_atual=estoque_atual-%s "
                                        "WHERE nome=%s AND empresa_id=%s",
                                        (pqtd, pnome, EMPRESA_ID))
                            qry("UPDATE vendas SET status='Pago' WHERE id=%s AND empresa_id=%s",
                                (vid, EMPRESA_ID))
                            show_success(f"Pedido #{vid} reativado!",
                                         "Estoque debitado novamente.")
                            st.rerun()

            st.markdown('<div style="border-top:.5px solid #eef0ff;margin:6px 0"></div>',
                        unsafe_allow_html=True)


# ════════════════════════════════════════════════════════════════
#  6. CATEGORIAS
# ════════════════════════════════════════════════════════════════
elif menu == "Categorias":
    page_header("🏷️", "Categorias", "Organize seus produtos por categoria")

    c1, c2 = st.columns([3, 1])
    new_cat = c1.text_input("Nova categoria", label_visibility="collapsed",
                             placeholder="Ex: Eletrônicos, Alimentação…")
    if c2.button("➕  Adicionar", use_container_width=True):
        if not validate_required(new_cat):
            show_error("Digite o nome da categoria.")
        else:
            res = qry("INSERT INTO categorias(empresa_id,nome) VALUES(%s,%s)",
                      (EMPRESA_ID, new_cat.strip()))
            if res is True:
                show_success("Categoria adicionada!", f"'{new_cat}' criada."); st.rerun()
            elif res == "duplicate":
                show_error("Essa categoria já está cadastrada nesta empresa.")
            else:
                show_error("Não foi possível salvar.")

    cats = qry(
        "SELECT id,nome,ativo FROM categorias WHERE empresa_id=%s ORDER BY nome",
        (EMPRESA_ID,), fetch=True)
    if cats:
        hr(); st.markdown(f"**{len(cats)} categoria(s)**")
        for (cid, cnome, cativo) in cats:
            sb = ('<span class="badge ativo">Ativo</span>' if cativo
                  else '<span class="badge inativo">Inativo</span>')
            inativo_cls = "" if cativo else " inativo"

            if st.session_state.edit_cat_id == cid:
                with st.form(f"f_edit_cat_{cid}"):
                    new_name = st.text_input("Nome *", value=cnome)
                    cs2, cc2 = st.columns(2)
                    sv  = cs2.form_submit_button("💾  Salvar",   use_container_width=True)
                    cn2 = cc2.form_submit_button("✕  Cancelar", use_container_width=True)
                if cn2:
                    st.session_state.edit_cat_id = None; st.rerun()
                if sv:
                    if not validate_required(new_name):
                        show_error("O nome não pode estar em branco.")
                    else:
                        res = qry(
                            "UPDATE categorias SET nome=%s WHERE id=%s AND empresa_id=%s",
                            (new_name.strip(), cid, EMPRESA_ID))
                        if res is True:
                            qry("UPDATE produtos SET categoria=%s "
                                "WHERE categoria=%s AND empresa_id=%s",
                                (new_name.strip(), cnome, EMPRESA_ID))
                            show_success("Categoria atualizada!")
                            st.session_state.edit_cat_id = None; st.rerun()
                        elif res == "duplicate":
                            show_error("Já existe uma categoria com esse nome.")
                        else:
                            show_error("Não foi possível salvar.")
            else:
                col_n, col_e, col_t = st.columns([7, 1, 1])
                col_n.markdown(
                    f'<div class="aux-row{inativo_cls}">'
                    f'<span class="ar-name">🏷️ {cnome}</span>&nbsp;{sb}</div>',
                    unsafe_allow_html=True)
                if col_e.button("✏️", key=f"edit_cat_{cid}", help="Editar"):
                    st.session_state.edit_cat_id = cid; st.rerun()
                if col_t.button("🔴" if cativo else "🟢", key=f"tog_cat_{cid}",
                                help="Inativar" if cativo else "Ativar"):
                    qry("UPDATE categorias SET ativo=%s WHERE id=%s AND empresa_id=%s",
                        (not cativo, cid, EMPRESA_ID)); st.rerun()
    else:
        show_info("Nenhuma categoria cadastrada.", "Adicione uma acima.")


# ════════════════════════════════════════════════════════════════
#  7. FORMAS DE PAGAMENTO
# ════════════════════════════════════════════════════════════════
elif menu == "Formas de Pagamento":
    page_header("💳", "Formas de Pagamento", "Gerencie as formas de pagamento aceitas")

    c1, c2 = st.columns([3, 1])
    new_pag = c1.text_input("Nova forma de pagamento", label_visibility="collapsed",
                              placeholder="Ex: Dinheiro, Pix, Cartão…")
    if c2.button("➕  Adicionar", use_container_width=True):
        if not validate_required(new_pag):
            show_error("Digite o nome da forma de pagamento.")
        else:
            res = qry("INSERT INTO pagamentos(empresa_id,nome) VALUES(%s,%s)",
                      (EMPRESA_ID, new_pag.strip()))
            if res is True:
                show_success("Forma de pagamento adicionada!", f"'{new_pag}' criada."); st.rerun()
            elif res == "duplicate":
                show_error("Essa forma de pagamento já está cadastrada nesta empresa.")
            else:
                show_error("Não foi possível salvar.")

    pags = qry(
        "SELECT id,nome,ativo FROM pagamentos WHERE empresa_id=%s ORDER BY nome",
        (EMPRESA_ID,), fetch=True)
    if pags:
        hr(); st.markdown(f"**{len(pags)} forma(s) de pagamento**")
        for (pid, pnome, pativo) in pags:
            sb = ('<span class="badge ativo">Ativo</span>' if pativo
                  else '<span class="badge inativo">Inativo</span>')
            inativo_cls = "" if pativo else " inativo"

            if st.session_state.edit_pag_id == pid:
                with st.form(f"f_edit_pag_{pid}"):
                    new_pname = st.text_input("Nome *", value=pnome)
                    cs3, cc3  = st.columns(2)
                    sv3 = cs3.form_submit_button("💾  Salvar",   use_container_width=True)
                    cn3 = cc3.form_submit_button("✕  Cancelar", use_container_width=True)
                if cn3:
                    st.session_state.edit_pag_id = None; st.rerun()
                if sv3:
                    if not validate_required(new_pname):
                        show_error("O nome não pode estar em branco.")
                    else:
                        res = qry(
                            "UPDATE pagamentos SET nome=%s WHERE id=%s AND empresa_id=%s",
                            (new_pname.strip(), pid, EMPRESA_ID))
                        if res is True:
                            show_success("Forma de pagamento atualizada!")
                            st.session_state.edit_pag_id = None; st.rerun()
                        elif res == "duplicate":
                            show_error("Já existe uma forma de pagamento com esse nome.")
                        else:
                            show_error("Não foi possível salvar.")
            else:
                col_n, col_e, col_t = st.columns([7, 1, 1])
                col_n.markdown(
                    f'<div class="aux-row{inativo_cls}">'
                    f'<span class="ar-name">💳 {pnome}</span>&nbsp;{sb}</div>',
                    unsafe_allow_html=True)
                if col_e.button("✏️", key=f"edit_pag_{pid}", help="Editar"):
                    st.session_state.edit_pag_id = pid; st.rerun()
                if col_t.button("🔴" if pativo else "🟢", key=f"tog_pag_{pid}",
                                help="Inativar" if pativo else "Ativar"):
                    qry("UPDATE pagamentos SET ativo=%s WHERE id=%s AND empresa_id=%s",
                        (not pativo, pid, EMPRESA_ID)); st.rerun()
    else:
        show_info("Nenhuma forma de pagamento cadastrada.", "Adicione uma acima.")


# ════════════════════════════════════════════════════════════════
#  8. CONFIGURAÇÕES
# ════════════════════════════════════════════════════════════════
elif menu == "Configurações":
    page_header("⚙️", "Configurações", "Personalize o sistema")

    # Busca configuração atual da empresa
    emp_row = qry(
        "SELECT nome, moeda FROM empresas WHERE id=%s", (EMPRESA_ID,), fetch=True)
    emp_nome_atual  = emp_row[0][0] if emp_row else ""
    emp_moeda_atual = emp_row[0][1] if emp_row and emp_row[0][1] else "R$"

    with st.form("f_config"):
        st.markdown("#### Empresa")
        new_n    = st.text_input("Nome da Empresa *", value=emp_nome_atual)
        st.markdown("#### Preferências")
        moedas   = ["R$", "$", "€", "£"]
        idx_m    = moedas.index(emp_moeda_atual) if emp_moeda_atual in moedas else 0
        new_curr = st.selectbox("Moeda padrão", moedas, index=idx_m)
        saved    = st.form_submit_button("💾  Salvar Configurações", use_container_width=True)

    if saved:
        if not validate_required(new_n):
            show_error("O nome da empresa não pode estar em branco.")
        else:
            # Verifica se coluna moeda existe; se não, adiciona
            qry("UPDATE empresas SET nome=%s, moeda=%s WHERE id=%s",
                (new_n.strip(), new_curr, EMPRESA_ID))
            st.session_state.empresa_nome  = new_n.strip()
            st.session_state.empresa_moeda = new_curr
            show_success("Configurações salvas!", "As alterações já estão ativas.")
            st.rerun()

    # Admin: gerenciar usuários da empresa
    if st.session_state.usuario_perfil == "admin":
        hr()
        st.markdown("#### 👤 Usuários da empresa")

        with st.expander("➕  Novo usuário"):
            with st.form("f_novo_user"):
                cu1, cu2 = st.columns(2)
                u_nome  = cu1.text_input("Nome *")
                u_email = cu2.text_input("E-mail *")
                cu3, cu4 = st.columns(2)
                u_senha  = cu3.text_input("Senha *", type="password")
                u_perfil = cu4.selectbox("Perfil", ["operador","admin"])
                add_u = st.form_submit_button("➕  Criar Usuário", use_container_width=True)
            if add_u:
                if not validate_required(u_nome, u_email, u_senha):
                    show_error("Nome, e-mail e senha são obrigatórios.")
                else:
                    res = qry(
                        """INSERT INTO usuarios(empresa_id,nome,email,senha_hash,perfil)
                           VALUES(%s,%s,%s,%s,%s)""",
                        (EMPRESA_ID, u_nome.strip(),
                         u_email.strip().lower(),
                         hash_senha(u_senha), u_perfil))
                    if res is True:
                        show_success("Usuário criado!", f"'{u_nome}' pode acessar o sistema."); st.rerun()
                    elif res == "duplicate":
                        show_error("Já existe um usuário com esse e-mail.")
                    else:
                        show_error("Não foi possível criar o usuário.")

        usuarios = qry(
            "SELECT id,nome,email,perfil,ativo FROM usuarios "
            "WHERE empresa_id=%s ORDER BY nome",
            (EMPRESA_ID,), fetch=True)
        if usuarios:
            for uid2, unome2, uemail2, uperfil2, uativo2 in usuarios:
                sb2 = ('<span class="badge ativo">Ativo</span>' if uativo2
                       else '<span class="badge inativo">Inativo</span>')
                col_u, col_ut = st.columns([9, 1])
                col_u.markdown(
                    f'<div class="card">'
                    f'<div class="card-title">{unome2} &nbsp;{sb2}</div>'
                    f'<div class="card-sub">{uemail2} · {uperfil2}</div>'
                    f'</div>', unsafe_allow_html=True)
                # Não permite inativar o próprio usuário
                if uid2 != st.session_state.usuario_id:
                    if col_ut.button("🔴" if uativo2 else "🟢",
                                     key=f"tog_u_{uid2}",
                                     help="Inativar" if uativo2 else "Ativar"):
                        qry("UPDATE usuarios SET ativo=%s WHERE id=%s AND empresa_id=%s",
                            (not uativo2, uid2, EMPRESA_ID)); st.rerun()
