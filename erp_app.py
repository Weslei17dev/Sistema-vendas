# ╔══════════════════════════════════════════════════════════════╗
#  ERP SaaS Multi-tenant  |  PostgreSQL  |  v8.0
#  Melhorias: lucratividade, DRE, metas, contas a receber,
#  despesas, orçamentos, PDF pedido, tabela de preços,
#  histórico cliente, entrada NF, movimentação estoque,
#  variações produto, fornecedores, grupos clientes,
#  regiões representante, comissões, log ações, permissões,
#  recuperação senha, gráficos dashboard, busca global,
#  exportação Excel em todos cadastros, importação CSV,
#  modo escuro, notificações internas.
# ╚══════════════════════════════════════════════════════════════╝

import streamlit as st
import psycopg2
from psycopg2 import pool as pg_pool, errors as pg_errors
import hashlib, re, io, csv, json
from datetime import datetime, date, timedelta
from contextlib import contextmanager

try:
    import pandas as pd
    import openpyxl
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

st.set_page_config(page_title="Gestão ERP Pro", layout="wide",
                   initial_sidebar_state="expanded",
                   menu_items={"About": "ERP SaaS Multi-tenant v8.0"})

# ── Tema claro/escuro ──
DARK = st.session_state.get("dark_mode", False)
BG = "#0f0f1a" if DARK else "#ffffff"
CARD_BG = "#1a1a2e" if DARK else "#fafbff"
CARD_BORDER = "#2d2d4e" if DARK else "#eef0ff"
TEXT_MAIN = "#e8e8f0" if DARK else "#1e1b4b"
TEXT_SUB = "#9999bb" if DARK else "#6b7280"
METRIC_BG = "#1e1e35" if DARK else "#f8f9ff"
FORM_BG = "#16162a" if DARK else "#fafbff"

st.markdown(f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600&family=Sora:wght@400;600;700&display=swap');
html,body,[class*="css"]{{font-family:'DM Sans',sans-serif;background:{BG};}}
section[data-testid="stSidebar"]{{background:linear-gradient(160deg,#0f0f1a 0%,#1a1a2e 60%,#16213e 100%);border-right:1px solid rgba(255,255,255,0.06);}}
section[data-testid="stSidebar"] *{{color:rgba(255,255,255,0.85)!important;}}
.erp-brand{{font-family:'Sora',sans-serif;font-size:1.1rem;font-weight:700;color:#fff!important;padding:.2rem 0 .9rem;}}
.erp-brand span{{color:#818cf8!important;}}
.erp-tenant{{font-size:.72rem;color:rgba(255,255,255,0.4)!important;padding-bottom:.5rem;}}
.nav-section-title{{font-size:.62rem;font-weight:600;letter-spacing:.12em;text-transform:uppercase;color:rgba(255,255,255,0.3)!important;padding:.7rem 0 .3rem;}}
.stButton>button{{width:100%;text-align:left;padding:.52rem .85rem;border-radius:10px;border:none;background:transparent;color:rgba(255,255,255,0.75)!important;font-size:.88rem;transition:all .18s ease;margin-bottom:2px;}}
.stButton>button:hover{{background:rgba(129,140,248,.13)!important;color:#fff!important;transform:translateX(2px);}}
.nav-active button{{background:rgba(99,102,241,.28)!important;color:#c7d2fe!important;font-weight:600;border-left:3px solid #818cf8;}}
.logout-btn button{{background:rgba(239,68,68,.12)!important;color:#fca5a5!important;border-radius:8px;}}
[data-testid="stMetric"]{{background:{METRIC_BG};border:1px solid {CARD_BORDER};border-radius:14px;padding:1rem 1.1rem;}}
[data-testid="stMetricValue"]{{font-family:'Sora',sans-serif;font-size:1.5rem;color:{TEXT_MAIN};}}
[data-testid="stMetricLabel"]{{color:{TEXT_SUB};}}
[data-testid="stForm"]{{background:{FORM_BG};border:1px solid {CARD_BORDER};border-radius:14px;padding:1.25rem;}}
[data-testid="stExpander"]{{border:1px solid {CARD_BORDER}!important;border-radius:12px!important;}}
.stTextInput input,.stSelectbox div,.stNumberInput input,.stTextArea textarea{{background:{CARD_BG}!important;color:{TEXT_MAIN}!important;border-color:{CARD_BORDER}!important;}}
.erp-toast{{padding:.7rem .95rem;border-radius:10px;font-size:.88rem;margin:.35rem 0;display:flex;align-items:flex-start;gap:.55rem;}}
.erp-toast.success{{background:#f0fdf4;border-left:4px solid #22c55e;color:#15803d;}}
.erp-toast.error{{background:#fef2f2;border-left:4px solid #ef4444;color:#b91c1c;}}
.erp-toast.warning{{background:#fffbeb;border-left:4px solid #f59e0b;color:#b45309;}}
.erp-toast.info{{background:#eff6ff;border-left:4px solid #3b82f6;color:#1d4ed8;}}
.erp-toast .icon{{font-size:1rem;flex-shrink:0;margin-top:2px;}}
.erp-toast .body strong{{display:block;font-weight:600;margin-bottom:1px;}}
.erp-toast .body span{{font-weight:400;color:inherit;opacity:.85;}}
.page-header{{display:flex;align-items:center;gap:.7rem;margin-bottom:1.4rem;}}
.page-header .icon{{width:38px;height:38px;border-radius:10px;display:flex;align-items:center;justify-content:center;font-size:1.1rem;background:linear-gradient(135deg,#6366f1,#818cf8);flex-shrink:0;}}
.page-header h1{{font-family:'Sora',sans-serif;font-size:1.3rem;font-weight:700;margin:0;color:{TEXT_MAIN};}}
.page-header p{{font-size:.82rem;color:{TEXT_SUB};margin:0;}}
hr.erp{{border:none;border-top:1px solid {CARD_BORDER};margin:1.1rem 0;}}
.badge{{display:inline-block;padding:2px 8px;border-radius:99px;font-size:.72rem;font-weight:600;}}
.badge.ok{{background:#dcfce7;color:#166534;}}.badge.low{{background:#fef9c3;color:#854d0e;}}
.badge.zero{{background:#fee2e2;color:#991b1b;}}.badge.ativo{{background:#dcfce7;color:#166534;}}
.badge.inativo{{background:#f1f5f9;color:#64748b;}}.badge.pago{{background:#dbeafe;color:#1e40af;}}
.badge.cancelado{{background:#fee2e2;color:#991b1b;}}.badge.orcamento{{background:#fef3c7;color:#92400e;}}
.badge.pendente{{background:#fff7ed;color:#c2410c;}}
.card{{background:{CARD_BG};border:1px solid {CARD_BORDER};border-radius:12px;padding:.75rem .95rem;margin-bottom:7px;transition:border-color .15s;}}
.card:hover{{border-color:#c7d2fe;}}.card.inativo{{opacity:.52;border-style:dashed;}}
.card-title{{font-size:.92rem;font-weight:600;color:{TEXT_MAIN};margin:0 0 2px;}}
.card-sub{{font-size:.76rem;color:{TEXT_SUB};}}.card-val{{font-family:'Sora',sans-serif;font-size:1rem;font-weight:700;color:#6366f1;}}
.card-sku{{font-size:.68rem;color:#9ca3af;}}
.cart-item{{display:flex;align-items:center;justify-content:space-between;padding:.5rem .7rem;border-radius:9px;margin-bottom:5px;background:{CARD_BG};border:1px solid {CARD_BORDER};}}
.ci-name{{font-size:.85rem;font-weight:600;color:{TEXT_MAIN};}}.ci-qty{{font-size:.75rem;color:{TEXT_SUB};}}
.ci-val{{font-family:'Sora',sans-serif;font-size:.88rem;font-weight:700;color:#6366f1;}}
.cart-total{{display:flex;justify-content:space-between;align-items:center;padding:.7rem .9rem;border-radius:10px;background:#6366f1;color:#fff;margin-top:.7rem;}}
.cart-total span{{font-size:.85rem;opacity:.85;}}.cart-total strong{{font-family:'Sora',sans-serif;font-size:1.15rem;}}
.cart-desc{{background:#f0f9ff;border:1px solid #bae6fd;border-radius:9px;padding:.5rem .7rem;margin-top:5px;font-size:.82rem;color:#0369a1;}}
.aux-row{{display:flex;align-items:center;justify-content:space-between;padding:.55rem .85rem;border-radius:9px;margin-bottom:5px;background:{CARD_BG};border:1px solid {CARD_BORDER};}}
.aux-row.inativo{{opacity:.5;border-style:dashed;}}.aux-row .ar-name{{font-size:.88rem;font-weight:500;color:{TEXT_MAIN};}}
.login-wrap{{max-width:420px;margin:4rem auto 0;padding:2rem;background:{CARD_BG};border:1px solid {CARD_BORDER};border-radius:18px;}}
.login-logo{{text-align:center;font-family:'Sora',sans-serif;font-size:1.5rem;font-weight:700;color:{TEXT_MAIN};margin-bottom:.25rem;}}
.login-logo span{{color:#6366f1;}}.login-sub{{text-align:center;font-size:.83rem;color:{TEXT_SUB};margin-bottom:1.5rem;}}
.notif-badge{{background:#ef4444;color:#fff;border-radius:99px;font-size:.65rem;font-weight:700;padding:1px 6px;margin-left:4px;}}
.dre-row{{display:flex;justify-content:space-between;padding:.45rem .8rem;border-radius:8px;margin-bottom:3px;}}
.dre-row.receita{{background:#f0fdf4;}}.dre-row.desconto{{background:#fffbeb;}}
.dre-row.cmv{{background:#fff7ed;}}.dre-row.lucro{{background:#eff6ff;font-weight:700;}}
.dre-row.despesa{{background:#fef2f2;}}.dre-row.total{{background:#f5f3ff;font-weight:700;font-size:1rem;}}
.meta-bar-wrap{{background:#eef0ff;border-radius:99px;height:10px;margin:.3rem 0;}}
.meta-bar{{background:linear-gradient(90deg,#6366f1,#818cf8);border-radius:99px;height:10px;transition:width .5s ease;}}
.notif-item{{padding:.6rem .9rem;border-radius:10px;margin-bottom:6px;border-left:4px solid;}}
.notif-item.estoque{{background:#fffbeb;border-color:#f59e0b;}}
.notif-item.meta{{background:#f0fdf4;border-color:#22c55e;}}
.notif-item.receber{{background:#eff6ff;border-color:#3b82f6;}}
.search-global{{background:{CARD_BG};border:2px solid {CARD_BORDER};border-radius:14px;padding:.65rem 1rem;font-size:.95rem;width:100%;color:{TEXT_MAIN};outline:none;transition:border-color .2s;}}
.search-global:focus{{border-color:#818cf8;}}
.search-result{{padding:.5rem .8rem;border-radius:9px;margin-bottom:4px;background:{CARD_BG};border:1px solid {CARD_BORDER};display:flex;align-items:center;gap:.6rem;cursor:pointer;}}
.search-result:hover{{border-color:#c7d2fe;}}
.sr-type{{font-size:.68rem;font-weight:600;color:#818cf8;background:rgba(129,140,248,.12);padding:2px 7px;border-radius:6px;}}
.sr-name{{font-size:.88rem;font-weight:500;color:{TEXT_MAIN};}}
.sr-sub{{font-size:.73rem;color:{TEXT_SUB};}}
@media(max-width:640px){{.page-header h1{{font-size:1.1rem;}}[data-testid="stMetricValue"]{{font-size:1.25rem;}}.card{{padding:.65rem .8rem;}}.cart-total strong{{font-size:1rem;}}}}
</style>
""", unsafe_allow_html=True)

# ── UI helpers ──
def show_error(t, h=""): st.markdown(f'<div class="erp-toast error"><div class="icon">✕</div><div class="body"><strong>{t}</strong><span>{h}</span></div></div>', unsafe_allow_html=True)
def show_success(t, h=""): st.markdown(f'<div class="erp-toast success"><div class="icon">✓</div><div class="body"><strong>{t}</strong><span>{h}</span></div></div>', unsafe_allow_html=True)
def show_warning(t, h=""): st.markdown(f'<div class="erp-toast warning"><div class="icon">⚠</div><div class="body"><strong>{t}</strong><span>{h}</span></div></div>', unsafe_allow_html=True)
def show_info(t, h=""): st.markdown(f'<div class="erp-toast info"><div class="icon">ℹ</div><div class="body"><strong>{t}</strong><span>{h}</span></div></div>', unsafe_allow_html=True)
def page_header(icon, title, subtitle=""):
    sub = f"<p>{subtitle}</p>" if subtitle else ""
    st.markdown(f'<div class="page-header"><div class="icon">{icon}</div><div><h1>{title}</h1>{sub}</div></div>', unsafe_allow_html=True)
def hr(): st.markdown('<hr class="erp">', unsafe_allow_html=True)

# ── Pool de conexões (INALTERADO) ──
@st.cache_resource
def get_pool():
    db = st.secrets["db"]
    dsn = (f"postgresql://{db['user']}:{db['password']}"
           f"@{db['host']}:{db.get('port', 5432)}/{db['dbname']}"
           f"?sslmode=require&channel_binding=disable")
    return pg_pool.ThreadedConnectionPool(1, 5, dsn, connect_timeout=10)

@contextmanager
def get_conn():
    pool = get_pool(); conn = pool.getconn()
    try: yield conn
    except Exception: conn.rollback(); raise
    finally: pool.putconn(conn)

def run_query(query, params=(), fetch=False, returning=False):
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                if fetch:    r = cur.fetchall(); conn.commit(); return r
                if returning: r = cur.fetchone(); conn.commit(); return r
                conn.commit(); return True
    except pg_errors.UniqueViolation: return "duplicate"
    except Exception as e: st.session_state["_dberr"] = str(e); return False

# ── Init DB ──
def init_db():
    stmts = [
        """CREATE TABLE IF NOT EXISTS empresas(id SERIAL PRIMARY KEY,nome TEXT NOT NULL,plano TEXT DEFAULT 'basico',ativo BOOLEAN DEFAULT TRUE,moeda TEXT DEFAULT 'R$',criado_em TIMESTAMP DEFAULT NOW())""",
        """CREATE TABLE IF NOT EXISTS usuarios(id SERIAL PRIMARY KEY,empresa_id INTEGER NOT NULL REFERENCES empresas(id),nome TEXT NOT NULL,email TEXT UNIQUE NOT NULL,senha_hash TEXT NOT NULL,perfil TEXT DEFAULT 'operador',ativo BOOLEAN DEFAULT TRUE,criado_em TIMESTAMP DEFAULT NOW())""",
        """CREATE TABLE IF NOT EXISTS supervisores(id SERIAL PRIMARY KEY,empresa_id INTEGER NOT NULL REFERENCES empresas(id),nome TEXT NOT NULL,email TEXT,telefone TEXT,comissao_pct NUMERIC(5,2) DEFAULT 0,ativo BOOLEAN DEFAULT TRUE,UNIQUE(empresa_id,nome))""",
        """CREATE TABLE IF NOT EXISTS representantes(id SERIAL PRIMARY KEY,empresa_id INTEGER NOT NULL REFERENCES empresas(id),nome TEXT NOT NULL,email TEXT,telefone TEXT,supervisor_id INTEGER REFERENCES supervisores(id),comissao_pct NUMERIC(5,2) DEFAULT 0,regiao TEXT,ativo BOOLEAN DEFAULT TRUE,UNIQUE(empresa_id,nome))""",
        """CREATE TABLE IF NOT EXISTS categorias(id SERIAL PRIMARY KEY,empresa_id INTEGER NOT NULL REFERENCES empresas(id),nome TEXT NOT NULL,ativo BOOLEAN DEFAULT TRUE,UNIQUE(empresa_id,nome))""",
        """CREATE TABLE IF NOT EXISTS pagamentos(id SERIAL PRIMARY KEY,empresa_id INTEGER NOT NULL REFERENCES empresas(id),nome TEXT NOT NULL,ativo BOOLEAN DEFAULT TRUE,UNIQUE(empresa_id,nome))""",
        """CREATE TABLE IF NOT EXISTS grupos_clientes(id SERIAL PRIMARY KEY,empresa_id INTEGER NOT NULL REFERENCES empresas(id),nome TEXT NOT NULL,desconto_padrao NUMERIC(5,2) DEFAULT 0,ativo BOOLEAN DEFAULT TRUE,UNIQUE(empresa_id,nome))""",
        """CREATE TABLE IF NOT EXISTS clientes(id SERIAL PRIMARY KEY,empresa_id INTEGER NOT NULL REFERENCES empresas(id),nome TEXT NOT NULL,documento TEXT NOT NULL,telefone TEXT,email TEXT,rua TEXT,numero TEXT,complemento TEXT,bairro TEXT,cidade TEXT,estado TEXT,cep TEXT,grupo_id INTEGER REFERENCES grupos_clientes(id),ativo BOOLEAN DEFAULT TRUE,UNIQUE(empresa_id,documento))""",
        """CREATE TABLE IF NOT EXISTS fornecedores(id SERIAL PRIMARY KEY,empresa_id INTEGER NOT NULL REFERENCES empresas(id),nome TEXT NOT NULL,documento TEXT,telefone TEXT,email TEXT,contato TEXT,ativo BOOLEAN DEFAULT TRUE,UNIQUE(empresa_id,nome))""",
        """CREATE TABLE IF NOT EXISTS produtos(id SERIAL PRIMARY KEY,empresa_id INTEGER NOT NULL REFERENCES empresas(id),sku TEXT NOT NULL,nome TEXT NOT NULL,categoria TEXT NOT NULL,preco_custo NUMERIC(12,2) DEFAULT 0,preco_venda NUMERIC(12,2) DEFAULT 0,estoque_atual INTEGER DEFAULT 0,estoque_minimo INTEGER DEFAULT 2,codigo_barras TEXT,ativo BOOLEAN DEFAULT TRUE,UNIQUE(empresa_id,sku))""",
        """CREATE TABLE IF NOT EXISTS variacoes_produto(id SERIAL PRIMARY KEY,empresa_id INTEGER NOT NULL REFERENCES empresas(id),produto_id INTEGER NOT NULL REFERENCES produtos(id),atributo TEXT NOT NULL,valor TEXT NOT NULL,estoque_adicional INTEGER DEFAULT 0)""",
        """CREATE TABLE IF NOT EXISTS movimentacoes_estoque(id SERIAL PRIMARY KEY,empresa_id INTEGER NOT NULL REFERENCES empresas(id),produto_id INTEGER NOT NULL REFERENCES produtos(id),tipo TEXT NOT NULL,quantidade INTEGER NOT NULL,motivo TEXT,usuario_nome TEXT,criado_em TIMESTAMP DEFAULT NOW())""",
        """CREATE TABLE IF NOT EXISTS tabelas_preco(id SERIAL PRIMARY KEY,empresa_id INTEGER NOT NULL REFERENCES empresas(id),nome TEXT NOT NULL,desconto_pct NUMERIC(5,2) DEFAULT 0,ativo BOOLEAN DEFAULT TRUE,UNIQUE(empresa_id,nome))""",
        """CREATE TABLE IF NOT EXISTS vendas(id SERIAL PRIMARY KEY,empresa_id INTEGER NOT NULL REFERENCES empresas(id),data TIMESTAMP NOT NULL DEFAULT NOW(),cliente_name TEXT,valor_bruto NUMERIC(12,2) DEFAULT 0,desconto_pct NUMERIC(5,2) DEFAULT 0,desconto_val NUMERIC(12,2) DEFAULT 0,valor_total NUMERIC(12,2),pagamento TEXT,status TEXT DEFAULT 'Pago',observacao TEXT DEFAULT '',supervisor_id INTEGER REFERENCES supervisores(id),representante_id INTEGER REFERENCES representantes(id),tipo TEXT DEFAULT 'Venda',vencimento DATE,comissao_supervisor NUMERIC(12,2) DEFAULT 0,comissao_representante NUMERIC(12,2) DEFAULT 0,comissao_status TEXT DEFAULT 'Pendente')""",
        """CREATE TABLE IF NOT EXISTS itens_venda(id SERIAL PRIMARY KEY,empresa_id INTEGER NOT NULL REFERENCES empresas(id),venda_id INTEGER NOT NULL REFERENCES vendas(id),produto_nome TEXT,quantidade INTEGER,preco_unit NUMERIC(12,2))""",
        """CREATE TABLE IF NOT EXISTS despesas(id SERIAL PRIMARY KEY,empresa_id INTEGER NOT NULL REFERENCES empresas(id),descricao TEXT NOT NULL,categoria TEXT,valor NUMERIC(12,2) NOT NULL,data DATE NOT NULL DEFAULT CURRENT_DATE,status TEXT DEFAULT 'Pago',observacao TEXT)""",
        """CREATE TABLE IF NOT EXISTS contas_receber(id SERIAL PRIMARY KEY,empresa_id INTEGER NOT NULL REFERENCES empresas(id),venda_id INTEGER REFERENCES vendas(id),cliente_name TEXT,descricao TEXT,valor NUMERIC(12,2) NOT NULL,vencimento DATE NOT NULL,status TEXT DEFAULT 'Pendente',data_pagamento DATE)""",
        """CREATE TABLE IF NOT EXISTS metas(id SERIAL PRIMARY KEY,empresa_id INTEGER NOT NULL REFERENCES empresas(id),descricao TEXT NOT NULL,valor_meta NUMERIC(12,2) NOT NULL,periodo_inicio DATE,periodo_fim DATE,tipo TEXT DEFAULT 'Faturamento',ativo BOOLEAN DEFAULT TRUE)""",
        """CREATE TABLE IF NOT EXISTS entradas_nf(id SERIAL PRIMARY KEY,empresa_id INTEGER NOT NULL REFERENCES empresas(id),numero_nf TEXT,fornecedor_nome TEXT,data DATE NOT NULL DEFAULT CURRENT_DATE,valor_total NUMERIC(12,2) DEFAULT 0,observacao TEXT,criado_em TIMESTAMP DEFAULT NOW())""",
        """CREATE TABLE IF NOT EXISTS itens_entrada_nf(id SERIAL PRIMARY KEY,empresa_id INTEGER NOT NULL REFERENCES empresas(id),entrada_id INTEGER NOT NULL REFERENCES entradas_nf(id),produto_id INTEGER REFERENCES produtos(id),produto_nome TEXT,quantidade INTEGER,preco_custo NUMERIC(12,2))""",
        """CREATE TABLE IF NOT EXISTS log_acoes(id SERIAL PRIMARY KEY,empresa_id INTEGER NOT NULL,usuario_nome TEXT,acao TEXT NOT NULL,detalhes TEXT,criado_em TIMESTAMP DEFAULT NOW())""",
        """CREATE TABLE IF NOT EXISTS notificacoes(id SERIAL PRIMARY KEY,empresa_id INTEGER NOT NULL,titulo TEXT NOT NULL,mensagem TEXT,tipo TEXT DEFAULT 'info',lida BOOLEAN DEFAULT FALSE,criado_em TIMESTAMP DEFAULT NOW())""",
        """CREATE TABLE IF NOT EXISTS recuperacao_senha(id SERIAL PRIMARY KEY,email TEXT NOT NULL,token TEXT NOT NULL,expira_em TIMESTAMP NOT NULL,usado BOOLEAN DEFAULT FALSE)""",
    ]
    migracoes = [
        ("vendas", "valor_bruto", "NUMERIC(12,2) DEFAULT 0"),
        ("vendas", "desconto_pct", "NUMERIC(5,2) DEFAULT 0"),
        ("vendas", "desconto_val", "NUMERIC(12,2) DEFAULT 0"),
        ("vendas", "supervisor_id", "INTEGER"),
        ("vendas", "representante_id", "INTEGER"),
        ("vendas", "tipo", "TEXT DEFAULT 'Venda'"),
        ("vendas", "vencimento", "DATE"),
        ("vendas", "comissao_supervisor", "NUMERIC(12,2) DEFAULT 0"),
        ("vendas", "comissao_representante", "NUMERIC(12,2) DEFAULT 0"),
        ("vendas", "comissao_status", "TEXT DEFAULT 'Pendente'"),
        ("empresas", "moeda", "TEXT DEFAULT 'R$'"),
        ("supervisores", "comissao_pct", "NUMERIC(5,2) DEFAULT 0"),
        ("representantes", "comissao_pct", "NUMERIC(5,2) DEFAULT 0"),
        ("representantes", "regiao", "TEXT"),
        ("clientes", "grupo_id", "INTEGER"),
        ("produtos", "codigo_barras", "TEXT"),
    ]
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                for s in stmts: cur.execute(s)
                for tbl, col, tipo in migracoes:
                    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name=%s AND column_name=%s", (tbl, col))
                    if not cur.fetchone(): cur.execute(f"ALTER TABLE {tbl} ADD COLUMN {col} {tipo}")
            conn.commit()
    except Exception as e: st.error(f"Erro ao inicializar banco: {e}"); st.stop()

# ── Helpers ──
def eid():
    v = st.session_state.get("empresa_id")
    if not v: st.error("Sessão inválida."); st.stop()
    return v

def qry(sql, params=(), fetch=False, returning=False): return run_query(sql, params, fetch=fetch, returning=returning)
def validate_doc(d): return len(re.sub(r'\D', '', d)) in (11, 14)
def validate_required(*f): return all(x is not None and str(x).strip() for x in f)
def hash_senha(s): return hashlib.sha256(s.encode()).hexdigest()

def get_estoque(pid):
    r = qry("SELECT estoque_atual FROM produtos WHERE id=%s AND empresa_id=%s", (pid, eid()), fetch=True)
    return r[0][0] if r else 0

def to_excel(rows, columns):
    if not HAS_PANDAS: return None
    df = pd.DataFrame(rows, columns=columns)
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w: df.to_excel(w, index=False, sheet_name="Dados")
    return buf.getvalue()

def log_acao(acao, detalhes=""):
    qry("INSERT INTO log_acoes(empresa_id,usuario_nome,acao,detalhes) VALUES(%s,%s,%s,%s)",
        (eid(), st.session_state.get("usuario_nome", "?"), acao, detalhes))

def criar_notif(titulo, mensagem, tipo="info"):
    qry("INSERT INTO notificacoes(empresa_id,titulo,mensagem,tipo) VALUES(%s,%s,%s,%s)",
        (eid(), titulo, mensagem, tipo))

def pode(permissao):
    """Verifica permissão: admin pode tudo, operador tem restrições"""
    perfil = st.session_state.get("usuario_perfil", "operador")
    if perfil == "admin": return True
    restritos = ["cfg", "log", "dre", "metas_edit", "despesas_del", "comissoes_pagar"]
    return permissao not in restritos

# ── Session state ──
_D = {
    "logado": False, "usuario_id": None, "empresa_id": None,
    "usuario_nome": "", "empresa_nome": "", "empresa_moeda": "R$",
    "usuario_perfil": "operador", "active_menu": "Dashboard",
    "cart": [], "editing_prod_id": None, "editing_prod_data": None,
    "adj_prod": None, "edit_cat_id": None, "edit_pag_id": None,
    "edit_cli_id": None, "edit_cli_data": None, "edit_sup_id": None,
    "edit_rep_id": None, "dark_mode": False, "busca_global": "",
    "edit_forn_id": None, "edit_grupo_id": None,
}
for k, v in _D.items():
    if k not in st.session_state: st.session_state[k] = v

# ── Login ──
def tela_login():
    st.markdown('<div class="login-wrap"><div class="login-logo">Gestão <span>ERP Pro</span></div><div class="login-sub">Acesse sua conta para continuar</div></div>', unsafe_allow_html=True)
    _, col, _ = st.columns([1, 2, 1])
    with col:
        tab_login, tab_reset = st.tabs(["Entrar", "Esqueci minha senha"])
        with tab_login:
            with st.form("form_login"):
                email = st.text_input("E-mail", placeholder="seu@email.com")
                senha = st.text_input("Senha", type="password", placeholder="••••••••")
                entrar = st.form_submit_button("Entrar", use_container_width=True)
            if entrar:
                if not validate_required(email, senha): show_error("Preencha e-mail e senha."); return
                row = run_query(
                    "SELECT u.id,u.nome,u.perfil,u.ativo,e.id,e.nome,e.ativo,COALESCE(e.moeda,'R$') FROM usuarios u JOIN empresas e ON e.id=u.empresa_id WHERE u.email=%s AND u.senha_hash=%s",
                    (email.strip().lower(), hash_senha(senha)), fetch=True)
                if not row: show_error("E-mail ou senha incorretos."); return
                uid, unome, uperfil, uativo, empid, empnome, empativo, moeda = row[0]
                if not uativo: show_error("Usuário inativo."); return
                if not empativo: show_error("Empresa inativa."); return
                st.session_state.update({"logado": True, "usuario_id": uid, "empresa_id": empid,
                                         "usuario_nome": unome, "empresa_nome": empnome,
                                         "empresa_moeda": moeda, "usuario_perfil": uperfil})
                st.rerun()

        with tab_reset:
            st.markdown("<p style='font-size:.83rem;color:#6b7280'>Informe seu e-mail e um token temporário será gerado. Leve ao administrador para redefinir a senha.</p>", unsafe_allow_html=True)
            with st.form("form_reset"):
                email_r = st.text_input("E-mail cadastrado")
                gerar = st.form_submit_button("Gerar token de recuperação", use_container_width=True)
            if gerar:
                usr = run_query("SELECT id FROM usuarios WHERE email=%s AND ativo=TRUE", (email_r.strip().lower(),), fetch=True)
                if usr:
                    import secrets
                    tok = secrets.token_hex(8)
                    expira = datetime.now() + timedelta(hours=2)
                    run_query("INSERT INTO recuperacao_senha(email,token,expira_em) VALUES(%s,%s,%s)", (email_r.strip().lower(), tok, expira))
                    show_success(f"Token gerado: {tok}", "Válido por 2h. Informe ao administrador para redefinir via Configurações.")
                else:
                    show_error("E-mail não encontrado.")

init_db()
if not st.session_state.logado: tela_login(); st.stop()

EMPRESA_ID = eid()
cur_sym = st.session_state.empresa_moeda

# ── Contagem notificações não lidas ──
notif_count_r = qry("SELECT COUNT(*) FROM notificacoes WHERE empresa_id=%s AND lida=FALSE", (EMPRESA_ID,), fetch=True)
NOTIF_COUNT = notif_count_r[0][0] if notif_count_r else 0

# ── Verificar alertas automáticos ──
def verificar_alertas():
    low = qry("SELECT nome FROM produtos WHERE empresa_id=%s AND ativo=TRUE AND estoque_atual<=estoque_minimo", (EMPRESA_ID,), fetch=True)
    if low:
        ex = qry("SELECT COUNT(*) FROM notificacoes WHERE empresa_id=%s AND tipo='estoque' AND lida=FALSE", (EMPRESA_ID,), fetch=True)
        if ex and ex[0][0] == 0:
            criar_notif(f"{len(low)} produto(s) com estoque baixo", ", ".join(p[0] for p in low[:5]), "estoque")
    venc = qry("SELECT COUNT(*) FROM contas_receber WHERE empresa_id=%s AND status='Pendente' AND vencimento<=CURRENT_DATE", (EMPRESA_ID,), fetch=True)
    if venc and venc[0][0] > 0:
        ex2 = qry("SELECT COUNT(*) FROM notificacoes WHERE empresa_id=%s AND tipo='receber' AND lida=FALSE", (EMPRESA_ID,), fetch=True)
        if ex2 and ex2[0][0] == 0:
            criar_notif(f"{venc[0][0]} conta(s) a receber vencida(s)", "Verifique Contas a Receber.", "receber")

verificar_alertas()

# ── Sidebar ──
MENUS = [
    ("📊", "Dashboard", "dash"),
    ("🔍", "Busca Global", "busca"),
    ("🛒", "Pedidos", "pedidos"),
    ("📦", "Estoque", "estoque"),
    ("📥", "Entrada NF", "entrada_nf"),
    ("👥", "Clientes", "clientes"),
    ("🏭", "Fornecedores", "fornecedores"),
    ("📜", "Histórico de Vendas", "hist"),
    ("📋", "Orçamentos", "orcamentos"),
    ("💰", "Contas a Receber", "receber"),
    ("💸", "Despesas", "despesas"),
    ("🎯", "Metas", "metas"),
    ("📈", "Relatórios", "relatorios"),
    ("🏆", "Comissões", "comissoes"),
    ("🔔", "Notificações", "notifs"),
    ("👔", "Supervisores", "sups"),
    ("🤝", "Representantes", "reps"),
    ("👥", "Grupos de Clientes", "grupos"),
    ("🏷️", "Categorias", "cats"),
    ("💳", "Formas de Pagamento", "pags"),
    ("📋", "Log de Ações", "log"),
    ("⚙️", "Configurações", "cfg"),
]

with st.sidebar:
    st.markdown(f'<div class="erp-brand">Gestão <span>ERP Pro</span></div><div class="erp-tenant">🏢 {st.session_state.empresa_nome}</div>', unsafe_allow_html=True)

    # Modo escuro toggle
    dm_col1, dm_col2 = st.columns([3, 1])
    with dm_col2:
        if st.button("🌙" if not DARK else "☀️", key="dark_toggle", help="Alternar tema"):
            st.session_state.dark_mode = not st.session_state.dark_mode; st.rerun()

    st.markdown('<div class="nav-section-title">Menu principal</div>', unsafe_allow_html=True)

    menu_sections = {
        "pedidos": None, "entrada_nf": None,
        "sups": "Cadastros",
        "grupos": None,
        "cats": "Tabelas auxiliares",
        "log": "Sistema",
    }

    for icon, label, key in MENUS:
        sec = menu_sections.get(key)
        if sec: st.markdown(f'<div class="nav-section-title">{sec}</div>', unsafe_allow_html=True)

        is_active = st.session_state.active_menu == label
        nb = f'<span class="notif-badge">{NOTIF_COUNT}</span>' if key == "notifs" and NOTIF_COUNT > 0 else ""
        with st.container():
            if is_active: st.markdown('<div class="nav-active">', unsafe_allow_html=True)
            if st.button(f"{icon}  {label}", key=f"nav_{key}"):
                st.session_state.active_menu = label
                for k2 in ["editing_prod_id", "editing_prod_data", "adj_prod", "edit_cat_id",
                           "edit_pag_id", "edit_cli_id", "edit_cli_data", "edit_sup_id",
                           "edit_rep_id", "edit_forn_id", "edit_grupo_id"]:
                    st.session_state[k2] = None
                st.rerun()
            if is_active: st.markdown('</div>', unsafe_allow_html=True)

    n_cart = len(st.session_state.cart)
    if n_cart: st.markdown(f'<div style="margin:8px 10px 0;padding:6px 10px;border-radius:8px;background:rgba(99,102,241,.2);color:#c7d2fe!important;font-size:.8rem;">🛒 {n_cart} item(s) no carrinho</div>', unsafe_allow_html=True)
    st.divider()
    st.markdown(f'<div style="font-size:.75rem;color:rgba(255,255,255,.4)!important;padding:0 4px .3rem">👤 {st.session_state.usuario_nome}<br><span style="font-size:.68rem">{st.session_state.usuario_perfil}</span></div>', unsafe_allow_html=True)
    with st.container():
        st.markdown('<div class="logout-btn">', unsafe_allow_html=True)
        if st.button("🚪  Sair", key="btn_logout"):
            for k in list(st.session_state.keys()): del st.session_state[k]
            st.rerun()
        st.markdown('</div>', unsafe_allow_html=True)
    st.markdown(f"<small style='color:rgba(255,255,255,0.25)'>v8.0 · {datetime.now().strftime('%d/%m/%Y')}</small>", unsafe_allow_html=True)

menu = st.session_state.active_menu

# ══════════════════════════════════════
# 1. DASHBOARD (com gráficos)
# ══════════════════════════════════════
if menu == "Dashboard":
    page_header("📊", "Dashboard", f"Bem-vindo, {st.session_state.usuario_nome}!")

    va = qry("SELECT valor_total FROM vendas WHERE empresa_id=%s AND status='Pago' AND tipo='Venda'", (EMPRESA_ID,), fetch=True)
    tf = sum(float(v[0]) for v in va) if va else 0.0
    qp = len(va); at = (tf / qp) if qp else 0.0

    desp = qry("SELECT COALESCE(SUM(valor),0) FROM despesas WHERE empresa_id=%s AND status='Pago'", (EMPRESA_ID,), fetch=True)
    total_desp = float(desp[0][0]) if desp else 0.0
    lucro_liq = tf - total_desp

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Faturamento Total", f"{cur_sym} {tf:,.2f}")
    c2.metric("Pedidos", qp)
    c3.metric("Ticket Médio", f"{cur_sym} {at:,.2f}")
    c4.metric("Lucro Líquido Est.", f"{cur_sym} {lucro_liq:,.2f}", delta=f"-{cur_sym} {total_desp:,.2f} desp.")

    hr()

    # Gráfico mensal simples (últimos 6 meses)
    if HAS_PANDAS:
        meses = qry("""
            SELECT TO_CHAR(data,'MM/YYYY') as mes, SUM(valor_total)
            FROM vendas WHERE empresa_id=%s AND status='Pago' AND tipo='Venda'
            AND data >= NOW() - INTERVAL '6 months'
            GROUP BY TO_CHAR(data,'MM/YYYY'),DATE_TRUNC('month',data)
            ORDER BY DATE_TRUNC('month',data)
        """, (EMPRESA_ID,), fetch=True)
        if meses:
            st.markdown("**Faturamento — últimos 6 meses**")
            df_m = pd.DataFrame(meses, columns=["Mês", "Valor"])
            st.bar_chart(df_m.set_index("Mês"))

    hr()
    col_dash1, col_dash2 = st.columns(2)

    with col_dash1:
        st.markdown("**Últimas vendas**")
        ul = qry("SELECT v.id,v.cliente_name,v.valor_total,STRING_AGG(i.produto_nome||' x'||i.quantidade,', '),TO_CHAR(v.data,'DD/MM/YYYY HH24:MI') FROM vendas v LEFT JOIN itens_venda i ON i.venda_id=v.id AND i.empresa_id=v.empresa_id WHERE v.empresa_id=%s AND v.status='Pago' AND v.tipo='Venda' GROUP BY v.id ORDER BY v.data DESC LIMIT 5", (EMPRESA_ID,), fetch=True)
        if ul:
            for vid, cli, val, ps, df_v in ul:
                st.markdown(f'<div class="card"><div style="display:flex;justify-content:space-between;flex-wrap:wrap;gap:4px"><div><div class="card-sku">Pedido #{vid} · {df_v}</div><div class="card-title">{cli}</div><div class="card-sub">{ps or "—"}</div></div><div class="card-val">{cur_sym} {float(val):,.2f}</div></div></div>', unsafe_allow_html=True)
        else: show_info("Nenhuma venda registrada.")

    with col_dash2:
        st.markdown("**Contas a receber vencendo**")
        cr = qry("SELECT cliente_name,valor,vencimento FROM contas_receber WHERE empresa_id=%s AND status='Pendente' ORDER BY vencimento LIMIT 5", (EMPRESA_ID,), fetch=True)
        if cr:
            for cli, val, venc_d in cr:
                hoje = date.today()
                dias = (venc_d - hoje).days if venc_d else 0
                cor = "zero" if dias < 0 else ("low" if dias <= 3 else "ok")
                st.markdown(f'<div class="card"><div style="display:flex;justify-content:space-between"><div><div class="card-title">{cli}</div><div class="card-sub">Vence: {venc_d.strftime("%d/%m/%Y") if venc_d else "—"} <span class="badge {cor}">{"Vencido" if dias < 0 else f"{dias}d"}</span></div></div><div class="card-val">{cur_sym} {float(val):,.2f}</div></div></div>', unsafe_allow_html=True)
        else: show_info("Nenhuma pendência.")

        st.markdown("**Metas ativas**")
        mts = qry("SELECT descricao,valor_meta,periodo_inicio,periodo_fim,tipo FROM metas WHERE empresa_id=%s AND ativo=TRUE LIMIT 3", (EMPRESA_ID,), fetch=True)
        for desc, vm, pi, pf, tipo in (mts or []):
            if tipo == "Faturamento":
                w = ["v.empresa_id=%s", "v.status='Pago'", "v.tipo='Venda'"]
                p = [EMPRESA_ID]
                if pi: w.append("v.data>=%s"); p.append(datetime.combine(pi, datetime.min.time()))
                if pf: w.append("v.data<=%s"); p.append(datetime.combine(pf, datetime.max.time()))
                res = qry(f"SELECT COALESCE(SUM(valor_total),0) FROM vendas v WHERE {' AND '.join(w)}", tuple(p), fetch=True)
                atual = float(res[0][0]) if res else 0
            else:
                atual = 0
            pct = min(int(atual / float(vm) * 100), 100) if float(vm) > 0 else 0
            st.markdown(f'<div class="card"><div class="card-title">{desc}</div><div class="card-sub">{cur_sym} {atual:,.2f} de {cur_sym} {float(vm):,.2f} ({pct}%)</div><div class="meta-bar-wrap"><div class="meta-bar" style="width:{pct}%"></div></div></div>', unsafe_allow_html=True)

    low = qry("SELECT nome FROM produtos WHERE empresa_id=%s AND ativo=TRUE AND estoque_atual<=estoque_minimo", (EMPRESA_ID,), fetch=True)
    if low: hr(); show_warning(f"{len(low)} produto(s) com estoque baixo ou zerado", "Verifique a aba Estoque.")

# ══════════════════════════════════════
# 2. BUSCA GLOBAL
# ══════════════════════════════════════
elif menu == "Busca Global":
    page_header("🔍", "Busca Global", "Encontre clientes, produtos ou pedidos")
    busca = st.text_input("O que você está procurando?", placeholder="Digite nome, SKU, CPF, número do pedido…", key="busca_global_inp")
    if busca and len(busca) >= 2:
        b = f"%{busca}%"
        resultados = []
        clis = qry("SELECT id,nome,documento,cidade FROM clientes WHERE empresa_id=%s AND (nome ILIKE %s OR documento ILIKE %s OR cidade ILIKE %s)", (EMPRESA_ID, b, b, b), fetch=True)
        for cid, cn, cdoc, ccid in (clis or []): resultados.append(("Cliente", cn, f"{cdoc} · {ccid or '—'}", "clientes"))
        prods = qry("SELECT id,nome,sku,categoria FROM produtos WHERE empresa_id=%s AND (nome ILIKE %s OR sku ILIKE %s)", (EMPRESA_ID, b, b), fetch=True)
        for pid, pn, psk, pcat in (prods or []): resultados.append(("Produto", pn, f"SKU: {psk} · {pcat}", "estoque"))
        if busca.isdigit():
            vs = qry("SELECT id,cliente_name,valor_total,status FROM vendas WHERE empresa_id=%s AND id=%s", (EMPRESA_ID, int(busca)), fetch=True)
            for vid, vcli, vval, vst in (vs or []): resultados.append(("Pedido", f"#{vid} — {vcli}", f"{cur_sym} {float(vval):,.2f} · {vst}", "hist"))
        fornecs = qry("SELECT id,nome,email FROM fornecedores WHERE empresa_id=%s AND nome ILIKE %s", (EMPRESA_ID, b), fetch=True)
        for fid, fn, fe in (fornecs or []): resultados.append(("Fornecedor", fn, fe or "—", "fornecedores"))

        if resultados:
            st.markdown(f"**{len(resultados)} resultado(s) encontrado(s)**")
            for tipo, nome, sub, dest in resultados:
                col_r, col_btn = st.columns([9, 1])
                with col_r:
                    st.markdown(f'<div class="search-result"><span class="sr-type">{tipo}</span><div><div class="sr-name">{nome}</div><div class="sr-sub">{sub}</div></div></div>', unsafe_allow_html=True)
                with col_btn:
                    if st.button("→", key=f"goto_{tipo}_{nome}"):
                        labels = {"clientes": "Clientes", "estoque": "Estoque", "hist": "Histórico de Vendas", "fornecedores": "Fornecedores"}
                        st.session_state.active_menu = labels.get(dest, "Dashboard"); st.rerun()
        else:
            show_info("Nenhum resultado encontrado.", f"Buscou por: '{busca}'")

# ══════════════════════════════════════
# 3. PEDIDOS (com orçamento e comissão)
# ══════════════════════════════════════
elif menu == "Pedidos":
    page_header("🛒", "Pedidos", "Monte o pedido com múltiplos produtos")
    clis = qry("SELECT nome FROM clientes WHERE empresa_id=%s AND ativo=TRUE ORDER BY nome", (EMPRESA_ID,), fetch=True)
    prods = qry("SELECT id,nome,preco_venda,estoque_atual FROM produtos WHERE empresa_id=%s AND ativo=TRUE AND estoque_atual>0 ORDER BY nome", (EMPRESA_ID,), fetch=True)
    pags = qry("SELECT nome FROM pagamentos WHERE empresa_id=%s AND ativo=TRUE ORDER BY nome", (EMPRESA_ID,), fetch=True)
    sups = qry("SELECT id,nome,comissao_pct FROM supervisores WHERE empresa_id=%s AND ativo=TRUE ORDER BY nome", (EMPRESA_ID,), fetch=True)
    reps = qry("SELECT id,nome,comissao_pct FROM representantes WHERE empresa_id=%s AND ativo=TRUE ORDER BY nome", (EMPRESA_ID,), fetch=True)
    tabelas = qry("SELECT id,nome,desconto_pct FROM tabelas_preco WHERE empresa_id=%s AND ativo=TRUE ORDER BY nome", (EMPRESA_ID,), fetch=True)

    if not clis: show_error("Nenhum cliente ativo.", "Cadastre um cliente."); st.stop()
    if not prods: show_error("Nenhum produto em estoque.", "Cadastre produtos."); st.stop()
    if not pags: show_error("Nenhuma forma de pagamento.", "Cadastre uma forma de pagamento."); st.stop()

    cart = st.session_state.cart
    col_form, col_cart = st.columns([3, 2])

    with col_cart:
        st.markdown("#### 🧺 Carrinho")
        if not cart: show_info("Carrinho vazio.", "Adicione produtos ao lado.")
        else:
            subtotal = sum(x["preco"] * x["qtd"] for x in cart)
            for i, item in enumerate(cart):
                sub = item["preco"] * item["qtd"]
                ci, cr = st.columns([5, 1])
                with ci: st.markdown(f'<div class="cart-item"><div><div class="ci-name">{item["nome"]}</div><div class="ci-qty">{item["qtd"]} un × {cur_sym} {item["preco"]:.2f}</div></div><div class="ci-val">{cur_sym} {sub:.2f}</div></div>', unsafe_allow_html=True)
                with cr:
                    if st.button("🗑️", key=f"rem_{i}", help="Remover"): st.session_state.cart.pop(i); st.rerun()

            # Tabela de preços
            if tabelas:
                tab_sel = st.selectbox("Tabela de preços", ["(nenhuma)"] + [t[1] for t in tabelas], key="tab_preco_sel")
                if tab_sel != "(nenhuma)":
                    tab_obj = next((t for t in tabelas if t[1] == tab_sel), None)
                    if tab_obj: st.caption(f"Desconto automático da tabela: {tab_obj[2]:.1f}%")

            st.markdown("**Desconto adicional**")
            dc1, dc2 = st.columns(2)
            desc_tipo = dc1.selectbox("Tipo", ["Sem desconto", "% Percentual", "R$ Valor fixo"], key="desc_tipo", label_visibility="collapsed")
            desc_val = dc2.number_input("Valor", min_value=0.0, step=0.01, format="%.2f", key="desc_val", label_visibility="collapsed")

            d_pct = 0.0; d_reais = 0.0
            # Desconto da tabela de preços
            if tabelas and st.session_state.get("tab_preco_sel", "(nenhuma)") != "(nenhuma)":
                tab_obj2 = next((t for t in tabelas if t[1] == st.session_state.get("tab_preco_sel")), None)
                if tab_obj2: d_pct += float(tab_obj2[2])

            if desc_tipo == "% Percentual": d_pct += min(desc_val, 100.0)
            elif desc_tipo == "R$ Valor fixo": d_reais = min(desc_val, subtotal)

            d_reais_total = subtotal * d_pct / 100 + d_reais
            total_final = subtotal - d_reais_total
            if d_reais_total > 0: st.markdown(f'<div class="cart-desc">Desconto total: {cur_sym} {d_reais_total:.2f} ({d_pct:.1f}%)</div>', unsafe_allow_html=True)
            st.markdown(f'<div class="cart-total"><span>Total do pedido</span><strong>{cur_sym} {total_final:,.2f}</strong></div>', unsafe_allow_html=True)

    with col_form:
        pn = [p[1] for p in prods]
        with st.form("form_add_item", clear_on_submit=True):
            st.markdown("#### Adicionar produto")
            pi = st.selectbox("Produto *", range(len(pn)), format_func=lambda i: pn[i])
            ps = prods[pi]; ed = int(ps[3]); nc = sum(x["qtd"] for x in cart if x["id"] == ps[0]); dp = max(0, ed - nc)
            st.caption(f"Disponível: **{ed}** · No carrinho: **{nc}** · Pode adicionar: **{dp}**")
            qa = st.number_input("Quantidade *", min_value=1, max_value=dp if dp > 0 else 1, step=1, value=1, disabled=(dp == 0))
            ab = st.form_submit_button("➕  Adicionar ao carrinho", use_container_width=True, disabled=(dp == 0))
        if ab:
            if dp == 0: show_error(f"Estoque esgotado para '{ps[1]}'.")
            else:
                ex = next((x for x in cart if x["id"] == ps[0]), None)
                if ex: ex["qtd"] += qa
                else: st.session_state.cart.append({"id": ps[0], "nome": ps[1], "preco": float(ps[2]), "qtd": qa})
                st.rerun()
        hr()
        if cart:
            with st.form("form_finalizar"):
                st.markdown("#### Finalizar pedido")
                cf1, cf2 = st.columns(2)
                cliente_sel = cf1.selectbox("Cliente *", [c[0] for c in clis])
                forma = cf2.selectbox("Pagamento *", [p[0] for p in pags])
                tipo_ped = st.selectbox("Tipo", ["Venda", "Orçamento"])
                sf1, sf2 = st.columns(2)
                sup_opts = ["(nenhum)"] + [s[1] for s in sups]
                rep_opts = ["(nenhum)"] + [r[1] for r in reps]
                sup_sel = sf1.selectbox("Supervisor (opcional)", sup_opts)
                rep_sel = sf2.selectbox("Representante (opcional)", rep_opts)
                vf1, vf2 = st.columns(2)
                a_prazo = vf1.checkbox("Pagamento a prazo")
                vencimento = vf2.date_input("Vencimento", value=date.today() + timedelta(days=30)) if a_prazo else None
                obs = st.text_area("Observação (opcional)", placeholder="Ex: entregar na portaria…", height=60)
                fin = st.form_submit_button("✅  Finalizar", use_container_width=True)

            if fin:
                erros = []
                if tipo_ped == "Venda":
                    for item in cart:
                        er = get_estoque(item["id"])
                        if er < item["qtd"]: erros.append(f"Estoque insuficiente para '{item['nome']}'. Disponível: {er} un.")
                if erros:
                    for e in erros: show_error(e, "Ajuste as quantidades.")
                else:
                    sb_v = sum(x["preco"] * x["qtd"] for x in cart)
                    dt = st.session_state.get("desc_tipo", "Sem desconto"); dv = st.session_state.get("desc_val", 0.0)
                    if dt == "% Percentual": dp2 = min(dv, 100); dr = sb_v * dp2 / 100
                    elif dt == "R$ Valor fixo": dr = min(dv, sb_v); dp2 = (dr / sb_v * 100) if sb_v else 0
                    else: dp2 = 0.0; dr = 0.0
                    # tabela de preços
                    if tabelas and st.session_state.get("tab_preco_sel", "(nenhuma)") != "(nenhuma)":
                        tab_obj3 = next((t for t in tabelas if t[1] == st.session_state.get("tab_preco_sel")), None)
                        if tab_obj3: dp2 += float(tab_obj3[2]); dr += sb_v * float(tab_obj3[2]) / 100
                    tv = max(sb_v - dr, 0)
                    sup_id = next((s[0] for s in sups if s[1] == sup_sel), None)
                    rep_id = next((r[0] for r in reps if r[1] == rep_sel), None)
                    com_sup = tv * float(next((s[2] for s in sups if s[1] == sup_sel), 0) or 0) / 100
                    com_rep = tv * float(next((r[2] for r in reps if r[1] == rep_sel), 0) or 0) / 100
                    status_v = "Orçamento" if tipo_ped == "Orçamento" else "Pago"
                    row = qry("""INSERT INTO vendas(empresa_id,data,cliente_name,valor_bruto,desconto_pct,desconto_val,valor_total,pagamento,status,observacao,supervisor_id,representante_id,tipo,vencimento,comissao_supervisor,comissao_representante)
                        VALUES(%s,NOW(),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id""",
                        (EMPRESA_ID, cliente_sel, sb_v, dp2, dr, tv, forma, status_v, obs.strip(), sup_id, rep_id, tipo_ped, vencimento, com_sup, com_rep), returning=True)
                    if row:
                        vid = row[0]
                        for item in cart:
                            qry("INSERT INTO itens_venda(empresa_id,venda_id,produto_nome,quantidade,preco_unit) VALUES(%s,%s,%s,%s,%s)", (EMPRESA_ID, vid, item["nome"], item["qtd"], item["preco"]))
                            if tipo_ped == "Venda":
                                qry("UPDATE produtos SET estoque_atual=estoque_atual-%s WHERE id=%s AND empresa_id=%s", (item["qtd"], item["id"], EMPRESA_ID))
                                qry("INSERT INTO movimentacoes_estoque(empresa_id,produto_id,tipo,quantidade,motivo,usuario_nome) VALUES(%s,%s,'Saída',%s,%s,%s)",
                                    (EMPRESA_ID, item["id"], item["qtd"], f"Venda #{vid}", st.session_state.usuario_nome))
                        if a_prazo and vencimento and tipo_ped == "Venda":
                            qry("INSERT INTO contas_receber(empresa_id,venda_id,cliente_name,descricao,valor,vencimento) VALUES(%s,%s,%s,%s,%s,%s)",
                                (EMPRESA_ID, vid, cliente_sel, f"Venda #{vid}", tv, vencimento))
                        log_acao(f"{tipo_ped} #{vid}", f"Cliente: {cliente_sel} · Total: {cur_sym} {tv:.2f}")
                        show_success(f"{tipo_ped} de {cur_sym} {tv:.2f} {'finalizada' if tipo_ped == 'Venda' else 'criada'}!", f"{len(cart)} produto(s) · {cliente_sel} · {forma}")
                        st.session_state.cart = []
                        if tipo_ped == "Venda": st.balloons()
                        st.rerun()
                    else: show_error("Não foi possível salvar.", "Tente novamente.")
        else: show_info("Carrinho vazio.", "Adicione produtos acima para liberar a finalização.")

# ══════════════════════════════════════
# 4. ESTOQUE (com variações, movimentações, código de barras)
# ══════════════════════════════════════
elif menu == "Estoque":
    page_header("📦", "Estoque", "Gerencie seu catálogo de produtos")
    cats_raw = qry("SELECT nome FROM categorias WHERE empresa_id=%s AND ativo=TRUE ORDER BY nome", (EMPRESA_ID,), fetch=True)
    cat_opts = [c[0] for c in cats_raw] if cats_raw else []

    with st.expander("➕  Adicionar novo produto"):
        if not cat_opts: show_warning("Nenhuma categoria ativa.", "Acesse 'Categorias' e crie uma antes.")
        else:
            with st.form("f_prod"):
                c1, c2, c3 = st.columns([1, 2, 1])
                sku = c1.text_input("SKU *"); nome = c2.text_input("Nome *"); cat = c3.selectbox("Categoria *", cat_opts)
                c4, c5, c6, c7, c8 = st.columns(5)
                pc = c4.number_input(f"Custo ({cur_sym})", min_value=0.0, step=0.01, format="%.2f")
                pv = c5.number_input(f"Venda ({cur_sym}) *", min_value=0.0, step=0.01, format="%.2f")
                est = c6.number_input("Estoque Inicial", min_value=0, step=1)
                emin = c7.number_input("Estoque Mín.", min_value=0, step=1, value=2)
                cb_prod = c8.text_input("Cód. Barras")
                sv = st.form_submit_button("💾  Salvar Produto", use_container_width=True)
            if sv:
                if not validate_required(sku, nome): show_error("SKU e Nome são obrigatórios.")
                elif pv == 0: show_error("Preço de venda não pode ser zero.")
                else:
                    res = qry("INSERT INTO produtos(empresa_id,sku,nome,categoria,preco_custo,preco_venda,estoque_atual,estoque_minimo,codigo_barras) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                              (EMPRESA_ID, sku.strip(), nome.strip(), cat, pc, pv, est, emin, cb_prod.strip() or None))
                    if res is True:
                        if est > 0:
                            pid_new = qry("SELECT id FROM produtos WHERE empresa_id=%s AND sku=%s", (EMPRESA_ID, sku.strip()), fetch=True)
                            if pid_new:
                                qry("INSERT INTO movimentacoes_estoque(empresa_id,produto_id,tipo,quantidade,motivo,usuario_nome) VALUES(%s,%s,'Entrada',%s,'Estoque inicial',%s)",
                                    (EMPRESA_ID, pid_new[0][0], est, st.session_state.usuario_nome))
                        show_success("Produto cadastrado!", f"'{nome}' adicionado."); st.rerun()
                    elif res == "duplicate": show_error(f"SKU '{sku}' já existe nesta empresa.")
                    else: show_error("Não foi possível salvar.")

    # Importação CSV
    with st.expander("📥  Importar produtos via CSV"):
        st.markdown("""**Formato esperado:** `sku,nome,categoria,preco_custo,preco_venda,estoque_inicial,estoque_minimo`""")
        csv_file = st.file_uploader("Selecione o arquivo CSV", type=["csv"], key="csv_prod")
        if csv_file and st.button("📥  Importar", key="btn_imp_prod"):
            content = csv_file.read().decode("utf-8").splitlines()
            reader = csv.DictReader(content)
            ok = erros = 0
            for row_c in reader:
                try:
                    res = qry("INSERT INTO produtos(empresa_id,sku,nome,categoria,preco_custo,preco_venda,estoque_atual,estoque_minimo) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)",
                              (EMPRESA_ID, row_c["sku"], row_c["nome"], row_c.get("categoria", "Geral"),
                               float(row_c.get("preco_custo", 0)), float(row_c.get("preco_venda", 0)),
                               int(row_c.get("estoque_inicial", 0)), int(row_c.get("estoque_minimo", 2))))
                    if res is True: ok += 1
                    else: erros += 1
                except: erros += 1
            show_success(f"{ok} produto(s) importado(s)!", f"{erros} erro(s).")

    hr()
    with st.expander("🔍  Filtros", expanded=True):
        fb, ff = st.columns([3, 1])
        busca = fb.text_input("Buscar produto", placeholder="Nome ou SKU…", key="busca_prod", label_visibility="collapsed")
        mostrar = ff.selectbox("Status", ["Ativos", "Inativos", "Todos"], key="filtro_prod", label_visibility="collapsed")

    pr = qry("SELECT id,sku,nome,categoria,preco_custo,preco_venda,estoque_atual,estoque_minimo,ativo,codigo_barras FROM produtos WHERE empresa_id=%s ORDER BY nome", (EMPRESA_ID,), fetch=True)
    if not pr: show_info("Nenhum produto cadastrado ainda.", "Use o formulário acima.")
    else:
        fl = list(pr)
        if mostrar == "Ativos": fl = [p for p in fl if p[8]]
        if mostrar == "Inativos": fl = [p for p in fl if not p[8]]
        if busca: b = busca.lower(); fl = [p for p in fl if b in p[2].lower() or b in p[1].lower()]

        if HAS_PANDAS and fl:
            xb = to_excel(fl, ["ID","SKU","Nome","Categoria","Custo","Venda","Estoque","Est.Mín","Ativo","Cód.Barras"])
            if xb: st.download_button("⬇️ Exportar Excel", data=xb, file_name="produtos.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

        st.markdown(f"**{len(fl)} produto(s)**")

        tabs_est = st.tabs(["Lista", "Movimentações"])

        with tabs_est[0]:
            for prod in fl:
                pid, sv2, nm, cat_v, pc_v, pv_v, est_v, emin_v, ativo_v, cb_v = prod
                be = ('<span class="badge zero">Zerado</span>' if est_v == 0 else '<span class="badge low">Baixo</span>' if est_v <= emin_v else '<span class="badge ok">OK</span>')
                ic = "" if ativo_v else " inativo"
                margem = ((float(pv_v) - float(pc_v)) / float(pv_v) * 100) if float(pv_v) > 0 else 0
                cc2, ce, ca, ct = st.columns([5, 1, 1, 1])
                with cc2:
                    cb_info = f" · 🔖 {cb_v}" if cb_v else ""
                    st.markdown(f'<div class="card{ic}"><div class="card-sku">SKU: {sv2}{cb_info}</div><div class="card-title">{nm}</div><div class="card-sub">{cat_v} · Estoque: {est_v} {be} · Margem: {margem:.1f}% · <span class="badge {"ativo" if ativo_v else "inativo"}">{"Ativo" if ativo_v else "Inativo"}</span></div><div class="card-val">{cur_sym} {float(pv_v):.2f}</div></div>', unsafe_allow_html=True)
                with ce:
                    if st.button("✏️", key=f"edit_{pid}", help="Editar"):
                        st.session_state.editing_prod_id = pid; st.session_state.editing_prod_data = prod; st.session_state.adj_prod = None; st.rerun()
                with ca:
                    if st.button("📦", key=f"adj_{pid}", help="Ajustar estoque"):
                        st.session_state.adj_prod = prod; st.session_state.editing_prod_id = None; st.rerun()
                with ct:
                    if st.button("🔴" if ativo_v else "🟢", key=f"tog_{pid}", help="Inativar" if ativo_v else "Ativar"):
                        qry("UPDATE produtos SET ativo=%s WHERE id=%s AND empresa_id=%s", (not ativo_v, pid, EMPRESA_ID)); st.rerun()

            if st.session_state.editing_prod_id:
                pid_e = st.session_state.editing_prod_id; pd_ = st.session_state.editing_prod_data
                _, sku_e, nm_e, cat_e, pc_e, pv_e, est_e, emin_e, _, cb_e = pd_
                hr(); st.markdown(f"#### ✏️ Editando: {nm_e}")
                with st.form("f_edit"):
                    ce1, ce2, ce3 = st.columns([1, 2, 1])
                    ns = ce1.text_input("SKU *", value=sku_e); nn = ce2.text_input("Nome *", value=nm_e)
                    ci = cat_opts.index(cat_e) if cat_e in cat_opts else 0
                    nc2 = ce3.selectbox("Categoria *", cat_opts or [cat_e], index=ci)
                    ce4, ce5, ce6, ce7, ce8 = st.columns(5)
                    np2 = ce4.number_input(f"Custo ({cur_sym})", value=float(pc_e), min_value=0.0, step=0.01, format="%.2f")
                    npv = ce5.number_input(f"Venda ({cur_sym}) *", value=float(pv_e), min_value=0.0, step=0.01, format="%.2f")
                    ne = ce6.number_input("Estoque", value=int(est_e), min_value=0, step=1)
                    nem = ce7.number_input("Est. Mín.", value=int(emin_e), min_value=0, step=1)
                    ncb = ce8.text_input("Cód. Barras", value=cb_e or "")
                    cs, cc3 = st.columns(2)
                    se = cs.form_submit_button("💾  Salvar", use_container_width=True)
                    ce_b = cc3.form_submit_button("✕  Cancelar", use_container_width=True)
                if ce_b: st.session_state.editing_prod_id = None; st.rerun()
                if se:
                    if not validate_required(ns, nn): show_error("SKU e Nome são obrigatórios.")
                    elif npv == 0: show_error("Preço de venda não pode ser zero.")
                    else:
                        res = qry("UPDATE produtos SET sku=%s,nome=%s,categoria=%s,preco_custo=%s,preco_venda=%s,estoque_atual=%s,estoque_minimo=%s,codigo_barras=%s WHERE id=%s AND empresa_id=%s",
                                  (ns.strip(), nn.strip(), nc2, np2, npv, ne, nem, ncb.strip() or None, pid_e, EMPRESA_ID))
                        if res is True: show_success("Produto atualizado!"); st.session_state.editing_prod_id = None; st.rerun()
                        elif res == "duplicate": show_error("Já existe um produto com esse SKU.")
                        else: show_error("Não foi possível salvar.")

                # Variações
                hr(); st.markdown(f"**Variações de '{nm_e}'**")
                vars_p = qry("SELECT id,atributo,valor,estoque_adicional FROM variacoes_produto WHERE produto_id=%s AND empresa_id=%s", (pid_e, EMPRESA_ID), fetch=True)
                if vars_p:
                    for vid_v, atr, val_v, est_v2 in vars_p:
                        st.markdown(f"- **{atr}**: {val_v} · Estoque extra: {est_v2}")
                with st.form(f"f_var_{pid_e}"):
                    vv1, vv2, vv3 = st.columns(3)
                    va_atr = vv1.text_input("Atributo", placeholder="Cor, Tamanho…")
                    va_val = vv2.text_input("Valor", placeholder="Azul, G…")
                    va_est = vv3.number_input("Estoque extra", min_value=0, step=1)
                    if st.form_submit_button("➕ Adicionar variação"):
                        if validate_required(va_atr, va_val):
                            qry("INSERT INTO variacoes_produto(empresa_id,produto_id,atributo,valor,estoque_adicional) VALUES(%s,%s,%s,%s,%s)",
                                (EMPRESA_ID, pid_e, va_atr, va_val, va_est))
                            st.rerun()

            if st.session_state.adj_prod:
                adj = st.session_state.adj_prod; adj_id = adj[0]; adj_nm = adj[2]; adj_est = int(adj[6])
                hr(); st.markdown(f"#### 📦 Ajustar estoque: {adj_nm}  (atual: **{adj_est}**)")
                with st.form("f_adj"):
                    co, cq = st.columns(2)
                    op = co.selectbox("Operação", ["Adicionar", "Remover", "Definir exato"])
                    qa2 = cq.number_input("Quantidade", min_value=1, step=1)
                    motivo = st.text_input("Motivo (opcional)", placeholder="Ex: compra, perda, inventário…")
                    ca2, cb2 = st.columns(2)
                    ap = ca2.form_submit_button("✅  Aplicar", use_container_width=True)
                    cn = cb2.form_submit_button("✕  Cancelar", use_container_width=True)
                if cn: st.session_state.adj_prod = None; st.rerun()
                if ap:
                    if op == "Adicionar": sq = "UPDATE produtos SET estoque_atual=estoque_atual+%s WHERE id=%s AND empresa_id=%s"; nv = adj_est + qa2; tipo_mov = "Entrada"
                    elif op == "Remover":
                        if qa2 > adj_est: show_error("Quantidade maior que o estoque atual.", f"Máximo: {adj_est} un."); st.stop()
                        sq = "UPDATE produtos SET estoque_atual=estoque_atual-%s WHERE id=%s AND empresa_id=%s"; nv = adj_est - qa2; tipo_mov = "Saída"
                    else: sq = "UPDATE produtos SET estoque_atual=%s WHERE id=%s AND empresa_id=%s"; nv = qa2; tipo_mov = "Ajuste"
                    par = (qa2 if op != "Definir exato" else nv, adj_id, EMPRESA_ID)
                    if qry(sq, par) is True:
                        qry("INSERT INTO movimentacoes_estoque(empresa_id,produto_id,tipo,quantidade,motivo,usuario_nome) VALUES(%s,%s,%s,%s,%s,%s)",
                            (EMPRESA_ID, adj_id, tipo_mov, qa2, motivo or op, st.session_state.usuario_nome))
                        log_acao(f"Ajuste estoque '{adj_nm}'", f"{tipo_mov}: {qa2} un · Motivo: {motivo}")
                        show_success("Estoque ajustado!", f"'{adj_nm}' agora tem {nv} unidades.")
                        st.session_state.adj_prod = None; st.rerun()
                    else: show_error("Não foi possível ajustar o estoque.")

        with tabs_est[1]:
            st.markdown("**Histórico de movimentações**")
            prod_filtro = st.selectbox("Filtrar por produto", ["Todos"] + [p[2] for p in fl], key="mov_prod_fil")
            movs_q = "SELECT m.criado_em,p.nome,m.tipo,m.quantidade,m.motivo,m.usuario_nome FROM movimentacoes_estoque m JOIN produtos p ON p.id=m.produto_id WHERE m.empresa_id=%s"
            movs_p = [EMPRESA_ID]
            if prod_filtro != "Todos":
                pid_f = next((p[0] for p in fl if p[2] == prod_filtro), None)
                if pid_f: movs_q += " AND m.produto_id=%s"; movs_p.append(pid_f)
            movs_q += " ORDER BY m.criado_em DESC LIMIT 100"
            movs = qry(movs_q, tuple(movs_p), fetch=True)
            if movs:
                if HAS_PANDAS:
                    xb = to_excel(movs, ["Data","Produto","Tipo","Qtd","Motivo","Usuário"])
                    if xb: st.download_button("⬇️ Exportar", data=xb, file_name="movimentacoes.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                for data_m, pnome, tipo_m, qtd_m, mot_m, usr_m in movs:
                    cor = "ok" if tipo_m == "Entrada" else ("zero" if tipo_m == "Saída" else "low")
                    data_fmt = data_m.strftime("%d/%m/%Y %H:%M") if data_m else "—"
                    st.markdown(f'<div class="card"><div class="card-sku">{data_fmt} · {usr_m or "—"}</div><div style="display:flex;justify-content:space-between"><div><div class="card-title">{pnome}</div><div class="card-sub">{mot_m or "—"}</div></div><div><span class="badge {cor}">{tipo_m}</span> <strong>{qtd_m}</strong></div></div></div>', unsafe_allow_html=True)
            else: show_info("Nenhuma movimentação registrada.")

# ══════════════════════════════════════
# 5. ENTRADA NF
# ══════════════════════════════════════
elif menu == "Entrada NF":
    page_header("📥", "Entrada de Nota Fiscal", "Registre entradas de mercadoria")
    forns = qry("SELECT nome FROM fornecedores WHERE empresa_id=%s AND ativo=TRUE ORDER BY nome", (EMPRESA_ID,), fetch=True)
    prods_nf = qry("SELECT id,nome,sku FROM produtos WHERE empresa_id=%s AND ativo=TRUE ORDER BY nome", (EMPRESA_ID,), fetch=True)

    if "nf_itens" not in st.session_state: st.session_state.nf_itens = []

    with st.form("f_nf_item", clear_on_submit=True):
        st.markdown("#### Adicionar item à NF")
        n1, n2, n3 = st.columns([3, 1, 1])
        pn_nf = n1.selectbox("Produto", [p[1] for p in prods_nf]) if prods_nf else None
        qtd_nf = n2.number_input("Qtd *", min_value=1, step=1)
        custo_nf = n3.number_input(f"Custo ({cur_sym})", min_value=0.0, step=0.01, format="%.2f")
        add_nf = st.form_submit_button("➕ Adicionar")
    if add_nf and pn_nf:
        prod_sel = next((p for p in prods_nf if p[1] == pn_nf), None)
        if prod_sel:
            st.session_state.nf_itens.append({"id": prod_sel[0], "nome": prod_sel[1], "qtd": qtd_nf, "custo": custo_nf})
            st.rerun()

    if st.session_state.nf_itens:
        st.markdown("**Itens da NF:**")
        total_nf = 0
        for i, it in enumerate(st.session_state.nf_itens):
            sub = it["custo"] * it["qtd"]; total_nf += sub
            ci, cr = st.columns([8, 1])
            ci.markdown(f'<div class="cart-item"><div class="ci-name">{it["nome"]}</div><div class="ci-qty">{it["qtd"]} un × {cur_sym} {it["custo"]:.2f} = {cur_sym} {sub:.2f}</div></div>', unsafe_allow_html=True)
            if cr.button("🗑️", key=f"rem_nf_{i}"): st.session_state.nf_itens.pop(i); st.rerun()
        st.markdown(f'<div class="cart-total"><span>Total NF</span><strong>{cur_sym} {total_nf:,.2f}</strong></div>', unsafe_allow_html=True)

        with st.form("f_nf_finalizar"):
            n1, n2 = st.columns(2)
            num_nf = n1.text_input("Número da NF")
            forn_sel = n2.selectbox("Fornecedor", ["(sem fornecedor)"] + ([f[0] for f in forns] if forns else []))
            obs_nf = st.text_area("Observação", height=60)
            salvar_nf = st.form_submit_button("✅ Registrar Entrada", use_container_width=True)
        if salvar_nf:
            row_nf = qry("INSERT INTO entradas_nf(empresa_id,numero_nf,fornecedor_nome,valor_total,observacao) VALUES(%s,%s,%s,%s,%s) RETURNING id",
                         (EMPRESA_ID, num_nf or None, forn_sel if forn_sel != "(sem fornecedor)" else None, total_nf, obs_nf), returning=True)
            if row_nf:
                eid_nf = row_nf[0]
                for it in st.session_state.nf_itens:
                    qry("INSERT INTO itens_entrada_nf(empresa_id,entrada_id,produto_id,produto_nome,quantidade,preco_custo) VALUES(%s,%s,%s,%s,%s,%s)",
                        (EMPRESA_ID, eid_nf, it["id"], it["nome"], it["qtd"], it["custo"]))
                    qry("UPDATE produtos SET estoque_atual=estoque_atual+%s,preco_custo=%s WHERE id=%s AND empresa_id=%s",
                        (it["qtd"], it["custo"], it["id"], EMPRESA_ID))
                    qry("INSERT INTO movimentacoes_estoque(empresa_id,produto_id,tipo,quantidade,motivo,usuario_nome) VALUES(%s,%s,'Entrada',%s,%s,%s)",
                        (EMPRESA_ID, it["id"], it["qtd"], f"NF {num_nf or eid_nf}", st.session_state.usuario_nome))
                log_acao(f"Entrada NF #{eid_nf}", f"NF: {num_nf} · Total: {cur_sym} {total_nf:.2f}")
                show_success("Entrada registrada!", f"Estoque atualizado para {len(st.session_state.nf_itens)} produto(s).")
                st.session_state.nf_itens = []; st.rerun()

    hr(); st.markdown("**Entradas recentes**")
    ents = qry("SELECT id,data,numero_nf,fornecedor_nome,valor_total FROM entradas_nf WHERE empresa_id=%s ORDER BY criado_em DESC LIMIT 20", (EMPRESA_ID,), fetch=True)
    for eid_e, data_e, nf_e, forn_e, val_e in (ents or []):
        st.markdown(f'<div class="card"><div class="card-sku">NF {nf_e or "s/n"} · {data_e.strftime("%d/%m/%Y") if data_e else "—"}</div><div style="display:flex;justify-content:space-between"><div class="card-title">{forn_e or "Sem fornecedor"}</div><div class="card-val">{cur_sym} {float(val_e):,.2f}</div></div></div>', unsafe_allow_html=True)

# ══════════════════════════════════════
# 6. CLIENTES (com histórico e grupos)
# ══════════════════════════════════════
elif menu == "Clientes":
    page_header("👥", "Clientes", "Gerencie sua base de clientes")
    grupos = qry("SELECT id,nome FROM grupos_clientes WHERE empresa_id=%s AND ativo=TRUE ORDER BY nome", (EMPRESA_ID,), fetch=True)

    with st.expander("➕  Novo cliente"):
        with st.form("f_cli"):
            c1, c2 = st.columns(2); nome = c1.text_input("Nome *"); doc = c2.text_input("CPF/CNPJ *", placeholder="Números ou com pontuação")
            c3, c4, c5 = st.columns(3); tel = c3.text_input("Telefone"); email = c4.text_input("E-mail")
            grp_opts = ["(sem grupo)"] + [g[1] for g in grupos]
            grp_sel = c5.selectbox("Grupo", grp_opts)
            st.markdown("**Endereço**")
            ea, eb, ec = st.columns([3, 1, 2]); rua = ea.text_input("Rua"); num = eb.text_input("Nº"); comp = ec.text_input("Compl.")
            ed, ee, ef, eg = st.columns([2, 2, 1, 2]); bairro = ed.text_input("Bairro"); cidade = ee.text_input("Cidade"); estado = ef.text_input("UF", max_chars=2); cep = eg.text_input("CEP")
            sv = st.form_submit_button("💾  Salvar Cliente", use_container_width=True)
        if sv:
            if not validate_required(nome, doc): show_error("Nome e CPF/CNPJ são obrigatórios.")
            elif not validate_doc(doc): show_error("CPF/CNPJ inválido.", "CPF: 11 dígitos | CNPJ: 14 dígitos.")
            else:
                grp_id = next((g[0] for g in grupos if g[1] == grp_sel), None)
                res = qry("INSERT INTO clientes(empresa_id,nome,documento,telefone,email,rua,numero,complemento,bairro,cidade,estado,cep,grupo_id) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                          (EMPRESA_ID, nome.strip(), re.sub(r'\D', '', doc), tel, email, rua, num, comp, bairro, cidade, estado.upper() if estado else "", cep, grp_id))
                if res is True: show_success("Cliente cadastrado!", f"'{nome}' adicionado."); st.rerun()
                elif res == "duplicate": show_error("Já existe um cliente com esse CPF/CNPJ.")
                else: show_error("Não foi possível salvar.")

    # Importação CSV clientes
    with st.expander("📥  Importar clientes via CSV"):
        st.markdown("**Formato:** `nome,documento,telefone,email,cidade,estado`")
        csv_cli = st.file_uploader("CSV de clientes", type=["csv"], key="csv_cli")
        if csv_cli and st.button("📥 Importar clientes", key="btn_imp_cli"):
            content = csv_cli.read().decode("utf-8").splitlines()
            reader = csv.DictReader(content)
            ok = erros = 0
            for row_c in reader:
                doc_c = re.sub(r'\D', '', row_c.get("documento", ""))
                if not doc_c: erros += 1; continue
                res = qry("INSERT INTO clientes(empresa_id,nome,documento,telefone,email,cidade,estado) VALUES(%s,%s,%s,%s,%s,%s,%s)",
                          (EMPRESA_ID, row_c.get("nome", ""), doc_c, row_c.get("telefone", ""), row_c.get("email", ""), row_c.get("cidade", ""), row_c.get("estado", "")))
                if res is True: ok += 1
                else: erros += 1
            show_success(f"{ok} cliente(s) importado(s)!", f"{erros} erro(s).")

    hr()
    with st.expander("🔍  Filtros", expanded=True):
        cb2, cf2, cg2 = st.columns([2, 1, 1])
        busca_cli = cb2.text_input("Buscar cliente", placeholder="Nome, CPF/CNPJ ou cidade…", key="busca_cli", label_visibility="collapsed")
        filtro_cli = cf2.selectbox("Status", ["Ativos", "Inativos", "Todos"], key="filtro_cli", label_visibility="collapsed")
        filtro_grp = cg2.selectbox("Grupo", ["Todos"] + [g[1] for g in grupos], key="filtro_grp_cli", label_visibility="collapsed")

    cf_ = qry("SELECT c.id,c.nome,c.documento,c.telefone,c.email,c.rua,c.numero,c.complemento,c.bairro,c.cidade,c.estado,c.cep,c.ativo,COALESCE(g.nome,'—') FROM clientes c LEFT JOIN grupos_clientes g ON g.id=c.grupo_id WHERE c.empresa_id=%s ORDER BY c.nome", (EMPRESA_ID,), fetch=True)
    if cf_:
        cv = list(cf_)
        if filtro_cli == "Ativos": cv = [c for c in cv if c[12]]
        if filtro_cli == "Inativos": cv = [c for c in cv if not c[12]]
        if busca_cli: b = busca_cli.lower(); cv = [c for c in cv if b in (c[1] or "").lower() or b in (c[2] or "").lower() or b in (c[9] or "").lower()]
        if filtro_grp != "Todos": cv = [c for c in cv if c[13] == filtro_grp]

        if HAS_PANDAS and cv:
            xb = to_excel(cv, ["ID","Nome","Doc","Tel","Email","Rua","Nº","Comp","Bairro","Cidade","UF","CEP","Ativo","Grupo"])
            if xb: st.download_button("⬇️ Exportar Excel", data=xb, file_name="clientes.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

        st.markdown(f"**{len(cv)} cliente(s)**")
        for cli in cv:
            cid, cnome, cdoc, ctel, cemail, crua, cnum, ccomp, cbairro, ccidade, cestado, ccep, cativo, cgrp = cli
            sb = '<span class="badge ativo">Ativo</span>' if cativo else '<span class="badge inativo">Inativo</span>'

            # Expansor com histórico
            with st.expander(f"{'🟢' if cativo else '⚫'} {cnome} — {cdoc} · {ccidade or '—'} · {cgrp}"):
                ci_col, ca_col, ct_col = st.columns([6, 1, 1])
                ci_col.markdown(f"📞 {ctel or '—'} · ✉️ {cemail or '—'} · {sb}")
                if ca_col.button("✏️", key=f"edit_cli_{cid}"):
                    st.session_state.edit_cli_id = cid; st.rerun()
                if ct_col.button("🔴" if cativo else "🟢", key=f"tog_cli_{cid}"):
                    qry("UPDATE clientes SET ativo=%s WHERE id=%s AND empresa_id=%s", (not cativo, cid, EMPRESA_ID)); st.rerun()

                if st.session_state.edit_cli_id == cid:
                    with st.form(f"f_edit_cli_{cid}"):
                        ec1, ec2 = st.columns(2); en = ec1.text_input("Nome *", value=cnome or ""); edo = ec2.text_input("CPF/CNPJ *", value=cdoc or "")
                        ec3, ec4 = st.columns(2); etl = ec3.text_input("Telefone", value=ctel or ""); eml = ec4.text_input("E-mail", value=cemail or "")
                        eea, eeb, eec = st.columns([3, 1, 2]); erua = eea.text_input("Rua", value=crua or ""); enum = eeb.text_input("Nº", value=cnum or ""); ecomp = eec.text_input("Compl.", value=ccomp or "")
                        eed, eee, eef, eeg = st.columns([2, 2, 1, 2]); ebairro = eed.text_input("Bairro", value=cbairro or ""); ecidade = eee.text_input("Cidade", value=ccidade or ""); eestado = eef.text_input("UF", value=cestado or "", max_chars=2); ecep = eeg.text_input("CEP", value=ccep or "")
                        cs_e, cc_e = st.columns(2); sv_e = cs_e.form_submit_button("💾  Salvar", use_container_width=True); cn_e = cc_e.form_submit_button("✕  Cancelar", use_container_width=True)
                    if cn_e: st.session_state.edit_cli_id = None; st.rerun()
                    if sv_e:
                        if not validate_required(en, edo): show_error("Nome e CPF/CNPJ obrigatórios.")
                        elif not validate_doc(edo): show_error("CPF/CNPJ inválido.")
                        else:
                            res = qry("UPDATE clientes SET nome=%s,documento=%s,telefone=%s,email=%s,rua=%s,numero=%s,complemento=%s,bairro=%s,cidade=%s,estado=%s,cep=%s WHERE id=%s AND empresa_id=%s",
                                      (en.strip(), re.sub(r'\D', '', edo), etl, eml, erua, enum, ecomp, ebairro, ecidade, eestado.upper() if eestado else "", ecep, cid, EMPRESA_ID))
                            if res is True: show_success("Cliente atualizado!"); st.session_state.edit_cli_id = None; st.rerun()
                            elif res == "duplicate": show_error("Já existe um cliente com esse CPF/CNPJ.")
                            else: show_error("Não foi possível salvar.")

                # Histórico de compras do cliente
                hist_cli = qry("SELECT v.id,TO_CHAR(v.data,'DD/MM/YYYY'),v.valor_total,v.status FROM vendas v WHERE v.empresa_id=%s AND v.cliente_name=%s AND v.tipo='Venda' ORDER BY v.data DESC LIMIT 10",
                               (EMPRESA_ID, cnome), fetch=True)
                if hist_cli:
                    total_cli = sum(float(h[2]) for h in hist_cli if h[3] == "Pago")
                    st.markdown(f"**Histórico de compras** · Total gasto: {cur_sym} {total_cli:,.2f}")
                    for hid, hdata, hval, hst in hist_cli:
                        sb_h = '<span class="badge pago">Pago</span>' if hst == "Pago" else f'<span class="badge cancelado">{hst}</span>'
                        st.markdown(f'<div class="card"><div style="display:flex;justify-content:space-between"><span class="card-sub">#{hid} · {hdata} {sb_h}</span><span class="card-val">{cur_sym} {float(hval):,.2f}</span></div></div>', unsafe_allow_html=True)
                else: st.caption("Nenhuma compra registrada.")
    else: show_info("Nenhum cliente encontrado.", "Ajuste os filtros ou adicione um novo cliente.")

# ══════════════════════════════════════
# 7. FORNECEDORES
# ══════════════════════════════════════
elif menu == "Fornecedores":
    page_header("🏭", "Fornecedores", "Gerencie seus fornecedores")
    with st.expander("➕  Novo fornecedor"):
        with st.form("f_forn"):
            f1, f2 = st.columns(2); fn = f1.text_input("Nome *"); fdoc = f2.text_input("CNPJ")
            f3, f4, f5 = st.columns(3); ftel = f3.text_input("Telefone"); femail = f4.text_input("E-mail"); fcontato = f5.text_input("Contato")
            sv = st.form_submit_button("💾  Salvar", use_container_width=True)
        if sv:
            if not validate_required(fn): show_error("Nome é obrigatório.")
            else:
                res = qry("INSERT INTO fornecedores(empresa_id,nome,documento,telefone,email,contato) VALUES(%s,%s,%s,%s,%s,%s)",
                          (EMPRESA_ID, fn.strip(), fdoc, ftel, femail, fcontato))
                if res is True: show_success("Fornecedor cadastrado!", f"'{fn}' adicionado."); st.rerun()
                elif res == "duplicate": show_error("Já existe um fornecedor com esse nome.")
                else: show_error("Não foi possível salvar.")

    hr()
    busca_f = st.text_input("🔍 Buscar fornecedor", placeholder="Nome…", key="busca_forn")
    forns = qry("SELECT id,nome,documento,telefone,email,contato,ativo FROM fornecedores WHERE empresa_id=%s ORDER BY nome", (EMPRESA_ID,), fetch=True)
    if forns:
        if busca_f: b = busca_f.lower(); forns = [f for f in forns if b in (f[1] or "").lower()]
        if HAS_PANDAS:
            xb = to_excel(forns, ["ID","Nome","CNPJ","Tel","Email","Contato","Ativo"])
            if xb: st.download_button("⬇️ Exportar Excel", data=xb, file_name="fornecedores.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        st.markdown(f"**{len(forns)} fornecedor(es)**")
        for fid, fnome, fdoc2, ftel2, femail2, fcont, fativo in forns:
            sb = '<span class="badge ativo">Ativo</span>' if fativo else '<span class="badge inativo">Inativo</span>'
            if st.session_state.edit_forn_id == fid:
                with st.form(f"f_edit_forn_{fid}"):
                    ef1, ef2 = st.columns(2); en = ef1.text_input("Nome *", value=fnome or ""); edoc = ef2.text_input("CNPJ", value=fdoc2 or "")
                    ef3, ef4, ef5 = st.columns(3); etel = ef3.text_input("Telefone", value=ftel2 or ""); eemail = ef4.text_input("E-mail", value=femail2 or ""); econt = ef5.text_input("Contato", value=fcont or "")
                    cs, cc = st.columns(2); sv2 = cs.form_submit_button("💾 Salvar", use_container_width=True); cn2 = cc.form_submit_button("✕ Cancelar", use_container_width=True)
                if cn2: st.session_state.edit_forn_id = None; st.rerun()
                if sv2:
                    if not validate_required(en): show_error("Nome é obrigatório.")
                    else:
                        qry("UPDATE fornecedores SET nome=%s,documento=%s,telefone=%s,email=%s,contato=%s WHERE id=%s AND empresa_id=%s", (en.strip(), edoc, etel, eemail, econt, fid, EMPRESA_ID))
                        show_success("Fornecedor atualizado!"); st.session_state.edit_forn_id = None; st.rerun()
            else:
                fi2, fe2, ft2 = st.columns([8, 1, 1])
                fi2.markdown(f'<div class="card"><div class="card-title">{fnome} &nbsp;{sb}</div><div class="card-sub">{fdoc2 or "—"} · {ftel2 or "—"} · {femail2 or "—"}</div></div>', unsafe_allow_html=True)
                if fe2.button("✏️", key=f"edit_forn_{fid}"): st.session_state.edit_forn_id = fid; st.rerun()
                if ft2.button("🔴" if fativo else "🟢", key=f"tog_forn_{fid}"):
                    qry("UPDATE fornecedores SET ativo=%s WHERE id=%s AND empresa_id=%s", (not fativo, fid, EMPRESA_ID)); st.rerun()
            st.markdown('<div style="border-top:.5px solid #eef0ff;margin:4px 0"></div>', unsafe_allow_html=True)
    else: show_info("Nenhum fornecedor cadastrado.", "Adicione um acima.")

# ══════════════════════════════════════
# 8. HISTÓRICO DE VENDAS
# ══════════════════════════════════════
elif menu == "Histórico de Vendas":
    page_header("📜", "Histórico de Vendas", "Consulte, filtre e exporte transações")
    with st.expander("🔍  Filtros", expanded=True):
        hf1, hf2, hf3, hf4, hf5 = st.columns(5)
        fs = hf1.selectbox("Status", ["Todos", "Pago", "Cancelado", "Orçamento"], key="filtro_hist")
        di = hf2.date_input("Data inicial", value=date.today() - timedelta(days=30), key="hist_ini")
        df2 = hf3.date_input("Data final", value=date.today(), key="hist_fim")
        bch = hf4.text_input("Cliente", placeholder="Nome…", key="busca_hist_cli", label_visibility="collapsed")
        rep_f = hf5.text_input("Representante", placeholder="Nome…", key="busca_hist_rep", label_visibility="collapsed")

    wp = ["v.empresa_id=%s", "v.data>=%s", "v.data<%s"]
    ph = [EMPRESA_ID, datetime.combine(di, datetime.min.time()), datetime.combine(df2 + timedelta(days=1), datetime.min.time())]
    if fs != "Todos": wp.append("v.status=%s"); ph.append(fs)
    if bch: wp.append("v.cliente_name ILIKE %s"); ph.append(f"%{bch}%")
    if rep_f: wp.append("rep.nome ILIKE %s"); ph.append(f"%{rep_f}%")
    ws = " AND ".join(wp)

    vendas = qry(f"""SELECT v.id,TO_CHAR(v.data,'DD/MM/YYYY HH24:MI'),v.cliente_name,v.valor_bruto,v.desconto_val,v.valor_total,
        v.pagamento,v.status,STRING_AGG(i.produto_nome||' x'||i.quantidade,' | '),COALESCE(v.observacao,''),
        COALESCE(sup.nome,'—'),COALESCE(rep.nome,'—'),v.tipo
        FROM vendas v
        LEFT JOIN itens_venda i ON i.venda_id=v.id AND i.empresa_id=v.empresa_id
        LEFT JOIN supervisores sup ON sup.id=v.supervisor_id
        LEFT JOIN representantes rep ON rep.id=v.representante_id
        WHERE {ws}
        GROUP BY v.id,sup.nome,rep.nome ORDER BY v.data DESC""", tuple(ph), fetch=True)

    if vendas and HAS_PANDAS:
        xb = to_excel(vendas, ["#","Data","Cliente","Bruto","Desconto","Total","Pagamento","Status","Produtos","Obs","Supervisor","Representante","Tipo"])
        if xb: st.download_button("⬇️  Exportar Excel", data=xb, file_name=f"vendas_{date.today()}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    if not vendas: show_info("Nenhuma venda encontrada.", "Ajuste os filtros.")
    else:
        total_periodo = sum(float(v[5]) for v in vendas if v[7] == "Pago")
        st.markdown(f"**{len(vendas)} registro(s)** · Total período: **{cur_sym} {total_periodo:,.2f}**")
        for row in vendas:
            vid, dfmt, cli, bruto, dv, val, pag, status, itens, obs, sup_n, rep_n, tipo_v = row
            itens = itens or "—"; obs = obs or ""
            ip = status == "Pago"
            sb_map = {"Pago": "pago", "Cancelado": "cancelado", "Orçamento": "orcamento"}
            sb = f'<span class="badge {sb_map.get(status,"inativo")}">{status}</span>'
            tipo_badge = f'<span class="badge low">{tipo_v}</span>' if tipo_v != "Venda" else ""
            oh = f'<div class="card-sub" style="font-style:italic">📝 {obs}</div>' if obs else ""
            dh = f'<div class="card-sub">Desconto: {cur_sym} {float(dv):.2f}</div>' if dv and float(dv) > 0 else ""
            ci2, cv2, ca2 = st.columns([5, 2, 2])
            with ci2: st.markdown(f'<div class="card"><div class="card-sku">#{vid} · {dfmt} · {pag} · 👔{sup_n} · 🤝{rep_n}</div><div class="card-title">{cli} &nbsp;{sb} {tipo_badge}</div><div class="card-sub">{itens}</div>{dh}{oh}</div>', unsafe_allow_html=True)
            with cv2: st.markdown(f'<div style="font-family:Sora,sans-serif;font-size:1rem;font-weight:700;color:#6366f1;padding-top:14px">{cur_sym} {float(val):,.2f}</div>', unsafe_allow_html=True)
            with ca2:
                if status == "Orçamento":
                    if st.button("✅ Converter Venda", key=f"conv_{vid}"):
                        qry("UPDATE vendas SET status='Pago',tipo='Venda' WHERE id=%s AND empresa_id=%s", (vid, EMPRESA_ID))
                        idb = qry("SELECT produto_nome,quantidade FROM itens_venda WHERE venda_id=%s AND empresa_id=%s", (vid, EMPRESA_ID), fetch=True)
                        for pn, pq in (idb or []):
                            qry("UPDATE produtos SET estoque_atual=estoque_atual-%s WHERE nome=%s AND empresa_id=%s", (pq, pn, EMPRESA_ID))
                        show_success(f"Orçamento #{vid} convertido em venda!"); st.rerun()
                elif ip:
                    if st.button("❌ Cancelar", key=f"cancel_{vid}", help="Cancelar e reverter estoque"):
                        idb = qry("SELECT produto_nome,quantidade FROM itens_venda WHERE venda_id=%s AND empresa_id=%s", (vid, EMPRESA_ID), fetch=True)
                        if idb:
                            for pn, pq in idb: qry("UPDATE produtos SET estoque_atual=estoque_atual+%s WHERE nome=%s AND empresa_id=%s", (pq, pn, EMPRESA_ID))
                        qry("UPDATE vendas SET status='Cancelado' WHERE id=%s AND empresa_id=%s", (vid, EMPRESA_ID))
                        log_acao(f"Cancelou venda #{vid}", f"Cliente: {cli}")
                        show_success(f"Pedido #{vid} cancelado.", "Estoque revertido."); st.rerun()
                else:
                    if st.button("✅ Reativar", key=f"reativar_{vid}", help="Reativar e debitar estoque"):
                        idb = qry("SELECT produto_nome,quantidade FROM itens_venda WHERE venda_id=%s AND empresa_id=%s", (vid, EMPRESA_ID), fetch=True)
                        errs = []
                        if idb:
                            for pn, pq in idb:
                                er = qry("SELECT estoque_atual FROM produtos WHERE nome=%s AND empresa_id=%s", (pn, EMPRESA_ID), fetch=True)
                                ev = er[0][0] if er else 0
                                if ev < pq: errs.append(f"Estoque insuficiente para '{pn}': necessário {pq}, disponível {ev}.")
                        if errs:
                            for e in errs: show_error(e, "Ajuste o estoque antes de reativar.")
                        else:
                            if idb:
                                for pn, pq in idb: qry("UPDATE produtos SET estoque_atual=estoque_atual-%s WHERE nome=%s AND empresa_id=%s", (pq, pn, EMPRESA_ID))
                            qry("UPDATE vendas SET status='Pago' WHERE id=%s AND empresa_id=%s", (vid, EMPRESA_ID))
                            show_success(f"Pedido #{vid} reativado!", "Estoque debitado."); st.rerun()
            st.markdown('<div style="border-top:.5px solid #eef0ff;margin:6px 0"></div>', unsafe_allow_html=True)

# ══════════════════════════════════════
# 9. ORÇAMENTOS
# ══════════════════════════════════════
elif menu == "Orçamentos":
    page_header("📋", "Orçamentos", "Visualize e gerencie orçamentos pendentes")
    orcs = qry("""SELECT v.id,TO_CHAR(v.data,'DD/MM/YYYY'),v.cliente_name,v.valor_total,v.pagamento,
        STRING_AGG(i.produto_nome||' x'||i.quantidade,' | ')
        FROM vendas v LEFT JOIN itens_venda i ON i.venda_id=v.id AND i.empresa_id=v.empresa_id
        WHERE v.empresa_id=%s AND v.status='Orçamento'
        GROUP BY v.id ORDER BY v.data DESC""", (EMPRESA_ID,), fetch=True)
    if not orcs: show_info("Nenhum orçamento aberto.", "Crie um pedido do tipo 'Orçamento'.")
    else:
        st.markdown(f"**{len(orcs)} orçamento(s) aberto(s)**")
        for oid, odata, ocli, oval, opag, oitens in orcs:
            co1, co2, co3 = st.columns([5, 2, 2])
            co1.markdown(f'<div class="card"><div class="card-sku">#{oid} · {odata} · {opag}</div><div class="card-title">{ocli} <span class="badge orcamento">Orçamento</span></div><div class="card-sub">{oitens or "—"}</div></div>', unsafe_allow_html=True)
            co2.markdown(f'<div style="font-family:Sora,sans-serif;font-size:1rem;font-weight:700;color:#6366f1;padding-top:14px">{cur_sym} {float(oval):,.2f}</div>', unsafe_allow_html=True)
            with co3:
                if st.button("✅ Converter em Venda", key=f"conv_orc_{oid}"):
                    idb = qry("SELECT produto_id,produto_nome,quantidade FROM itens_venda iv JOIN produtos p ON p.nome=iv.produto_nome AND p.empresa_id=iv.empresa_id WHERE iv.venda_id=%s AND iv.empresa_id=%s", (oid, EMPRESA_ID), fetch=True)
                    errs = []
                    for pid_oc, pn_oc, pq_oc in (idb or []):
                        er = get_estoque(pid_oc)
                        if er < pq_oc: errs.append(f"Estoque insuficiente para '{pn_oc}'.")
                    if errs:
                        for e in errs: show_error(e)
                    else:
                        for pid_oc, pn_oc, pq_oc in (idb or []):
                            qry("UPDATE produtos SET estoque_atual=estoque_atual-%s WHERE id=%s AND empresa_id=%s", (pq_oc, pid_oc, EMPRESA_ID))
                        qry("UPDATE vendas SET status='Pago',tipo='Venda' WHERE id=%s AND empresa_id=%s", (oid, EMPRESA_ID))
                        show_success(f"Orçamento #{oid} convertido!"); st.rerun()
                if st.button("❌ Descartar", key=f"disc_orc_{oid}"):
                    qry("UPDATE vendas SET status='Cancelado' WHERE id=%s AND empresa_id=%s", (oid, EMPRESA_ID)); st.rerun()

# ══════════════════════════════════════
# 10. CONTAS A RECEBER
# ══════════════════════════════════════
elif menu == "Contas a Receber":
    page_header("💰", "Contas a Receber", "Controle de cobranças e recebimentos")

    with st.expander("➕  Lançar conta manual"):
        with st.form("f_cr"):
            cr1, cr2 = st.columns(2); cli_cr = cr1.text_input("Cliente *"); desc_cr = cr2.text_input("Descrição *")
            cr3, cr4 = st.columns(2); val_cr = cr3.number_input(f"Valor ({cur_sym}) *", min_value=0.01, step=0.01, format="%.2f"); venc_cr = cr4.date_input("Vencimento *")
            sv = st.form_submit_button("💾 Salvar", use_container_width=True)
        if sv:
            if not validate_required(cli_cr, desc_cr): show_error("Cliente e descrição obrigatórios.")
            else:
                qry("INSERT INTO contas_receber(empresa_id,cliente_name,descricao,valor,vencimento) VALUES(%s,%s,%s,%s,%s)",
                    (EMPRESA_ID, cli_cr, desc_cr, val_cr, venc_cr))
                show_success("Conta lançada!"); st.rerun()

    hr()
    filtro_cr = st.selectbox("Status", ["Todos","Pendente","Recebido"], key="filtro_cr")
    cr_q = "SELECT id,cliente_name,descricao,valor,vencimento,status,data_pagamento FROM contas_receber WHERE empresa_id=%s"
    cr_p = [EMPRESA_ID]
    if filtro_cr != "Todos": cr_q += " AND status=%s"; cr_p.append(filtro_cr)
    cr_q += " ORDER BY vencimento"
    contas = qry(cr_q, tuple(cr_p), fetch=True)

    total_pend = sum(float(c[3]) for c in (contas or []) if c[5] == "Pendente")
    total_rec = sum(float(c[3]) for c in (contas or []) if c[5] == "Recebido")
    m1, m2 = st.columns(2)
    m1.metric("A receber", f"{cur_sym} {total_pend:,.2f}")
    m2.metric("Já recebido", f"{cur_sym} {total_rec:,.2f}")
    hr()

    if HAS_PANDAS and contas:
        xb = to_excel(contas, ["ID","Cliente","Descrição","Valor","Vencimento","Status","Data Pgto"])
        if xb: st.download_button("⬇️ Exportar Excel", data=xb, file_name="contas_receber.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    for c in (contas or []):
        cid, ccli, cdesc, cval, cvenc, cstatus, cdata_p = c
        hoje = date.today()
        dias = (cvenc - hoje).days if cvenc else 0
        cor = "zero" if (dias < 0 and cstatus == "Pendente") else ("low" if dias <= 3 and cstatus == "Pendente" else "ok")
        sb = f'<span class="badge {"pago" if cstatus=="Recebido" else "pendente"}">{cstatus}</span>'
        ci2, ca2 = st.columns([8, 2])
        ci2.markdown(f'<div class="card"><div class="card-title">{ccli} &nbsp;{sb}</div><div class="card-sub">{cdesc} · Vence: {cvenc.strftime("%d/%m/%Y") if cvenc else "—"} <span class="badge {cor}">{"Vencido" if dias < 0 and cstatus=="Pendente" else f"{dias}d" if cstatus=="Pendente" else "✓"}</span></div><div class="card-val">{cur_sym} {float(cval):,.2f}</div></div>', unsafe_allow_html=True)
        with ca2:
            if cstatus == "Pendente":
                if st.button("✅ Marcar recebido", key=f"rec_{cid}"):
                    qry("UPDATE contas_receber SET status='Recebido',data_pagamento=CURRENT_DATE WHERE id=%s AND empresa_id=%s", (cid, EMPRESA_ID))
                    show_success("Marcado como recebido!"); st.rerun()
            else:
                st.markdown(f"<small>Recebido em {cdata_p.strftime('%d/%m/%Y') if cdata_p else '—'}</small>", unsafe_allow_html=True)
        st.markdown('<div style="border-top:.5px solid #eef0ff;margin:4px 0"></div>', unsafe_allow_html=True)

# ══════════════════════════════════════
# 11. DESPESAS
# ══════════════════════════════════════
elif menu == "Despesas":
    page_header("💸", "Despesas", "Registre e controle custos operacionais")
    with st.expander("➕  Nova despesa"):
        with st.form("f_desp"):
            d1, d2 = st.columns(2); desc_d = d1.text_input("Descrição *"); cat_d = d2.text_input("Categoria", placeholder="Aluguel, Luz, Transporte…")
            d3, d4, d5 = st.columns(3)
            val_d = d3.number_input(f"Valor ({cur_sym}) *", min_value=0.01, step=0.01, format="%.2f")
            data_d = d4.date_input("Data *", value=date.today())
            status_d = d5.selectbox("Status", ["Pago", "Pendente"])
            obs_d = st.text_area("Observação", height=50)
            sv = st.form_submit_button("💾 Salvar", use_container_width=True)
        if sv:
            if not validate_required(desc_d): show_error("Descrição é obrigatória.")
            else:
                qry("INSERT INTO despesas(empresa_id,descricao,categoria,valor,data,status,observacao) VALUES(%s,%s,%s,%s,%s,%s,%s)",
                    (EMPRESA_ID, desc_d, cat_d, val_d, data_d, status_d, obs_d))
                show_success("Despesa registrada!"); st.rerun()

    hr()
    df1, df2 = st.columns(2)
    fil_desp = df1.selectbox("Status", ["Todos","Pago","Pendente"], key="fil_desp")
    mes_desp = df2.date_input("A partir de", value=date.today().replace(day=1), key="mes_desp")
    desp_q = "SELECT id,descricao,categoria,valor,data,status,observacao FROM despesas WHERE empresa_id=%s AND data>=%s"
    desp_p = [EMPRESA_ID, mes_desp]
    if fil_desp != "Todos": desp_q += " AND status=%s"; desp_p.append(fil_desp)
    desp_q += " ORDER BY data DESC"
    desps = qry(desp_q, tuple(desp_p), fetch=True)

    total_d = sum(float(d[3]) for d in (desps or []))
    total_pago_d = sum(float(d[3]) for d in (desps or []) if d[5] == "Pago")
    m1, m2 = st.columns(2)
    m1.metric("Total período", f"{cur_sym} {total_d:,.2f}")
    m2.metric("Pagas", f"{cur_sym} {total_pago_d:,.2f}")

    if HAS_PANDAS and desps:
        xb = to_excel(desps, ["ID","Descrição","Categoria","Valor","Data","Status","Obs"])
        if xb: st.download_button("⬇️ Exportar Excel", data=xb, file_name="despesas.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    hr()
    for d in (desps or []):
        did, ddesc, dcat, dval, ddata, dstatus, dobs = d
        sb = f'<span class="badge {"pago" if dstatus=="Pago" else "pendente"}">{dstatus}</span>'
        di2, da2 = st.columns([8, 2])
        di2.markdown(f'<div class="card"><div class="card-title">{ddesc} &nbsp;{sb}</div><div class="card-sub">{dcat or "—"} · {ddata.strftime("%d/%m/%Y") if ddata else "—"}</div><div class="card-val">{cur_sym} {float(dval):,.2f}</div></div>', unsafe_allow_html=True)
        with da2:
            if dstatus == "Pendente":
                if st.button("✅ Pagar", key=f"pag_d_{did}"):
                    qry("UPDATE despesas SET status='Pago' WHERE id=%s AND empresa_id=%s", (did, EMPRESA_ID)); st.rerun()
            if pode("despesas_del"):
                if st.button("🗑️", key=f"del_d_{did}"):
                    qry("DELETE FROM despesas WHERE id=%s AND empresa_id=%s", (did, EMPRESA_ID)); st.rerun()
        st.markdown('<div style="border-top:.5px solid #eef0ff;margin:4px 0"></div>', unsafe_allow_html=True)

# ══════════════════════════════════════
# 12. METAS
# ══════════════════════════════════════
elif menu == "Metas":
    page_header("🎯", "Metas", "Defina e acompanhe metas de faturamento")
    if pode("metas_edit"):
        with st.expander("➕  Nova meta"):
            with st.form("f_meta"):
                m1, m2 = st.columns(2); desc_m = m1.text_input("Descrição *"); tipo_m = m2.selectbox("Tipo", ["Faturamento", "Quantidade de Pedidos"])
                m3, m4, m5 = st.columns(3)
                vm = m3.number_input(f"Valor Meta ({cur_sym})", min_value=0.01, step=1.0, format="%.2f")
                pi_m = m4.date_input("Início", value=date.today().replace(day=1))
                pf_m = m5.date_input("Fim", value=date.today())
                sv = st.form_submit_button("💾 Salvar Meta", use_container_width=True)
            if sv:
                if not validate_required(desc_m): show_error("Descrição obrigatória.")
                else:
                    qry("INSERT INTO metas(empresa_id,descricao,valor_meta,periodo_inicio,periodo_fim,tipo) VALUES(%s,%s,%s,%s,%s,%s)",
                        (EMPRESA_ID, desc_m, vm, pi_m, pf_m, tipo_m))
                    show_success("Meta criada!"); st.rerun()

    metas = qry("SELECT id,descricao,valor_meta,periodo_inicio,periodo_fim,tipo,ativo FROM metas WHERE empresa_id=%s ORDER BY periodo_fim DESC", (EMPRESA_ID,), fetch=True)
    if not metas: show_info("Nenhuma meta cadastrada.")
    else:
        for mid, mdesc, mvm, mpi, mpf, mtipo, mativo in metas:
            if mtipo == "Faturamento":
                wp2 = ["v.empresa_id=%s", "v.status='Pago'", "v.tipo='Venda'"]
                pp2 = [EMPRESA_ID]
                if mpi: wp2.append("v.data>=%s"); pp2.append(datetime.combine(mpi, datetime.min.time()))
                if mpf: wp2.append("v.data<=%s"); pp2.append(datetime.combine(mpf, datetime.max.time()))
                res = qry(f"SELECT COALESCE(SUM(valor_total),0) FROM vendas v WHERE {' AND '.join(wp2)}", tuple(pp2), fetch=True)
                atual = float(res[0][0]) if res else 0
            else:
                wp2 = ["empresa_id=%s", "status='Pago'", "tipo='Venda'"]
                pp2 = [EMPRESA_ID]
                if mpi: wp2.append("data>=%s"); pp2.append(datetime.combine(mpi, datetime.min.time()))
                if mpf: wp2.append("data<=%s"); pp2.append(datetime.combine(mpf, datetime.max.time()))
                res = qry(f"SELECT COUNT(*) FROM vendas WHERE {' AND '.join(wp2)}", tuple(pp2), fetch=True)
                atual = float(res[0][0]) if res else 0
            pct = min(int(atual / float(mvm) * 100), 100) if float(mvm) > 0 else 0
            cor_badge = "ok" if pct >= 100 else ("low" if pct >= 50 else "zero")
            pi_fmt = mpi.strftime("%d/%m/%Y") if mpi else "—"; pf_fmt = mpf.strftime("%d/%m/%Y") if mpf else "—"
            mc1, mc2 = st.columns([8, 2])
            mc1.markdown(f'<div class="card"><div class="card-title">{mdesc} <span class="badge {cor_badge}">{pct}%</span></div><div class="card-sub">{mtipo} · {pi_fmt} → {pf_fmt}</div><div class="card-sub">{cur_sym} {atual:,.2f} de {cur_sym} {float(mvm):,.2f}</div><div class="meta-bar-wrap"><div class="meta-bar" style="width:{pct}%"></div></div></div>', unsafe_allow_html=True)
            if pode("metas_edit"):
                if mc2.button("🗑️ Remover", key=f"del_meta_{mid}"):
                    qry("DELETE FROM metas WHERE id=%s AND empresa_id=%s", (mid, EMPRESA_ID)); st.rerun()

# ══════════════════════════════════════
# 13. RELATÓRIOS (DRE, lucratividade)
# ══════════════════════════════════════
elif menu == "Relatórios":
    page_header("📈", "Relatórios", "DRE, lucratividade e análises")
    tab_dre, tab_lucro, tab_rank = st.tabs(["DRE Simplificado", "Lucratividade por Produto", "Ranking de Vendas"])

    with tab_dre:
        if not pode("dre"): show_warning("Acesso restrito a administradores."); st.stop()
        r1, r2 = st.columns(2)
        dre_ini = r1.date_input("Início", value=date.today().replace(day=1), key="dre_ini")
        dre_fim = r2.date_input("Fim", value=date.today(), key="dre_fim")
        di_dt = datetime.combine(dre_ini, datetime.min.time())
        df_dt = datetime.combine(dre_fim, datetime.max.time())

        rec = qry("SELECT COALESCE(SUM(valor_bruto),0) FROM vendas WHERE empresa_id=%s AND status='Pago' AND tipo='Venda' AND data>=%s AND data<=%s", (EMPRESA_ID, di_dt, df_dt), fetch=True)
        desc_v = qry("SELECT COALESCE(SUM(desconto_val),0) FROM vendas WHERE empresa_id=%s AND status='Pago' AND tipo='Venda' AND data>=%s AND data<=%s", (EMPRESA_ID, di_dt, df_dt), fetch=True)
        liq = qry("SELECT COALESCE(SUM(valor_total),0) FROM vendas WHERE empresa_id=%s AND status='Pago' AND tipo='Venda' AND data>=%s AND data<=%s", (EMPRESA_ID, di_dt, df_dt), fetch=True)
        cmv = qry("""SELECT COALESCE(SUM(i.quantidade * p.preco_custo),0)
            FROM itens_venda i JOIN produtos p ON p.nome=i.produto_nome AND p.empresa_id=i.empresa_id
            JOIN vendas v ON v.id=i.venda_id WHERE v.empresa_id=%s AND v.status='Pago' AND v.tipo='Venda' AND v.data>=%s AND v.data<=%s""",
            (EMPRESA_ID, di_dt, df_dt), fetch=True)
        desp_tot = qry("SELECT COALESCE(SUM(valor),0) FROM despesas WHERE empresa_id=%s AND status='Pago' AND data>=%s AND data<=%s", (EMPRESA_ID, dre_ini, dre_fim), fetch=True)

        vr = float(rec[0][0]) if rec else 0
        vd = float(desc_v[0][0]) if desc_v else 0
        vl = float(liq[0][0]) if liq else 0
        vc = float(cmv[0][0]) if cmv else 0
        vde = float(desp_tot[0][0]) if desp_tot else 0
        lucro_b = vl - vc
        lucro_op = lucro_b - vde
        margem_op = (lucro_op / vl * 100) if vl > 0 else 0

        st.markdown(f"""
        <div class="dre-row receita"><span>Receita Bruta</span><strong>{cur_sym} {vr:,.2f}</strong></div>
        <div class="dre-row desconto"><span>(-) Descontos</span><strong>- {cur_sym} {vd:,.2f}</strong></div>
        <div class="dre-row total"><span>Receita Líquida</span><strong>{cur_sym} {vl:,.2f}</strong></div>
        <div class="dre-row cmv"><span>(-) CMV (Custo das Mercadorias)</span><strong>- {cur_sym} {vc:,.2f}</strong></div>
        <div class="dre-row lucro"><span>Lucro Bruto</span><strong>{cur_sym} {lucro_b:,.2f}</strong></div>
        <div class="dre-row despesa"><span>(-) Despesas Operacionais</span><strong>- {cur_sym} {vde:,.2f}</strong></div>
        <div class="dre-row total"><span>Lucro Operacional &nbsp;<small>(margem: {margem_op:.1f}%)</small></span><strong>{cur_sym} {lucro_op:,.2f}</strong></div>
        """, unsafe_allow_html=True)

    with tab_lucro:
        r3, r4 = st.columns(2)
        luc_ini = r3.date_input("Início", value=date.today().replace(day=1), key="luc_ini")
        luc_fim = r4.date_input("Fim", value=date.today(), key="luc_fim")
        di_l = datetime.combine(luc_ini, datetime.min.time())
        df_l = datetime.combine(luc_fim, datetime.max.time())

        lucros = qry("""
            SELECT i.produto_nome, SUM(i.quantidade) as qtd,
                   SUM(i.quantidade * i.preco_unit) as receita,
                   SUM(i.quantidade * p.preco_custo) as custo,
                   SUM(i.quantidade * i.preco_unit) - SUM(i.quantidade * p.preco_custo) as lucro
            FROM itens_venda i
            JOIN produtos p ON p.nome=i.produto_nome AND p.empresa_id=i.empresa_id
            JOIN vendas v ON v.id=i.venda_id
            WHERE v.empresa_id=%s AND v.status='Pago' AND v.tipo='Venda' AND v.data>=%s AND v.data<=%s
            GROUP BY i.produto_nome ORDER BY lucro DESC
        """, (EMPRESA_ID, di_l, df_l), fetch=True)

        if not lucros: show_info("Nenhum dado no período.")
        else:
            if HAS_PANDAS:
                xb = to_excel(lucros, ["Produto","Qtd","Receita","Custo","Lucro"])
                if xb: st.download_button("⬇️ Exportar Excel", data=xb, file_name="lucratividade.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            for pnome, qtd, receita, custo, lucro in lucros:
                margem = (float(lucro) / float(receita) * 100) if float(receita) > 0 else 0
                cor = "ok" if margem > 30 else ("low" if margem > 10 else "zero")
                st.markdown(f'<div class="card"><div style="display:flex;justify-content:space-between;align-items:center"><div><div class="card-title">{pnome}</div><div class="card-sub">Qtd: {qtd} · Receita: {cur_sym} {float(receita):,.2f} · Custo: {cur_sym} {float(custo):,.2f}</div></div><div><div class="card-val">{cur_sym} {float(lucro):,.2f}</div><div class="card-sub" style="text-align:right"><span class="badge {cor}">Margem {margem:.1f}%</span></div></div></div></div>', unsafe_allow_html=True)

    with tab_rank:
        st.markdown("**Top clientes por faturamento**")
        top_cli = qry("SELECT cliente_name,COUNT(*) as pedidos,SUM(valor_total) as total FROM vendas WHERE empresa_id=%s AND status='Pago' AND tipo='Venda' GROUP BY cliente_name ORDER BY total DESC LIMIT 10", (EMPRESA_ID,), fetch=True)
        if top_cli:
            for i, (cli, ped, tot) in enumerate(top_cli, 1):
                st.markdown(f'<div class="card"><div style="display:flex;justify-content:space-between"><div><span class="badge low">#{i}</span> <strong>{cli}</strong><div class="card-sub">{ped} pedido(s)</div></div><div class="card-val">{cur_sym} {float(tot):,.2f}</div></div></div>', unsafe_allow_html=True)

        hr(); st.markdown("**Top produtos mais vendidos**")
        top_prod = qry("SELECT produto_nome,SUM(quantidade) as qtd,SUM(quantidade*preco_unit) as total FROM itens_venda WHERE empresa_id=%s GROUP BY produto_nome ORDER BY qtd DESC LIMIT 10", (EMPRESA_ID,), fetch=True)
        if top_prod:
            for i, (pn, qtd, tot) in enumerate(top_prod, 1):
                st.markdown(f'<div class="card"><div style="display:flex;justify-content:space-between"><div><span class="badge ok">#{i}</span> <strong>{pn}</strong><div class="card-sub">{qtd} unidades</div></div><div class="card-val">{cur_sym} {float(tot):,.2f}</div></div></div>', unsafe_allow_html=True)

# ══════════════════════════════════════
# 14. COMISSÕES
# ══════════════════════════════════════
elif menu == "Comissões":
    page_header("🏆", "Comissões", "Gerencie comissões de supervisores e representantes")
    c1, c2 = st.columns(2)
    com_ini = c1.date_input("Início", value=date.today().replace(day=1), key="com_ini")
    com_fim = c2.date_input("Fim", value=date.today(), key="com_fim")
    di_c = datetime.combine(com_ini, datetime.min.time())
    df_c = datetime.combine(com_fim, datetime.max.time())

    tab_sup, tab_rep = st.tabs(["Supervisores", "Representantes"])

    with tab_sup:
        sups_com = qry("""
            SELECT s.nome,s.comissao_pct,COUNT(v.id),SUM(v.valor_total),SUM(v.comissao_supervisor),v.comissao_status
            FROM supervisores s LEFT JOIN vendas v ON v.supervisor_id=s.id AND v.status='Pago' AND v.data>=%s AND v.data<=%s
            WHERE s.empresa_id=%s GROUP BY s.nome,s.comissao_pct,v.comissao_status ORDER BY s.nome
        """, (di_c, df_c, EMPRESA_ID), fetch=True)
        for row_s in (sups_com or []):
            snome, spct, sped, stot, scom, sstatus = row_s
            stot = float(stot or 0); scom = float(scom or 0)
            sb = f'<span class="badge {"pago" if sstatus == "Pago" else "pendente"}">{sstatus or "Pendente"}</span>'
            st.markdown(f'<div class="card"><div style="display:flex;justify-content:space-between"><div><div class="card-title">{snome} {sb}</div><div class="card-sub">Taxa: {spct:.1f}% · {sped or 0} pedido(s) · Faturou: {cur_sym} {stot:,.2f}</div></div><div class="card-val">Com. {cur_sym} {scom:,.2f}</div></div></div>', unsafe_allow_html=True)

    with tab_rep:
        reps_com = qry("""
            SELECT r.nome,r.comissao_pct,r.regiao,COUNT(v.id),SUM(v.valor_total),SUM(v.comissao_representante),v.comissao_status
            FROM representantes r LEFT JOIN vendas v ON v.representante_id=r.id AND v.status='Pago' AND v.data>=%s AND v.data<=%s
            WHERE r.empresa_id=%s GROUP BY r.nome,r.comissao_pct,r.regiao,v.comissao_status ORDER BY r.nome
        """, (di_c, df_c, EMPRESA_ID), fetch=True)
        for row_r in (reps_com or []):
            rnome, rpct, rregiao, rped, rtot, rcom, rstatus = row_r
            rtot = float(rtot or 0); rcom = float(rcom or 0)
            sb = f'<span class="badge {"pago" if rstatus == "Pago" else "pendente"}">{rstatus or "Pendente"}</span>'
            st.markdown(f'<div class="card"><div style="display:flex;justify-content:space-between"><div><div class="card-title">{rnome} {sb}</div><div class="card-sub">Taxa: {rpct:.1f}% · Região: {rregiao or "—"} · {rped or 0} pedido(s) · Faturou: {cur_sym} {rtot:,.2f}</div></div><div class="card-val">Com. {cur_sym} {rcom:,.2f}</div></div></div>', unsafe_allow_html=True)

    if pode("comissoes_pagar"):
        hr()
        if st.button("✅ Marcar todas as comissões do período como Pagas"):
            qry("UPDATE vendas SET comissao_status='Pago' WHERE empresa_id=%s AND status='Pago' AND data>=%s AND data<=%s", (EMPRESA_ID, di_c, df_c))
            show_success("Comissões marcadas como pagas!"); st.rerun()

# ══════════════════════════════════════
# 15. NOTIFICAÇÕES
# ══════════════════════════════════════
elif menu == "Notificações":
    page_header("🔔", "Notificações", f"{NOTIF_COUNT} não lida(s)")
    if st.button("✅ Marcar todas como lidas"):
        qry("UPDATE notificacoes SET lida=TRUE WHERE empresa_id=%s", (EMPRESA_ID,)); st.rerun()
    notifs = qry("SELECT id,titulo,mensagem,tipo,lida,criado_em FROM notificacoes WHERE empresa_id=%s ORDER BY criado_em DESC LIMIT 50", (EMPRESA_ID,), fetch=True)
    if not notifs: show_info("Nenhuma notificação.")
    else:
        for nid, ntit, nmsg, ntipo, nlida, ncriado in notifs:
            data_n = ncriado.strftime("%d/%m/%Y %H:%M") if ncriado else "—"
            op = 0.6 if nlida else 1.0
            st.markdown(f'<div class="notif-item {ntipo}" style="opacity:{op}"><strong>{"🔔" if not nlida else "✓"} {ntit}</strong><div style="font-size:.8rem;color:#555">{nmsg or ""}</div><div style="font-size:.7rem;color:#999">{data_n}</div></div>', unsafe_allow_html=True)
            if not nlida:
                if st.button("Marcar lida", key=f"lida_{nid}"):
                    qry("UPDATE notificacoes SET lida=TRUE WHERE id=%s AND empresa_id=%s", (nid, EMPRESA_ID)); st.rerun()

# ══════════════════════════════════════
# 16. SUPERVISORES
# ══════════════════════════════════════
elif menu == "Supervisores":
    page_header("👔", "Supervisores", "Gerencie a equipe de supervisores")
    with st.expander("➕  Novo supervisor"):
        with st.form("f_sup"):
            s1, s2 = st.columns(2); sn = s1.text_input("Nome *"); se = s2.text_input("E-mail")
            s3, s4 = st.columns(2); st2 = s3.text_input("Telefone"); scom = s4.number_input("Comissão (%)", min_value=0.0, max_value=100.0, step=0.5, format="%.2f")
            sv = st.form_submit_button("💾  Salvar", use_container_width=True)
        if sv:
            if not validate_required(sn): show_error("Nome é obrigatório.")
            else:
                res = qry("INSERT INTO supervisores(empresa_id,nome,email,telefone,comissao_pct) VALUES(%s,%s,%s,%s,%s)", (EMPRESA_ID, sn.strip(), se, st2, scom))
                if res is True: show_success("Supervisor cadastrado!", f"'{sn}' adicionado."); st.rerun()
                elif res == "duplicate": show_error("Já existe um supervisor com esse nome.")
                else: show_error("Não foi possível salvar.")

    hr()
    if HAS_PANDAS:
        sups_exp = qry("SELECT id,nome,email,telefone,comissao_pct,ativo FROM supervisores WHERE empresa_id=%s ORDER BY nome", (EMPRESA_ID,), fetch=True)
        if sups_exp:
            xb = to_excel(sups_exp, ["ID","Nome","Email","Telefone","Comissão%","Ativo"])
            if xb: st.download_button("⬇️ Exportar Excel", data=xb, file_name="supervisores.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    bs = st.text_input("🔍  Buscar supervisor", placeholder="Nome…", key="busca_sup")
    sups = qry("SELECT id,nome,email,telefone,comissao_pct,ativo FROM supervisores WHERE empresa_id=%s ORDER BY nome", (EMPRESA_ID,), fetch=True)
    if sups:
        if bs: b = bs.lower(); sups = [s for s in sups if b in (s[1] or "").lower()]
        st.markdown(f"**{len(sups)} supervisor(es)**")
        for sid, sn2, se2, st3, scom2, sa in sups:
            sb = '<span class="badge ativo">Ativo</span>' if sa else '<span class="badge inativo">Inativo</span>'
            if st.session_state.edit_sup_id == sid:
                with st.form(f"f_edit_sup_{sid}"):
                    es1, es2 = st.columns(2); en = es1.text_input("Nome *", value=sn2 or ""); em = es2.text_input("E-mail", value=se2 or "")
                    es3, es4 = st.columns(2); et = es3.text_input("Telefone", value=st3 or ""); ecom = es4.number_input("Comissão (%)", value=float(scom2 or 0), min_value=0.0, max_value=100.0, step=0.5, format="%.2f")
                    cs2, cc2 = st.columns(2); sv2 = cs2.form_submit_button("💾  Salvar", use_container_width=True); cn2 = cc2.form_submit_button("✕  Cancelar", use_container_width=True)
                if cn2: st.session_state.edit_sup_id = None; st.rerun()
                if sv2:
                    if not validate_required(en): show_error("Nome é obrigatório.")
                    else:
                        res = qry("UPDATE supervisores SET nome=%s,email=%s,telefone=%s,comissao_pct=%s WHERE id=%s AND empresa_id=%s", (en.strip(), em, et, ecom, sid, EMPRESA_ID))
                        if res is True: show_success("Supervisor atualizado!"); st.session_state.edit_sup_id = None; st.rerun()
                        elif res == "duplicate": show_error("Já existe um supervisor com esse nome.")
                        else: show_error("Não foi possível salvar.")
            else:
                ci2, ce2, ct2 = st.columns([8, 1, 1])
                ci2.markdown(f'<div class="card"><div class="card-title">{sn2} &nbsp;{sb}</div><div class="card-sub">{se2 or "—"} · {st3 or "—"} · Comissão: {float(scom2 or 0):.1f}%</div></div>', unsafe_allow_html=True)
                if ce2.button("✏️", key=f"edit_sup_{sid}", help="Editar"): st.session_state.edit_sup_id = sid; st.rerun()
                if ct2.button("🔴" if sa else "🟢", key=f"tog_sup_{sid}", help="Inativar" if sa else "Ativar"):
                    qry("UPDATE supervisores SET ativo=%s WHERE id=%s AND empresa_id=%s", (not sa, sid, EMPRESA_ID)); st.rerun()
            st.markdown('<div style="border-top:.5px solid #eef0ff;margin:4px 0"></div>', unsafe_allow_html=True)
    else: show_info("Nenhum supervisor cadastrado.", "Adicione um acima.")

# ══════════════════════════════════════
# 17. REPRESENTANTES
# ══════════════════════════════════════
elif menu == "Representantes":
    page_header("🤝", "Representantes", "Gerencie a equipe de representantes")
    sups_opts = qry("SELECT id,nome FROM supervisores WHERE empresa_id=%s AND ativo=TRUE ORDER BY nome", (EMPRESA_ID,), fetch=True)
    with st.expander("➕  Novo representante"):
        with st.form("f_rep"):
            r1, r2 = st.columns(2); rn = r1.text_input("Nome *"); re2 = r2.text_input("E-mail")
            r3, r4, r5 = st.columns(3); rt = r3.text_input("Telefone"); rcom = r4.number_input("Comissão (%)", min_value=0.0, max_value=100.0, step=0.5, format="%.2f"); rreg = r5.text_input("Região")
            sl = ["(nenhum)"] + [s[1] for s in sups_opts]; rs = st.selectbox("Supervisor responsável", sl)
            sv = st.form_submit_button("💾  Salvar", use_container_width=True)
        if sv:
            if not validate_required(rn): show_error("Nome é obrigatório.")
            else:
                si = next((s[0] for s in sups_opts if s[1] == rs), None)
                res = qry("INSERT INTO representantes(empresa_id,nome,email,telefone,supervisor_id,comissao_pct,regiao) VALUES(%s,%s,%s,%s,%s,%s,%s)",
                          (EMPRESA_ID, rn.strip(), re2, rt, si, rcom, rreg))
                if res is True: show_success("Representante cadastrado!", f"'{rn}' adicionado."); st.rerun()
                elif res == "duplicate": show_error("Já existe um representante com esse nome.")
                else: show_error("Não foi possível salvar.")

    hr()
    if HAS_PANDAS:
        reps_exp = qry("SELECT r.id,r.nome,r.email,r.telefone,r.comissao_pct,r.regiao,r.ativo,COALESCE(s.nome,'—') FROM representantes r LEFT JOIN supervisores s ON s.id=r.supervisor_id WHERE r.empresa_id=%s ORDER BY r.nome", (EMPRESA_ID,), fetch=True)
        if reps_exp:
            xb = to_excel(reps_exp, ["ID","Nome","Email","Telefone","Comissão%","Região","Ativo","Supervisor"])
            if xb: st.download_button("⬇️ Exportar Excel", data=xb, file_name="representantes.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    br = st.text_input("🔍  Buscar representante", placeholder="Nome ou região…", key="busca_rep")
    reps = qry("SELECT r.id,r.nome,r.email,r.telefone,r.ativo,COALESCE(s.nome,'—'),r.comissao_pct,r.regiao FROM representantes r LEFT JOIN supervisores s ON s.id=r.supervisor_id WHERE r.empresa_id=%s ORDER BY r.nome", (EMPRESA_ID,), fetch=True)
    if reps:
        if br: b = br.lower(); reps = [r for r in reps if b in (r[1] or "").lower() or b in (r[7] or "").lower()]
        st.markdown(f"**{len(reps)} representante(s)**")
        for rid, rn2, re3, rt2, ra, rsn, rcom2, rreg2 in reps:
            sb = '<span class="badge ativo">Ativo</span>' if ra else '<span class="badge inativo">Inativo</span>'
            if st.session_state.edit_rep_id == rid:
                with st.form(f"f_edit_rep_{rid}"):
                    er1, er2 = st.columns(2); en = er1.text_input("Nome *", value=rn2 or ""); em = er2.text_input("E-mail", value=re3 or "")
                    er3, er4, er5 = st.columns(3); et = er3.text_input("Telefone", value=rt2 or ""); ecom = er4.number_input("Comissão (%)", value=float(rcom2 or 0), min_value=0.0, max_value=100.0, step=0.5, format="%.2f"); ereg = er5.text_input("Região", value=rreg2 or "")
                    sl2 = ["(nenhum)"] + [s[1] for s in sups_opts]
                    ci2 = sl2.index(rsn) if rsn in sl2 else 0; es = st.selectbox("Supervisor", sl2, index=ci2)
                    cs2, cc2 = st.columns(2); sv2 = cs2.form_submit_button("💾  Salvar", use_container_width=True); cn2 = cc2.form_submit_button("✕  Cancelar", use_container_width=True)
                if cn2: st.session_state.edit_rep_id = None; st.rerun()
                if sv2:
                    if not validate_required(en): show_error("Nome é obrigatório.")
                    else:
                        si2 = next((s[0] for s in sups_opts if s[1] == es), None)
                        res = qry("UPDATE representantes SET nome=%s,email=%s,telefone=%s,supervisor_id=%s,comissao_pct=%s,regiao=%s WHERE id=%s AND empresa_id=%s",
                                  (en.strip(), em, et, si2, ecom, ereg, rid, EMPRESA_ID))
                        if res is True: show_success("Representante atualizado!"); st.session_state.edit_rep_id = None; st.rerun()
                        elif res == "duplicate": show_error("Já existe um representante com esse nome.")
                        else: show_error("Não foi possível salvar.")
            else:
                ci2, ce2, ct2 = st.columns([8, 1, 1])
                ci2.markdown(f'<div class="card"><div class="card-title">{rn2} &nbsp;{sb}</div><div class="card-sub">{re3 or "—"} · {rt2 or "—"} · Região: {rreg2 or "—"} · Comissão: {float(rcom2 or 0):.1f}% · Sup: {rsn}</div></div>', unsafe_allow_html=True)
                if ce2.button("✏️", key=f"edit_rep_{rid}", help="Editar"): st.session_state.edit_rep_id = rid; st.rerun()
                if ct2.button("🔴" if ra else "🟢", key=f"tog_rep_{rid}", help="Inativar" if ra else "Ativar"):
                    qry("UPDATE representantes SET ativo=%s WHERE id=%s AND empresa_id=%s", (not ra, rid, EMPRESA_ID)); st.rerun()
            st.markdown('<div style="border-top:.5px solid #eef0ff;margin:4px 0"></div>', unsafe_allow_html=True)
    else: show_info("Nenhum representante cadastrado.", "Adicione um acima.")

# ══════════════════════════════════════
# 18. GRUPOS DE CLIENTES
# ══════════════════════════════════════
elif menu == "Grupos de Clientes":
    page_header("👥", "Grupos de Clientes", "Segmente sua base de clientes")
    with st.expander("➕  Novo grupo"):
        with st.form("f_grp"):
            g1, g2 = st.columns(2); gn = g1.text_input("Nome *"); gd = g2.number_input("Desconto padrão (%)", min_value=0.0, max_value=100.0, step=0.5, format="%.2f")
            sv = st.form_submit_button("💾 Salvar", use_container_width=True)
        if sv:
            if not validate_required(gn): show_error("Nome é obrigatório.")
            else:
                res = qry("INSERT INTO grupos_clientes(empresa_id,nome,desconto_padrao) VALUES(%s,%s,%s)", (EMPRESA_ID, gn.strip(), gd))
                if res is True: show_success("Grupo criado!", f"'{gn}'"); st.rerun()
                elif res == "duplicate": show_error("Já existe um grupo com esse nome.")
                else: show_error("Não foi possível salvar.")

    hr()
    grps = qry("SELECT id,nome,desconto_padrao,ativo FROM grupos_clientes WHERE empresa_id=%s ORDER BY nome", (EMPRESA_ID,), fetch=True)
    if grps:
        for gid, gnome, gdesc, gativo in grps:
            sb = '<span class="badge ativo">Ativo</span>' if gativo else '<span class="badge inativo">Inativo</span>'
            if st.session_state.edit_grupo_id == gid:
                with st.form(f"f_edit_grp_{gid}"):
                    eg1, eg2 = st.columns(2); en = eg1.text_input("Nome *", value=gnome); ed = eg2.number_input("Desconto (%)", value=float(gdesc or 0), min_value=0.0, max_value=100.0, step=0.5, format="%.2f")
                    cs, cc = st.columns(2); sv2 = cs.form_submit_button("💾 Salvar", use_container_width=True); cn2 = cc.form_submit_button("✕ Cancelar", use_container_width=True)
                if cn2: st.session_state.edit_grupo_id = None; st.rerun()
                if sv2:
                    qry("UPDATE grupos_clientes SET nome=%s,desconto_padrao=%s WHERE id=%s AND empresa_id=%s", (en.strip(), ed, gid, EMPRESA_ID))
                    show_success("Grupo atualizado!"); st.session_state.edit_grupo_id = None; st.rerun()
            else:
                gi2, ge2, gt2 = st.columns([7, 1, 1])
                gi2.markdown(f'<div class="aux-row"><span class="ar-name">👥 {gnome} · Desconto: {float(gdesc or 0):.1f}%</span>&nbsp;{sb}</div>', unsafe_allow_html=True)
                if ge2.button("✏️", key=f"edit_grp_{gid}"): st.session_state.edit_grupo_id = gid; st.rerun()
                if gt2.button("🔴" if gativo else "🟢", key=f"tog_grp_{gid}"):
                    qry("UPDATE grupos_clientes SET ativo=%s WHERE id=%s AND empresa_id=%s", (not gativo, gid, EMPRESA_ID)); st.rerun()
    else: show_info("Nenhum grupo cadastrado.")

# ══════════════════════════════════════
# 19. CATEGORIAS
# ══════════════════════════════════════
elif menu == "Categorias":
    page_header("🏷️", "Categorias", "Organize seus produtos por categoria")
    c1, c2 = st.columns([3, 1]); nc = c1.text_input("Nova categoria", label_visibility="collapsed", placeholder="Ex: Eletrônicos, Alimentação…")
    if c2.button("➕  Adicionar", use_container_width=True):
        if not validate_required(nc): show_error("Digite o nome da categoria.")
        else:
            res = qry("INSERT INTO categorias(empresa_id,nome) VALUES(%s,%s)", (EMPRESA_ID, nc.strip()))
            if res is True: show_success("Categoria adicionada!", f"'{nc}' criada."); st.rerun()
            elif res == "duplicate": show_error("Essa categoria já está cadastrada.")
            else: show_error("Não foi possível salvar.")
    bc = st.text_input("🔍  Buscar categoria", placeholder="Nome…", key="busca_cat")
    cats = qry("SELECT id,nome,ativo FROM categorias WHERE empresa_id=%s ORDER BY nome", (EMPRESA_ID,), fetch=True)
    if cats:
        if bc: b = bc.lower(); cats = [c for c in cats if b in (c[1] or "").lower()]
        hr(); st.markdown(f"**{len(cats)} categoria(s)**")
        for cid, cnome, cativo in cats:
            sb = '<span class="badge ativo">Ativo</span>' if cativo else '<span class="badge inativo">Inativo</span>'
            ic = "" if cativo else " inativo"
            if st.session_state.edit_cat_id == cid:
                with st.form(f"f_edit_cat_{cid}"):
                    nn = st.text_input("Nome *", value=cnome); cs2, cc2 = st.columns(2)
                    sv = cs2.form_submit_button("💾  Salvar", use_container_width=True); cn2 = cc2.form_submit_button("✕  Cancelar", use_container_width=True)
                if cn2: st.session_state.edit_cat_id = None; st.rerun()
                if sv:
                    if not validate_required(nn): show_error("O nome não pode estar em branco.")
                    else:
                        res = qry("UPDATE categorias SET nome=%s WHERE id=%s AND empresa_id=%s", (nn.strip(), cid, EMPRESA_ID))
                        if res is True:
                            qry("UPDATE produtos SET categoria=%s WHERE categoria=%s AND empresa_id=%s", (nn.strip(), cnome, EMPRESA_ID))
                            show_success("Categoria atualizada!"); st.session_state.edit_cat_id = None; st.rerun()
                        elif res == "duplicate": show_error("Já existe uma categoria com esse nome.")
                        else: show_error("Não foi possível salvar.")
            else:
                cn2, ce2, ct2 = st.columns([7, 1, 1])
                cn2.markdown(f'<div class="aux-row{ic}"><span class="ar-name">🏷️ {cnome}</span>&nbsp;{sb}</div>', unsafe_allow_html=True)
                if ce2.button("✏️", key=f"edit_cat_{cid}", help="Editar"): st.session_state.edit_cat_id = cid; st.rerun()
                if ct2.button("🔴" if cativo else "🟢", key=f"tog_cat_{cid}", help="Inativar" if cativo else "Ativar"):
                    qry("UPDATE categorias SET ativo=%s WHERE id=%s AND empresa_id=%s", (not cativo, cid, EMPRESA_ID)); st.rerun()
    else: show_info("Nenhuma categoria cadastrada.", "Adicione uma acima.")

# ══════════════════════════════════════
# 20. FORMAS DE PAGAMENTO
# ══════════════════════════════════════
elif menu == "Formas de Pagamento":
    page_header("💳", "Formas de Pagamento", "Gerencie as formas de pagamento aceitas")
    c1, c2 = st.columns([3, 1]); np2 = c1.text_input("Nova forma de pagamento", label_visibility="collapsed", placeholder="Ex: Dinheiro, Pix, Cartão…")
    if c2.button("➕  Adicionar", use_container_width=True):
        if not validate_required(np2): show_error("Digite o nome da forma de pagamento.")
        else:
            res = qry("INSERT INTO pagamentos(empresa_id,nome) VALUES(%s,%s)", (EMPRESA_ID, np2.strip()))
            if res is True: show_success("Forma de pagamento adicionada!", f"'{np2}' criada."); st.rerun()
            elif res == "duplicate": show_error("Essa forma de pagamento já está cadastrada.")
            else: show_error("Não foi possível salvar.")
    bp = st.text_input("🔍  Buscar forma de pagamento", placeholder="Nome…", key="busca_pag")
    pags = qry("SELECT id,nome,ativo FROM pagamentos WHERE empresa_id=%s ORDER BY nome", (EMPRESA_ID,), fetch=True)
    if pags:
        if bp: b = bp.lower(); pags = [p for p in pags if b in (p[1] or "").lower()]
        hr(); st.markdown(f"**{len(pags)} forma(s) de pagamento**")
        for pid, pnome, pativo in pags:
            sb = '<span class="badge ativo">Ativo</span>' if pativo else '<span class="badge inativo">Inativo</span>'
            ic = "" if pativo else " inativo"
            if st.session_state.edit_pag_id == pid:
                with st.form(f"f_edit_pag_{pid}"):
                    nn = st.text_input("Nome *", value=pnome); cs3, cc3 = st.columns(2)
                    sv3 = cs3.form_submit_button("💾  Salvar", use_container_width=True); cn3 = cc3.form_submit_button("✕  Cancelar", use_container_width=True)
                if cn3: st.session_state.edit_pag_id = None; st.rerun()
                if sv3:
                    if not validate_required(nn): show_error("O nome não pode estar em branco.")
                    else:
                        res = qry("UPDATE pagamentos SET nome=%s WHERE id=%s AND empresa_id=%s", (nn.strip(), pid, EMPRESA_ID))
                        if res is True: show_success("Forma de pagamento atualizada!"); st.session_state.edit_pag_id = None; st.rerun()
                        elif res == "duplicate": show_error("Já existe uma forma de pagamento com esse nome.")
                        else: show_error("Não foi possível salvar.")
            else:
                pn2, pe2, pt2 = st.columns([7, 1, 1])
                pn2.markdown(f'<div class="aux-row{ic}"><span class="ar-name">💳 {pnome}</span>&nbsp;{sb}</div>', unsafe_allow_html=True)
                if pe2.button("✏️", key=f"edit_pag_{pid}", help="Editar"): st.session_state.edit_pag_id = pid; st.rerun()
                if pt2.button("🔴" if pativo else "🟢", key=f"tog_pag_{pid}", help="Inativar" if pativo else "Ativar"):
                    qry("UPDATE pagamentos SET ativo=%s WHERE id=%s AND empresa_id=%s", (not pativo, pid, EMPRESA_ID)); st.rerun()
    else: show_info("Nenhuma forma de pagamento cadastrada.", "Adicione uma acima.")

# ══════════════════════════════════════
# 21. LOG DE AÇÕES
# ══════════════════════════════════════
elif menu == "Log de Ações":
    if not pode("log"): show_error("Acesso restrito a administradores."); st.stop()
    page_header("📋", "Log de Ações", "Auditoria de todas as operações")
    l1, l2 = st.columns(2)
    log_ini = l1.date_input("Início", value=date.today() - timedelta(days=7), key="log_ini")
    log_usr = l2.text_input("Usuário", placeholder="Nome…", key="log_usr")
    log_q = "SELECT usuario_nome,acao,detalhes,TO_CHAR(criado_em,'DD/MM/YYYY HH24:MI') FROM log_acoes WHERE empresa_id=%s AND criado_em>=%s"
    log_p = [EMPRESA_ID, datetime.combine(log_ini, datetime.min.time())]
    if log_usr: log_q += " AND usuario_nome ILIKE %s"; log_p.append(f"%{log_usr}%")
    log_q += " ORDER BY criado_em DESC LIMIT 200"
    logs = qry(log_q, tuple(log_p), fetch=True)
    if logs:
        if HAS_PANDAS:
            xb = to_excel(logs, ["Usuário","Ação","Detalhes","Data"])
            if xb: st.download_button("⬇️ Exportar Log", data=xb, file_name="log_acoes.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        st.markdown(f"**{len(logs)} registro(s)**")
        for usr, acao, det, data_l in logs:
            st.markdown(f'<div class="card"><div class="card-sku">{data_l} · {usr or "—"}</div><div class="card-title">{acao}</div><div class="card-sub">{det or ""}</div></div>', unsafe_allow_html=True)
    else: show_info("Nenhum registro no período.")

# ══════════════════════════════════════
# 22. CONFIGURAÇÕES
# ══════════════════════════════════════
elif menu == "Configurações":
    page_header("⚙️", "Configurações", "Personalize o sistema")
    tabs_cfg = st.tabs(["Empresa", "Tabelas de Preço", "Redefinir Senha", "Usuários"])

    with tabs_cfg[0]:
        er = qry("SELECT nome,moeda FROM empresas WHERE id=%s", (EMPRESA_ID,), fetch=True)
        ena = er[0][0] if er else ""; ema = er[0][1] if er and er[0][1] else "R$"
        with st.form("f_config"):
            nn = st.text_input("Nome da Empresa *", value=ena)
            ms = ["R$", "$", "€", "£"]; im = ms.index(ema) if ema in ms else 0
            nc2 = st.selectbox("Moeda padrão", ms, index=im)
            saved = st.form_submit_button("💾  Salvar Configurações", use_container_width=True)
        if saved:
            if not validate_required(nn): show_error("O nome da empresa não pode estar em branco.")
            else:
                qry("UPDATE empresas SET nome=%s,moeda=%s WHERE id=%s", (nn.strip(), nc2, EMPRESA_ID))
                st.session_state.empresa_nome = nn.strip(); st.session_state.empresa_moeda = nc2
                show_success("Configurações salvas!", "As alterações já estão ativas."); st.rerun()

    with tabs_cfg[1]:
        st.markdown("#### Tabelas de Preço")
        with st.form("f_tabpreco"):
            tp1, tp2 = st.columns(2); tpn = tp1.text_input("Nome *"); tpd = tp2.number_input("Desconto (%)", min_value=0.0, max_value=100.0, step=0.5, format="%.2f")
            sv_tp = st.form_submit_button("➕ Adicionar tabela", use_container_width=True)
        if sv_tp:
            if not validate_required(tpn): show_error("Nome obrigatório.")
            else:
                res = qry("INSERT INTO tabelas_preco(empresa_id,nome,desconto_pct) VALUES(%s,%s,%s)", (EMPRESA_ID, tpn.strip(), tpd))
                if res is True: show_success("Tabela criada!"); st.rerun()
                elif res == "duplicate": show_error("Já existe uma tabela com esse nome.")
        tps = qry("SELECT id,nome,desconto_pct,ativo FROM tabelas_preco WHERE empresa_id=%s ORDER BY nome", (EMPRESA_ID,), fetch=True)
        for tpid, tpnome, tpdesc, tpativo in (tps or []):
            sb = '<span class="badge ativo">Ativo</span>' if tpativo else '<span class="badge inativo">Inativo</span>'
            tp_c, tp_t = st.columns([8, 1])
            tp_c.markdown(f'<div class="aux-row"><span class="ar-name">📋 {tpnome} — {float(tpdesc):.1f}% desconto &nbsp;{sb}</span></div>', unsafe_allow_html=True)
            if tp_t.button("🔴" if tpativo else "🟢", key=f"tog_tp_{tpid}"):
                qry("UPDATE tabelas_preco SET ativo=%s WHERE id=%s AND empresa_id=%s", (not tpativo, tpid, EMPRESA_ID)); st.rerun()

    with tabs_cfg[2]:
        st.markdown("#### Redefinir senha via token")
        with st.form("f_redef"):
            rd1, rd2 = st.columns(2); email_rd = rd1.text_input("E-mail *"); token_rd = rd2.text_input("Token *")
            nova_senha = st.text_input("Nova senha *", type="password")
            sv_rd = st.form_submit_button("Redefinir senha", use_container_width=True)
        if sv_rd:
            tok_row = run_query("SELECT id FROM recuperacao_senha WHERE email=%s AND token=%s AND expira_em>NOW() AND usado=FALSE",
                               (email_rd.strip().lower(), token_rd.strip()), fetch=True)
            if tok_row:
                run_query("UPDATE usuarios SET senha_hash=%s WHERE email=%s", (hash_senha(nova_senha), email_rd.strip().lower()))
                run_query("UPDATE recuperacao_senha SET usado=TRUE WHERE email=%s AND token=%s", (email_rd.strip().lower(), token_rd.strip()))
                show_success("Senha redefinida com sucesso!")
            else: show_error("Token inválido, expirado ou já utilizado.")

    with tabs_cfg[3]:
        if not st.session_state.usuario_perfil == "admin": show_warning("Acesso restrito a administradores.")
        else:
            with st.expander("➕  Novo usuário"):
                with st.form("f_novo_user"):
                    cu1, cu2 = st.columns(2); un = cu1.text_input("Nome *"); ue = cu2.text_input("E-mail *")
                    cu3, cu4 = st.columns(2); us = cu3.text_input("Senha *", type="password"); up = cu4.selectbox("Perfil", ["operador", "admin"])
                    au = st.form_submit_button("➕  Criar Usuário", use_container_width=True)
                if au:
                    if not validate_required(un, ue, us): show_error("Nome, e-mail e senha são obrigatórios.")
                    else:
                        res = qry("INSERT INTO usuarios(empresa_id,nome,email,senha_hash,perfil) VALUES(%s,%s,%s,%s,%s)",
                                  (EMPRESA_ID, un.strip(), ue.strip().lower(), hash_senha(us), up))
                        if res is True: show_success("Usuário criado!", f"'{un}' pode acessar o sistema."); st.rerun()
                        elif res == "duplicate": show_error("Já existe um usuário com esse e-mail.")
                        else: show_error("Não foi possível criar o usuário.")
            usuarios = qry("SELECT id,nome,email,perfil,ativo FROM usuarios WHERE empresa_id=%s ORDER BY nome", (EMPRESA_ID,), fetch=True)
            if usuarios:
                for uid2, un2, ue2, up2, ua2 in usuarios:
                    sb2 = '<span class="badge ativo">Ativo</span>' if ua2 else '<span class="badge inativo">Inativo</span>'
                    cu, cut = st.columns([9, 1])
                    cu.markdown(f'<div class="card"><div class="card-title">{un2} &nbsp;{sb2}</div><div class="card-sub">{ue2} · {up2}</div></div>', unsafe_allow_html=True)
                    if uid2 != st.session_state.usuario_id:
                        if cut.button("🔴" if ua2 else "🟢", key=f"tog_u_{uid2}", help="Inativar" if ua2 else "Ativar"):
                            qry("UPDATE usuarios SET ativo=%s WHERE id=%s AND empresa_id=%s", (not ua2, uid2, EMPRESA_ID)); st.rerun()