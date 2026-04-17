import streamlit as st
import sqlite3
import pandas as pd
import json
import os
import re
from datetime import datetime

# ─────────────────────────────────────────────
#  CONFIGURAÇÕES
# ─────────────────────────────────────────────
def load_config():
    if os.path.exists('config.json'):
        with open('config.json', 'r', encoding='utf-8') as f:
            return json.load(f)
    return {"nome_empresa": "Gestão ERP Pro", "logo_url": "", "currency": "R$"}

def save_config(cfg):
    with open('config.json', 'w', encoding='utf-8') as f:
        json.dump(cfg, f, ensure_ascii=False)

config = load_config()
cur    = config.get("currency", "R$")

st.set_page_config(page_title=config['nome_empresa'], layout="wide",
                   initial_sidebar_state="expanded")

# ─────────────────────────────────────────────
#  ESTILOS
# ─────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600&family=Sora:wght@400;600;700&display=swap');
html,body,[class*="css"]{font-family:'DM Sans',sans-serif;}

section[data-testid="stSidebar"]{background:linear-gradient(160deg,#0f0f1a 0%,#1a1a2e 60%,#16213e 100%);border-right:1px solid rgba(255,255,255,0.06);}
section[data-testid="stSidebar"] *{color:rgba(255,255,255,0.85)!important;}
.erp-brand{font-family:'Sora',sans-serif;font-size:1.15rem;font-weight:700;color:#fff!important;padding:0.25rem 0 1rem;}
.erp-brand span{color:#818cf8!important;}
.nav-section-title{font-size:0.65rem;font-weight:600;letter-spacing:.12em;text-transform:uppercase;color:rgba(255,255,255,0.35)!important;padding:.75rem 0 .35rem;}
.stButton>button{width:100%;text-align:left;padding:.55rem .9rem;border-radius:10px;border:none;background:transparent;color:rgba(255,255,255,0.75)!important;font-size:.9rem;font-weight:400;transition:all .18s ease;margin-bottom:2px;}
.stButton>button:hover{background:rgba(129,140,248,.12)!important;color:#fff!important;transform:translateX(2px);}
.nav-active button{background:rgba(99,102,241,.25)!important;color:#c7d2fe!important;font-weight:600;border-left:3px solid #818cf8;}

[data-testid="stMetric"]{background:#f8f9ff;border:1px solid #e8eaff;border-radius:14px;padding:1.1rem 1.25rem;}
[data-testid="stMetricValue"]{font-family:'Sora',sans-serif;font-size:1.6rem;}
[data-testid="stForm"]{background:#fafbff;border:1px solid #eef0ff;border-radius:14px;padding:1.5rem;}
[data-testid="stExpander"]{border:1px solid #eef0ff!important;border-radius:12px!important;}
[data-testid="stDataFrame"]{border-radius:12px;overflow:hidden;}

.erp-toast{padding:.75rem 1rem;border-radius:10px;font-size:.9rem;margin:.4rem 0;display:flex;align-items:flex-start;gap:.6rem;}
.erp-toast.success{background:#f0fdf4;border-left:4px solid #22c55e;color:#15803d;}
.erp-toast.error  {background:#fef2f2;border-left:4px solid #ef4444;color:#b91c1c;}
.erp-toast.warning{background:#fffbeb;border-left:4px solid #f59e0b;color:#b45309;}
.erp-toast.info   {background:#eff6ff;border-left:4px solid #3b82f6;color:#1d4ed8;}
.erp-toast .icon  {font-size:1.1rem;flex-shrink:0;margin-top:1px;}
.erp-toast .body strong{display:block;font-weight:600;margin-bottom:2px;}
.erp-toast .body span  {font-weight:400;color:inherit;opacity:.85;}

.page-header{display:flex;align-items:center;gap:.75rem;margin-bottom:1.5rem;}
.page-header .icon{width:40px;height:40px;border-radius:10px;display:flex;align-items:center;justify-content:center;font-size:1.2rem;background:linear-gradient(135deg,#6366f1,#818cf8);}
.page-header h1{font-family:'Sora',sans-serif;font-size:1.4rem;font-weight:700;margin:0;color:#1e1b4b;}
.page-header p{font-size:.85rem;color:#6b7280;margin:0;}
hr.erp{border:none;border-top:1px solid #eef0ff;margin:1.25rem 0;}

.badge{display:inline-block;padding:2px 8px;border-radius:99px;font-size:.75rem;font-weight:600;}
.badge.ok       {background:#dcfce7;color:#166534;}
.badge.low      {background:#fef9c3;color:#854d0e;}
.badge.zero     {background:#fee2e2;color:#991b1b;}
.badge.ativo    {background:#dcfce7;color:#166534;}
.badge.inativo  {background:#f1f5f9;color:#64748b;}
.badge.pago     {background:#dbeafe;color:#1e40af;}
.badge.cancelado{background:#fee2e2;color:#991b1b;}

.cart-item{display:flex;align-items:center;justify-content:space-between;padding:.55rem .75rem;border-radius:10px;margin-bottom:5px;background:#f8f9ff;border:1px solid #eef0ff;}
.ci-name{font-size:.88rem;font-weight:600;color:#1e1b4b;}
.ci-qty {font-size:.78rem;color:#6b7280;}
.ci-val {font-family:'Sora',sans-serif;font-size:.9rem;font-weight:700;color:#6366f1;}
.cart-total{display:flex;justify-content:space-between;align-items:center;padding:.75rem;border-radius:10px;background:#6366f1;color:#fff;margin-top:.75rem;}
.cart-total span{font-size:.9rem;opacity:.85;}
.cart-total strong{font-family:'Sora',sans-serif;font-size:1.2rem;}

.sale-row{display:flex;align-items:center;justify-content:space-between;padding:.6rem .75rem;border-radius:10px;margin-bottom:6px;background:#f8f9ff;border:1px solid #eef0ff;}
.sale-row .sr-left{display:flex;flex-direction:column;}
.sale-row .sr-id  {font-size:.72rem;color:#9ca3af;}
.sale-row .sr-name{font-size:.92rem;font-weight:600;color:#1e1b4b;}
.sale-row .sr-prod{font-size:.78rem;color:#6b7280;}
.sale-row .sr-right{text-align:right;}
.sale-row .sr-val {font-family:'Sora',sans-serif;font-size:1rem;font-weight:700;color:#6366f1;}
.sale-row .sr-date{font-size:.72rem;color:#9ca3af;}

.prod-card{display:flex;align-items:center;justify-content:space-between;padding:.65rem .9rem;border-radius:10px;margin-bottom:6px;background:#fafbff;border:1px solid #eef0ff;}
.prod-card.inativo{opacity:.5;border-style:dashed;}
.prod-card .pc-left{display:flex;flex-direction:column;}
.prod-card .pc-sku {font-size:.7rem;color:#9ca3af;}
.prod-card .pc-name{font-size:.93rem;font-weight:600;color:#1e1b4b;}
.prod-card .pc-cat {font-size:.77rem;color:#6b7280;}
.prod-card .pc-right{display:flex;align-items:center;gap:.75rem;}
.prod-card .pc-price{font-size:.9rem;font-weight:600;color:#1e1b4b;}

.aux-row{display:flex;align-items:center;justify-content:space-between;padding:.6rem .9rem;border-radius:10px;margin-bottom:5px;background:#fafbff;border:1px solid #eef0ff;}
.aux-row.inativo{opacity:.5;border-style:dashed;}
.aux-row .ar-name{font-size:.9rem;font-weight:500;color:#1e1b4b;}
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────
#  HELPERS UI
# ─────────────────────────────────────────────
def show_error(title, hint=""):
    st.markdown(f'<div class="erp-toast error"><div class="icon">✕</div><div class="body"><strong>{title}</strong><span>{hint}</span></div></div>', unsafe_allow_html=True)

def show_success(title, hint=""):
    st.markdown(f'<div class="erp-toast success"><div class="icon">✓</div><div class="body"><strong>{title}</strong><span>{hint}</span></div></div>', unsafe_allow_html=True)

def show_warning(title, hint=""):
    st.markdown(f'<div class="erp-toast warning"><div class="icon">⚠</div><div class="body"><strong>{title}</strong><span>{hint}</span></div></div>', unsafe_allow_html=True)

def show_info(title, hint=""):
    st.markdown(f'<div class="erp-toast info"><div class="icon">ℹ</div><div class="body"><strong>{title}</strong><span>{hint}</span></div></div>', unsafe_allow_html=True)

def page_header(icon, title, subtitle=""):
    sub = f"<p>{subtitle}</p>" if subtitle else ""
    st.markdown(f'<div class="page-header"><div class="icon">{icon}</div><div><h1>{title}</h1>{sub}</div></div>', unsafe_allow_html=True)

# ─────────────────────────────────────────────
#  BANCO DE DADOS
# ─────────────────────────────────────────────
DB = "database.db"

def run_query(query, params=(), fetch=False):
    try:
        with sqlite3.connect(DB) as conn:
            conn.execute("PRAGMA foreign_keys = ON")
            c = conn.cursor()
            c.execute(query, params)
            if fetch:
                return c.fetchall()
            conn.commit()
            return True
    except sqlite3.IntegrityError:
        return "duplicate"
    except Exception as e:
        st.session_state["_last_db_error"] = str(e)
        return False

def init_db():
    run_query("""CREATE TABLE IF NOT EXISTS categorias(
        id INTEGER PRIMARY KEY AUTOINCREMENT, nome TEXT UNIQUE NOT NULL, ativo INTEGER DEFAULT 1)""")
    run_query("""CREATE TABLE IF NOT EXISTS pagamentos(
        id INTEGER PRIMARY KEY AUTOINCREMENT, nome TEXT UNIQUE NOT NULL, ativo INTEGER DEFAULT 1)""")
    run_query("""CREATE TABLE IF NOT EXISTS clientes(
        id INTEGER PRIMARY KEY AUTOINCREMENT, nome TEXT NOT NULL, documento TEXT UNIQUE NOT NULL,
        telefone TEXT, email TEXT, rua TEXT, numero TEXT, complemento TEXT,
        bairro TEXT, cidade TEXT, estado TEXT, cep TEXT, ativo INTEGER DEFAULT 1)""")
    run_query("""CREATE TABLE IF NOT EXISTS produtos(
        id INTEGER PRIMARY KEY AUTOINCREMENT, sku TEXT UNIQUE NOT NULL, nome TEXT NOT NULL,
        categoria TEXT NOT NULL, preco_custo REAL DEFAULT 0, preco_venda REAL DEFAULT 0,
        estoque_atual INTEGER DEFAULT 0, estoque_minimo INTEGER DEFAULT 2, ativo INTEGER DEFAULT 1)""")
    run_query("""CREATE TABLE IF NOT EXISTS vendas(
        id INTEGER PRIMARY KEY AUTOINCREMENT, data TIMESTAMP NOT NULL, cliente_name TEXT,
        valor_total REAL, pagamento TEXT, status TEXT DEFAULT 'Pago', observacao TEXT DEFAULT '')""")
    run_query("""CREATE TABLE IF NOT EXISTS itens_venda(
        id INTEGER PRIMARY KEY AUTOINCREMENT, venda_id INTEGER, produto_nome TEXT,
        quantidade INTEGER, preco_unit REAL, FOREIGN KEY(venda_id) REFERENCES vendas(id))""")
    # Migrações seguras
    for tbl, col in [("clientes","ativo"),("produtos","ativo"),("categorias","ativo"),
                     ("pagamentos","ativo"),("clientes","rua"),("clientes","numero"),
                     ("clientes","complemento"),("clientes","bairro"),("clientes","cidade"),
                     ("clientes","estado"),("clientes","cep"),("vendas","observacao")]:
        cols = run_query(f"PRAGMA table_info({tbl})", fetch=True)
        if cols and col not in [c[1] for c in cols]:
            dflt = "1" if col == "ativo" else "''"
            run_query(f"ALTER TABLE {tbl} ADD COLUMN {col} TEXT DEFAULT {dflt}")

init_db()

# ─────────────────────────────────────────────
#  VALIDAÇÕES
# ─────────────────────────────────────────────
def validate_doc(doc):
    return len(re.sub(r'\D','',doc)) in (11,14)

def validate_required(*fields):
    return all(f is not None and str(f).strip() for f in fields)

def get_estoque(prod_id):
    r = run_query("SELECT estoque_atual FROM produtos WHERE id=?", (prod_id,), fetch=True)
    return r[0][0] if r else 0

# ─────────────────────────────────────────────
#  SESSION STATE
# ─────────────────────────────────────────────
defaults = {"active_menu":"Dashboard","cart":[],"editing_prod_id":None,
            "editing_prod_data":None,"adj_prod":None,"edit_cat_id":None,"edit_pag_id":None,
            "edit_cli_id":None,"edit_cli_data":None}
for k,v in defaults.items():
    if k not in st.session_state:
        st.session_state[k] = v

# ─────────────────────────────────────────────
#  SIDEBAR
# ─────────────────────────────────────────────
MENUS = [("📊","Dashboard","dash"),("🛒","Pedidos","pedidos"),("📦","Estoque","estoque"),
         ("👥","Clientes","clientes"),("📜","Histórico de Vendas","hist"),
         ("🏷️","Categorias","cats"),("💳","Formas de Pagamento","pags"),("⚙️","Configurações","config")]

with st.sidebar:
    parts = config['nome_empresa'].split()
    b1 = parts[0] if parts else "ERP"
    b2 = " ".join(parts[1:]) if len(parts)>1 else "Pro"
    st.markdown(f'<div class="erp-brand">{b1} <span>{b2}</span></div>', unsafe_allow_html=True)
    if config.get('logo_url'):
        st.image(config['logo_url'], use_column_width=True)
    st.markdown('<div class="nav-section-title">Menu principal</div>', unsafe_allow_html=True)
    for icon, label, key in MENUS:
        if key == "cats":
            st.markdown('<div class="nav-section-title">Tabelas auxiliares</div>', unsafe_allow_html=True)
        is_active = st.session_state.active_menu == label
        with st.container():
            if is_active:
                st.markdown('<div class="nav-active">', unsafe_allow_html=True)
            if st.button(f"{icon}  {label}", key=f"nav_{key}"):
                st.session_state.active_menu = label
                for k2 in ["editing_prod_id","editing_prod_data","adj_prod","edit_cat_id","edit_pag_id","edit_cli_id","edit_cli_data"]:
                    st.session_state[k2] = None
                st.rerun()
            if is_active:
                st.markdown('</div>', unsafe_allow_html=True)
    n_cart = len(st.session_state.cart)
    if n_cart:
        st.markdown(f'<div style="margin:8px 10px 0;padding:6px 10px;border-radius:8px;background:rgba(99,102,241,.2);color:#c7d2fe!important;font-size:.82rem;">🛒 {n_cart} item(s) no carrinho</div>', unsafe_allow_html=True)
    st.divider()
    st.markdown(f"<small style='color:rgba(255,255,255,0.3)'>v5.0 · {datetime.now().strftime('%d/%m/%Y')}</small>", unsafe_allow_html=True)

menu = st.session_state.active_menu


# ════════════════════════════════════════════════════════════
#  1. DASHBOARD
# ════════════════════════════════════════════════════════════
if menu == "Dashboard":
    page_header("📊","Dashboard","Visão geral do negócio")
    vendas_all = run_query("SELECT valor_total FROM vendas WHERE status='Pago'", fetch=True)
    total_fat = sum(v[0] for v in vendas_all) if vendas_all else 0
    qtd_ped   = len(vendas_all) if vendas_all else 0
    avg_tick  = (total_fat/qtd_ped) if qtd_ped else 0
    c1,c2,c3 = st.columns(3)
    c1.metric("Faturamento Total", f"{cur} {total_fat:,.2f}")
    c2.metric("Pedidos", qtd_ped)
    c3.metric("Ticket Médio", f"{cur} {avg_tick:,.2f}")
    st.markdown('<hr class="erp">', unsafe_allow_html=True)
    st.markdown("**Últimas vendas**")
    ultimas = run_query(
        """SELECT v.id,v.cliente_name,v.valor_total,
                  GROUP_CONCAT(i.produto_nome||' x'||i.quantidade,', '),
                  strftime('%d/%m/%Y %H:%M',v.data)
           FROM vendas v LEFT JOIN itens_venda i ON i.venda_id=v.id
           WHERE v.status='Pago' GROUP BY v.id ORDER BY v.data DESC LIMIT 10""", fetch=True)
    if ultimas:
        for row in ultimas:
            vid,cli,val,prods_str,data_fmt = row
            prods_str = prods_str or "—"
            st.markdown(f"""<div class="sale-row">
                <div class="sr-left"><span class="sr-id">Pedido #{vid}</span>
                <span class="sr-name">{cli}</span><span class="sr-prod">{prods_str}</span></div>
                <div class="sr-right"><div class="sr-val">{cur} {val:,.2f}</div>
                <div class="sr-date">{data_fmt}</div></div></div>""", unsafe_allow_html=True)
    else:
        show_info("Nenhuma venda registrada ainda.","Cadastre clientes e produtos para começar.")
    low = run_query("SELECT nome FROM produtos WHERE estoque_atual<=estoque_minimo AND ativo=1", fetch=True)
    if low:
        st.markdown('<hr class="erp">', unsafe_allow_html=True)
        show_warning(f"{len(low)} produto(s) com estoque baixo ou zerado","Verifique a aba Estoque.")


# ════════════════════════════════════════════════════════════
#  2. PEDIDOS — CARRINHO MULTI-PRODUTO
# ════════════════════════════════════════════════════════════
elif menu == "Pedidos":
    page_header("🛒","Pedidos","Monte o pedido com múltiplos produtos")

    clis  = run_query("SELECT nome FROM clientes WHERE ativo=1 ORDER BY nome", fetch=True)
    prods = run_query("SELECT id,nome,preco_venda,estoque_atual FROM produtos WHERE ativo=1 AND estoque_atual>0 ORDER BY nome", fetch=True)
    pags  = run_query("SELECT nome FROM pagamentos WHERE ativo=1 ORDER BY nome", fetch=True)

    if not clis:
        show_error("Nenhum cliente ativo cadastrado","Cadastre ou ative um cliente antes de vender."); st.stop()
    if not prods:
        show_error("Nenhum produto disponível em estoque","Cadastre produtos ou ajuste o estoque."); st.stop()
    if not pags:
        show_error("Nenhuma forma de pagamento ativa","Cadastre ou ative uma forma de pagamento."); st.stop()

    cart: list = st.session_state.cart
    col_form, col_cart = st.columns([3,2])

    # ── Carrinho (coluna direita) ──
    with col_cart:
        st.markdown("#### 🧺 Carrinho")
        if not cart:
            show_info("Carrinho vazio","Adicione produtos à esquerda.")
        else:
            total_cart = 0.0
            for i, item in enumerate(cart):
                sub = item["preco"] * item["qtd"]
                total_cart += sub
                c_item, c_rem = st.columns([5,1])
                with c_item:
                    st.markdown(f"""<div class="cart-item">
                        <div><div class="ci-name">{item['nome']}</div>
                        <div class="ci-qty">{item['qtd']} un × {cur} {item['preco']:.2f}</div></div>
                        <div class="ci-val">{cur} {sub:.2f}</div>
                    </div>""", unsafe_allow_html=True)
                with c_rem:
                    if st.button("🗑️", key=f"rem_{i}", help="Remover item"):
                        st.session_state.cart.pop(i); st.rerun()
            st.markdown(f"""<div class="cart-total">
                <span>Total do pedido</span>
                <strong>{cur} {total_cart:,.2f}</strong>
            </div>""", unsafe_allow_html=True)

    # ── Formulário: adicionar item (coluna esquerda) ──
    with col_form:
        prod_nomes = [p[1] for p in prods]
        with st.form("form_add_item", clear_on_submit=True):
            st.markdown("#### Adicionar produto ao carrinho")
            prod_idx = st.selectbox("Produto *", range(len(prod_nomes)),
                                    format_func=lambda i: prod_nomes[i])
            prod_sel   = prods[prod_idx]
            est_disp   = int(prod_sel[3])
            no_carrinho = sum(x["qtd"] for x in cart if x["id"] == prod_sel[0])
            disponivel  = max(0, est_disp - no_carrinho)
            st.caption(f"Disponível: **{est_disp}** &nbsp;·&nbsp; No carrinho: **{no_carrinho}** &nbsp;·&nbsp; Pode adicionar: **{disponivel}**")
            qtd_add = st.number_input("Quantidade *", min_value=1,
                max_value=disponivel if disponivel > 0 else 1, step=1, value=1,
                disabled=(disponivel == 0))
            add_btn = st.form_submit_button("➕  Adicionar ao carrinho",
                use_container_width=True, disabled=(disponivel == 0))

        if add_btn:
            if disponivel == 0:
                show_error(f"Estoque esgotado para '{prod_sel[1]}'.",
                           "Remova o item do carrinho ou aguarde reposição.")
            else:
                existe = next((x for x in cart if x["id"] == prod_sel[0]), None)
                if existe:
                    existe["qtd"] += qtd_add
                else:
                    st.session_state.cart.append({"id":prod_sel[0],"nome":prod_sel[1],
                                                   "preco":prod_sel[2],"qtd":qtd_add})
                st.rerun()

        st.markdown('<hr class="erp">', unsafe_allow_html=True)

        if cart:
            with st.form("form_finalizar"):
                st.markdown("#### Finalizar pedido")
                cf1, cf2 = st.columns(2)
                cliente_sel = cf1.selectbox("Cliente *", [c[0] for c in clis])
                forma       = cf2.selectbox("Forma de Pagamento *", [p[0] for p in pags])
                obs_pedido  = st.text_area("Observação (opcional)",
                    placeholder="Ex: entregar na portaria, cliente solicitou nota fiscal, etc.",
                    height=80)
                fin_btn = st.form_submit_button(f"✅  Finalizar Venda", use_container_width=True)

            if fin_btn:
                erros = []
                for item in cart:
                    est_real = get_estoque(item["id"])
                    if est_real < item["qtd"]:
                        erros.append(f"Estoque insuficiente para '{item['nome']}'. Disponível: {est_real} un.")
                if erros:
                    for e in erros:
                        show_error(e,"Ajuste as quantidades no carrinho.")
                else:
                    total_venda = sum(x["preco"]*x["qtd"] for x in cart)
                    ok = run_query("INSERT INTO vendas(data,cliente_name,valor_total,pagamento,status,observacao) VALUES(?,?,?,?,?,?)",
                                   (datetime.now(),cliente_sel,total_venda,forma,"Pago",obs_pedido.strip()))
                    if ok is True:
                        vid_r = run_query("SELECT last_insert_rowid()", fetch=True)
                        if vid_r:
                            venda_id = vid_r[0][0]
                            for item in cart:
                                run_query("INSERT INTO itens_venda(venda_id,produto_nome,quantidade,preco_unit) VALUES(?,?,?,?)",
                                          (venda_id,item["nome"],item["qtd"],item["preco"]))
                                run_query("UPDATE produtos SET estoque_atual=estoque_atual-? WHERE id=?",
                                          (item["qtd"],item["id"]))
                        show_success(f"Venda de {cur} {total_venda:.2f} finalizada!",
                                     f"{len(cart)} produto(s) para {cliente_sel} · {forma}")
                        st.session_state.cart = []
                        st.balloons(); st.rerun()
                    else:
                        show_error("Não foi possível salvar a venda.","Tente novamente.")
        else:
            show_info("Carrinho vazio.","Adicione produtos acima para liberar a finalização.")


# ════════════════════════════════════════════════════════════
#  3. ESTOQUE
# ════════════════════════════════════════════════════════════
elif menu == "Estoque":
    page_header("📦","Estoque","Gerencie seu catálogo de produtos")

    cats_raw = run_query("SELECT nome FROM categorias WHERE ativo=1 ORDER BY nome", fetch=True)
    cat_opts = [c[0] for c in cats_raw] if cats_raw else []

    with st.expander("➕  Adicionar novo produto"):
        if not cat_opts:
            show_warning("Nenhuma categoria ativa.","Acesse 'Categorias' e crie uma antes de cadastrar produtos.")
        else:
            with st.form("f_prod"):
                c1,c2,c3 = st.columns([1,2,1])
                sku  = c1.text_input("Código / SKU *")
                nome = c2.text_input("Nome do Produto *")
                cat  = c3.selectbox("Categoria *", cat_opts)
                c4,c5,c6,c7 = st.columns(4)
                pc    = c4.number_input(f"Preço Custo ({cur})",   min_value=0.0, step=0.01, format="%.2f")
                pv    = c5.number_input(f"Preço Venda ({cur}) *", min_value=0.0, step=0.01, format="%.2f")
                est   = c6.number_input("Estoque Inicial",         min_value=0, step=1)
                e_min = c7.number_input("Estoque Mínimo",          min_value=0, step=1, value=2)
                save_btn = st.form_submit_button("💾  Salvar Produto", use_container_width=True)
            if save_btn:
                if not validate_required(sku, nome):
                    show_error("Preencha os campos obrigatórios.","SKU e Nome são obrigatórios.")
                elif pv == 0:
                    show_error("Preço de venda não pode ser zero.","Informe um valor válido.")
                else:
                    res = run_query("INSERT INTO produtos(sku,nome,categoria,preco_custo,preco_venda,estoque_atual,estoque_minimo) VALUES(?,?,?,?,?,?,?)",
                                    (sku.strip(),nome.strip(),cat,pc,pv,est,e_min))
                    if res is True:
                        show_success("Produto cadastrado com sucesso!",f"'{nome}' adicionado."); st.rerun()
                    elif res == "duplicate":
                        show_error("Já existe um produto com esse SKU.",f"O código '{sku}' já está em uso.")
                    else:
                        show_error("Não foi possível salvar.","Tente novamente.")

    st.markdown('<hr class="erp">', unsafe_allow_html=True)
    col_b,col_f = st.columns([3,1])
    busca   = col_b.text_input("🔍  Buscar produto",placeholder="Nome ou SKU…",key="busca_prod",label_visibility="collapsed")
    mostrar = col_f.selectbox("Status",["Ativos","Inativos","Todos"],key="filtro_prod",label_visibility="collapsed")

    prods_raw = run_query("SELECT id,sku,nome,categoria,preco_custo,preco_venda,estoque_atual,estoque_minimo,ativo FROM produtos ORDER BY nome", fetch=True)

    if not prods_raw:
        show_info("Nenhum produto cadastrado ainda.","Use o formulário acima.")
    else:
        filtrados = prods_raw
        if mostrar=="Ativos":   filtrados=[p for p in filtrados if p[8]==1]
        if mostrar=="Inativos": filtrados=[p for p in filtrados if p[8]==0]
        if busca:
            b=busca.lower()
            filtrados=[p for p in filtrados if b in p[2].lower() or b in p[1].lower()]
        st.markdown(f"**{len(filtrados)} produto(s)**")
        for prod in filtrados:
            pid,sku_v,nm,cat_v,pc_v,pv_v,est_v,emin_v,ativo_v = prod
            badge_est = ('<span class="badge zero">Zerado</span>' if est_v==0
                         else '<span class="badge low">Baixo</span>' if est_v<=emin_v
                         else '<span class="badge ok">OK</span>')
            inativo_cls = "" if ativo_v else " inativo"
            col_card,col_edit,col_adj,col_tog = st.columns([5,1,1,1])
            with col_card:
                st.markdown(f"""<div class="prod-card{inativo_cls}">
                    <div class="pc-left"><span class="pc-sku">SKU: {sku_v}</span>
                    <span class="pc-name">{nm}</span>
                    <span class="pc-cat">{cat_v} · Estoque: {est_v} {badge_est}</span></div>
                    <div class="pc-right"><span class="pc-price">{cur} {pv_v:.2f}</span>
                    <span class="badge {'ativo' if ativo_v else 'inativo'}">{'Ativo' if ativo_v else 'Inativo'}</span></div>
                </div>""", unsafe_allow_html=True)
            with col_edit:
                if st.button("✏️",key=f"edit_{pid}",help="Editar produto"):
                    st.session_state.editing_prod_id=pid; st.session_state.editing_prod_data=prod
                    st.session_state.adj_prod=None; st.rerun()
            with col_adj:
                if st.button("📦",key=f"adj_{pid}",help="Ajustar estoque"):
                    st.session_state.adj_prod=prod; st.session_state.editing_prod_id=None; st.rerun()
            with col_tog:
                if st.button("🔴" if ativo_v else "🟢",key=f"tog_{pid}",
                             help="Inativar produto" if ativo_v else "Ativar produto"):
                    run_query("UPDATE produtos SET ativo=? WHERE id=?",(0 if ativo_v else 1,pid)); st.rerun()

        # Edição
        if st.session_state.editing_prod_id:
            pid_e=st.session_state.editing_prod_id
            pd_=st.session_state.editing_prod_data
            _,sku_e,nm_e,cat_e,pc_e,pv_e,est_e,emin_e,_ = pd_
            st.markdown('<hr class="erp">', unsafe_allow_html=True)
            st.markdown(f"#### ✏️ Editando: {nm_e}")
            with st.form("f_edit"):
                ce1,ce2,ce3=st.columns([1,2,1])
                new_sku =ce1.text_input("SKU *",value=sku_e)
                new_nome=ce2.text_input("Nome *",value=nm_e)
                ci=cat_opts.index(cat_e) if cat_e in cat_opts else 0
                new_cat =ce3.selectbox("Categoria *",cat_opts or [cat_e],index=ci)
                ce4,ce5,ce6,ce7=st.columns(4)
                new_pc  =ce4.number_input(f"Custo ({cur})", value=float(pc_e),  min_value=0.0,step=0.01,format="%.2f")
                new_pv  =ce5.number_input(f"Venda ({cur})*",value=float(pv_e),  min_value=0.0,step=0.01,format="%.2f")
                new_est =ce6.number_input("Estoque",         value=int(est_e),   min_value=0,step=1)
                new_emin=ce7.number_input("Estoque Mín.",    value=int(emin_e),  min_value=0,step=1)
                cs,cc=st.columns(2)
                save_e=cs.form_submit_button("💾  Salvar",use_container_width=True)
                cncl_e=cc.form_submit_button("✕  Cancelar",use_container_width=True)
            if cncl_e:
                st.session_state.editing_prod_id=None; st.rerun()
            if save_e:
                if not validate_required(new_sku,new_nome):
                    show_error("SKU e Nome são obrigatórios.")
                elif new_pv==0:
                    show_error("Preço de venda não pode ser zero.")
                else:
                    res=run_query("UPDATE produtos SET sku=?,nome=?,categoria=?,preco_custo=?,preco_venda=?,estoque_atual=?,estoque_minimo=? WHERE id=?",
                                  (new_sku.strip(),new_nome.strip(),new_cat,new_pc,new_pv,new_est,new_emin,pid_e))
                    if res is True:
                        show_success("Produto atualizado com sucesso!")
                        st.session_state.editing_prod_id=None; st.rerun()
                    elif res=="duplicate":
                        show_error("Já existe um produto com esse SKU.")
                    else:
                        show_error("Não foi possível salvar.")

        # Ajuste de estoque
        if st.session_state.adj_prod:
            adj=st.session_state.adj_prod
            adj_id,_,adj_nm=adj[0],adj[1],adj[2]; adj_est=int(adj[6])
            st.markdown('<hr class="erp">', unsafe_allow_html=True)
            st.markdown(f"#### 📦 Ajustar estoque: {adj_nm}  (atual: **{adj_est}**)")
            with st.form("f_adj"):
                co,cq=st.columns(2)
                op    =co.selectbox("Operação",["Adicionar","Remover","Definir exato"])
                qtd_aj=cq.number_input("Quantidade",min_value=1,step=1)
                st.text_input("Motivo (opcional)",placeholder="Ex: compra, perda, inventário…")
                ca2,cb2=st.columns(2)
                ap=ca2.form_submit_button("✅  Aplicar",use_container_width=True)
                cn=cb2.form_submit_button("✕  Cancelar",use_container_width=True)
            if cn:
                st.session_state.adj_prod=None; st.rerun()
            if ap:
                if op=="Adicionar":
                    sql,par,nv="UPDATE produtos SET estoque_atual=estoque_atual+? WHERE id=?",(qtd_aj,adj_id),adj_est+qtd_aj
                elif op=="Remover":
                    if qtd_aj>adj_est:
                        show_error("Quantidade maior que o estoque atual.",f"Máximo: {adj_est} un."); st.stop()
                    sql,par,nv="UPDATE produtos SET estoque_atual=estoque_atual-? WHERE id=?",(qtd_aj,adj_id),adj_est-qtd_aj
                else:
                    sql,par,nv="UPDATE produtos SET estoque_atual=? WHERE id=?",(qtd_aj,adj_id),qtd_aj
                if run_query(sql,par) is True:
                    show_success("Estoque ajustado!",f"'{adj_nm}' agora tem {nv} unidades.")
                    st.session_state.adj_prod=None; st.rerun()
                else:
                    show_error("Não foi possível ajustar o estoque.")


# ════════════════════════════════════════════════════════════
#  4. CLIENTES
# ════════════════════════════════════════════════════════════
elif menu == "Clientes":
    page_header("👥","Clientes","Gerencie sua base de clientes")
    with st.expander("➕  Novo cliente"):
        with st.form("f_cli"):
            c1,c2=st.columns(2)
            nome=c1.text_input("Nome Completo *"); doc=c2.text_input("CPF / CNPJ *",placeholder="Somente números ou com pontuação")
            c3,c4=st.columns(2); tel=c3.text_input("Telefone"); email=c4.text_input("E-mail")
            st.markdown("**Endereço**")
            ea,eb,ec=st.columns([3,1,2]); rua=ea.text_input("Rua / Logradouro"); num=eb.text_input("Número"); comp=ec.text_input("Complemento")
            ed,ee,ef,eg=st.columns([2,2,1,2]); bairro=ed.text_input("Bairro"); cidade=ee.text_input("Cidade"); estado=ef.text_input("UF",max_chars=2); cep=eg.text_input("CEP",placeholder="00000-000")
            save_btn=st.form_submit_button("💾  Salvar Cliente",use_container_width=True)
        if save_btn:
            if not validate_required(nome,doc):
                show_error("Preencha os campos obrigatórios.","Nome e CPF/CNPJ são obrigatórios.")
            elif not validate_doc(doc):
                show_error("CPF/CNPJ inválido.","CPF deve ter 11 dígitos e CNPJ 14 dígitos.")
            else:
                res=run_query("INSERT INTO clientes(nome,documento,telefone,email,rua,numero,complemento,bairro,cidade,estado,cep) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                              (nome.strip(),doc.strip(),tel,email,rua,num,comp,bairro,cidade,estado.upper() if estado else "",cep))
                if res is True:
                    show_success("Cliente cadastrado com sucesso!",f"'{nome}' adicionado."); st.rerun()
                elif res=="duplicate":
                    show_error("Já existe um cliente com esse CPF/CNPJ.")
                else:
                    show_error("Não foi possível salvar.")

    st.markdown('<hr class="erp">', unsafe_allow_html=True)
    col_b,col_f=st.columns([3,1])
    busca_cli =col_b.text_input("🔍  Buscar cliente",placeholder="Nome, CPF/CNPJ ou cidade…",key="busca_cli",label_visibility="collapsed")
    filtro_cli=col_f.selectbox("Status",["Ativos","Inativos","Todos"],key="filtro_cli",label_visibility="collapsed")

    # Busca todos os dados do cliente para edição
    clis_full=run_query(
        "SELECT id,nome,documento,telefone,email,rua,numero,complemento,bairro,cidade,estado,cep,ativo "
        "FROM clientes ORDER BY nome", fetch=True)

    if clis_full:
        clis_view = clis_full
        if filtro_cli=="Ativos":   clis_view=[c for c in clis_view if c[12]==1]
        if filtro_cli=="Inativos": clis_view=[c for c in clis_view if c[12]==0]
        if busca_cli:
            b=busca_cli.lower()
            clis_view=[c for c in clis_view if b in (c[1] or "").lower()
                       or b in (c[2] or "").lower() or b in (c[9] or "").lower()]

        st.markdown(f"**{len(clis_view)} cliente(s)**")
        for cli in clis_view:
            (cid,cnome,cdoc,ctel,cemail,crua,cnum,ccomp,
             cbairro,ccidade,cestado,ccep,cativo) = cli
            sb=('<span class="badge ativo">Ativo</span>' if cativo
                else '<span class="badge inativo">Inativo</span>')

            # ── Modo edição ──
            if st.session_state.edit_cli_id == cid:
                st.markdown(f"#### ✏️ Editando: {cnome}")
                with st.form(f"f_edit_cli_{cid}"):
                    ec1,ec2=st.columns(2)
                    en =ec1.text_input("Nome Completo *", value=cnome or "")
                    edo=ec2.text_input("CPF / CNPJ *",    value=cdoc  or "",
                                       placeholder="Somente números ou com pontuação")
                    ec3,ec4=st.columns(2)
                    etl =ec3.text_input("Telefone", value=ctel   or "")
                    eml =ec4.text_input("E-mail",   value=cemail or "")
                    st.markdown("**Endereço**")
                    eea,eeb,eec=st.columns([3,1,2])
                    erua =eea.text_input("Rua / Logradouro", value=crua   or "")
                    enum =eeb.text_input("Número",            value=cnum  or "")
                    ecomp=eec.text_input("Complemento",       value=ccomp or "")
                    eed,eee,eef,eeg=st.columns([2,2,1,2])
                    ebairro =eed.text_input("Bairro",  value=cbairro  or "")
                    ecidade =eee.text_input("Cidade",  value=ccidade  or "")
                    eestado =eef.text_input("UF",      value=cestado  or "", max_chars=2)
                    ecep    =eeg.text_input("CEP",     value=ccep     or "", placeholder="00000-000")
                    cs_e, cc_e = st.columns(2)
                    sv_e=cs_e.form_submit_button("💾  Salvar alterações", use_container_width=True)
                    cn_e=cc_e.form_submit_button("✕  Cancelar",           use_container_width=True)

                if cn_e:
                    st.session_state.edit_cli_id=None; st.session_state.edit_cli_data=None; st.rerun()
                if sv_e:
                    if not validate_required(en, edo):
                        show_error("Preencha os campos obrigatórios.","Nome e CPF/CNPJ são obrigatórios.")
                    elif not validate_doc(edo):
                        show_error("CPF/CNPJ inválido.","CPF deve ter 11 dígitos e CNPJ 14 dígitos.")
                    else:
                        res=run_query(
                            "UPDATE clientes SET nome=?,documento=?,telefone=?,email=?,"
                            "rua=?,numero=?,complemento=?,bairro=?,cidade=?,estado=?,cep=? WHERE id=?",
                            (en.strip(),edo.strip(),etl,eml,erua,enum,ecomp,
                             ebairro,ecidade,eestado.upper() if eestado else "",ecep, cid))
                        if res is True:
                            show_success("Cliente atualizado com sucesso!",f"'{en}' foi salvo.")
                            st.session_state.edit_cli_id=None; st.session_state.edit_cli_data=None
                            st.rerun()
                        elif res=="duplicate":
                            show_error("Já existe um cliente com esse CPF/CNPJ.")
                        else:
                            show_error("Não foi possível salvar.")
            else:
                # ── Modo exibição ──
                col_i, col_e, col_t = st.columns([8,1,1])
                col_i.markdown(
                    f"**{cnome}** &nbsp; {sb}<br>"
                    f"<small style='color:#6b7280'>{cdoc} · {ctel or '—'} · {ccidade or '—'}</small>",
                    unsafe_allow_html=True)
                if col_e.button("✏️", key=f"edit_cli_{cid}", help="Editar cliente"):
                    st.session_state.edit_cli_id  = cid
                    st.session_state.edit_cli_data = cli
                    st.rerun()
                if col_t.button("🔴" if cativo else "🟢", key=f"tog_cli_{cid}",
                                help="Inativar" if cativo else "Ativar"):
                    run_query("UPDATE clientes SET ativo=? WHERE id=?",(0 if cativo else 1, cid))
                    st.rerun()

            st.markdown('<div style="border-top:0.5px solid #eef0ff;margin:4px 0"></div>',
                        unsafe_allow_html=True)
    else:
        show_info("Nenhum cliente encontrado.","Ajuste os filtros ou adicione um novo cliente.")


# ════════════════════════════════════════════════════════════
#  5. HISTÓRICO DE VENDAS
# ════════════════════════════════════════════════════════════
elif menu == "Histórico de Vendas":
    page_header("📜","Histórico de Vendas","Consulte e gerencie todas as transações")
    filtro_status=st.selectbox("Filtrar por status",["Todos","Pago","Cancelado"],key="filtro_hist")
    where="" if filtro_status=="Todos" else f"WHERE v.status='{filtro_status}'"
    vendas=run_query(
        f"""SELECT v.id, strftime('%d/%m/%Y %H:%M',v.data), v.cliente_name,
                   v.valor_total, v.pagamento, v.status,
                   GROUP_CONCAT(i.produto_nome||' x'||i.quantidade,' | '),
                   COALESCE(v.observacao,'')
           FROM vendas v LEFT JOIN itens_venda i ON i.venda_id=v.id
           {where} GROUP BY v.id ORDER BY v.data DESC""", fetch=True)

    if not vendas:
        show_info("Nenhuma venda encontrada.","Ajuste os filtros ou realize uma venda.")
    else:
        for row in vendas:
            vid, data_fmt, cli, val, pag, status, itens, obs = row
            itens = itens or "—"
            obs   = obs   or ""
            is_pago = status == "Pago"
            sb = ('<span class="badge pago">Pago</span>' if is_pago
                  else '<span class="badge cancelado">Cancelado</span>')

            col_info, col_val, col_acoes = st.columns([5, 2, 2])

            with col_info:
                obs_html = (f"<br><small style='color:#9ca3af;font-style:italic'>📝 {obs}</small>"
                            if obs else "")
                st.markdown(
                    f"**#{vid} — {cli}** &nbsp; {sb}<br>"
                    f"<small style='color:#6b7280'>{data_fmt} · {pag} · {itens}</small>"
                    f"{obs_html}",
                    unsafe_allow_html=True)

            with col_val:
                st.markdown(
                    f"<div style='font-family:Sora,sans-serif;font-size:1rem;"
                    f"font-weight:700;color:#6366f1;padding-top:4px'>"
                    f"{cur} {val:,.2f}</div>",
                    unsafe_allow_html=True)

            with col_acoes:
                if is_pago:
                    # ── Cancelar pedido ──
                    if st.button("❌ Cancelar", key=f"cancel_{vid}",
                                 help="Cancelar pedido e reverter estoque"):
                        itens_db = run_query(
                            "SELECT produto_nome,quantidade FROM itens_venda WHERE venda_id=?",
                            (vid,), fetch=True)
                        if itens_db:
                            for pnome, pqtd in itens_db:
                                run_query(
                                    "UPDATE produtos SET estoque_atual=estoque_atual+? WHERE nome=?",
                                    (pqtd, pnome))
                        run_query("UPDATE vendas SET status='Cancelado' WHERE id=?", (vid,))
                        show_success(f"Pedido #{vid} cancelado.",
                                     "Estoque dos produtos revertido automaticamente.")
                        st.rerun()
                else:
                    # ── Reativar pedido cancelado ──
                    if st.button("✅ Reativar", key=f"reativar_{vid}",
                                 help="Reativar pedido e debitar estoque novamente"):
                        # Verificar se há estoque suficiente para todos os itens
                        itens_db = run_query(
                            "SELECT produto_nome,quantidade FROM itens_venda WHERE venda_id=?",
                            (vid,), fetch=True)
                        erros_reativ = []
                        if itens_db:
                            for pnome, pqtd in itens_db:
                                est_atual = run_query(
                                    "SELECT estoque_atual FROM produtos WHERE nome=?",
                                    (pnome,), fetch=True)
                                est_val = est_atual[0][0] if est_atual else 0
                                if est_val < pqtd:
                                    erros_reativ.append(
                                        f"Estoque insuficiente para '{pnome}': "
                                        f"necessário {pqtd}, disponível {est_val}.")
                        if erros_reativ:
                            for e in erros_reativ:
                                show_error(e, "Ajuste o estoque antes de reativar este pedido.")
                        else:
                            if itens_db:
                                for pnome, pqtd in itens_db:
                                    run_query(
                                        "UPDATE produtos SET estoque_atual=estoque_atual-? WHERE nome=?",
                                        (pqtd, pnome))
                            run_query("UPDATE vendas SET status='Pago' WHERE id=?", (vid,))
                            show_success(f"Pedido #{vid} reativado com sucesso!",
                                         "O estoque foi debitado novamente.")
                            st.rerun()

            st.markdown(
                '<div style="border-top:0.5px solid #eef0ff;margin:6px 0"></div>',
                unsafe_allow_html=True)


# ════════════════════════════════════════════════════════════
#  6. CATEGORIAS
# ════════════════════════════════════════════════════════════
elif menu == "Categorias":
    page_header("🏷️","Categorias","Organize seus produtos por categoria")
    c1,c2=st.columns([3,1])
    new_cat=c1.text_input("Nova categoria",label_visibility="collapsed",placeholder="Ex: Eletrônicos, Alimentação…")
    if c2.button("➕  Adicionar",use_container_width=True):
        if not validate_required(new_cat):
            show_error("Digite o nome da categoria.")
        else:
            res=run_query("INSERT INTO categorias(nome) VALUES(?)",(new_cat.strip(),))
            if res is True:
                show_success("Categoria adicionada!",f"'{new_cat}' criada."); st.rerun()
            elif res=="duplicate":
                show_error("Essa categoria já está cadastrada.","Escolha um nome diferente.")
            else:
                show_error("Não foi possível salvar.")
    cats=run_query("SELECT id,nome,ativo FROM categorias ORDER BY nome",fetch=True)
    if cats:
        st.markdown('<hr class="erp">', unsafe_allow_html=True)
        st.markdown(f"**{len(cats)} categoria(s)**")
        for (cid,cnome,cativo) in cats:
            sb='<span class="badge ativo">Ativo</span>' if cativo else '<span class="badge inativo">Inativo</span>'
            inativo_cls="" if cativo else " inativo"
            if st.session_state.edit_cat_id==cid:
                with st.form(f"f_edit_cat_{cid}"):
                    new_name=st.text_input("Nome da categoria *",value=cnome)
                    cs2,cc2=st.columns(2)
                    sv=cs2.form_submit_button("💾  Salvar",use_container_width=True)
                    cn2=cc2.form_submit_button("✕  Cancelar",use_container_width=True)
                if cn2:
                    st.session_state.edit_cat_id=None; st.rerun()
                if sv:
                    if not validate_required(new_name):
                        show_error("O nome não pode estar em branco.")
                    else:
                        res=run_query("UPDATE categorias SET nome=? WHERE id=?",(new_name.strip(),cid))
                        if res is True:
                            run_query("UPDATE produtos SET categoria=? WHERE categoria=?",(new_name.strip(),cnome))
                            show_success("Categoria atualizada!","Os produtos vinculados foram atualizados.")
                            st.session_state.edit_cat_id=None; st.rerun()
                        elif res=="duplicate":
                            show_error("Já existe uma categoria com esse nome.")
                        else:
                            show_error("Não foi possível salvar.")
            else:
                col_n,col_e,col_t=st.columns([7,1,1])
                col_n.markdown(f'<div class="aux-row{inativo_cls}"><span class="ar-name">🏷️ {cnome}</span>&nbsp;{sb}</div>',unsafe_allow_html=True)
                if col_e.button("✏️",key=f"edit_cat_{cid}",help="Editar nome"):
                    st.session_state.edit_cat_id=cid; st.rerun()
                if col_t.button("🔴" if cativo else "🟢",key=f"tog_cat_{cid}",help="Inativar" if cativo else "Ativar"):
                    run_query("UPDATE categorias SET ativo=? WHERE id=?",(0 if cativo else 1,cid)); st.rerun()
    else:
        show_info("Nenhuma categoria cadastrada.","Adicione uma categoria acima.")


# ════════════════════════════════════════════════════════════
#  7. FORMAS DE PAGAMENTO
# ════════════════════════════════════════════════════════════
elif menu == "Formas de Pagamento":
    page_header("💳","Formas de Pagamento","Gerencie as formas de pagamento aceitas")
    c1,c2=st.columns([3,1])
    new_pag=c1.text_input("Nova forma de pagamento",label_visibility="collapsed",placeholder="Ex: Dinheiro, Pix, Cartão…")
    if c2.button("➕  Adicionar",use_container_width=True):
        if not validate_required(new_pag):
            show_error("Digite o nome da forma de pagamento.")
        else:
            res=run_query("INSERT INTO pagamentos(nome) VALUES(?)",(new_pag.strip(),))
            if res is True:
                show_success("Forma de pagamento adicionada!",f"'{new_pag}' criada."); st.rerun()
            elif res=="duplicate":
                show_error("Essa forma de pagamento já está cadastrada.")
            else:
                show_error("Não foi possível salvar.")
    pags=run_query("SELECT id,nome,ativo FROM pagamentos ORDER BY nome",fetch=True)
    if pags:
        st.markdown('<hr class="erp">', unsafe_allow_html=True)
        st.markdown(f"**{len(pags)} forma(s) de pagamento**")
        for (pid,pnome,pativo) in pags:
            sb='<span class="badge ativo">Ativo</span>' if pativo else '<span class="badge inativo">Inativo</span>'
            inativo_cls="" if pativo else " inativo"
            if st.session_state.edit_pag_id==pid:
                with st.form(f"f_edit_pag_{pid}"):
                    new_pname=st.text_input("Nome *",value=pnome)
                    cs3,cc3=st.columns(2)
                    sv3=cs3.form_submit_button("💾  Salvar",use_container_width=True)
                    cn3=cc3.form_submit_button("✕  Cancelar",use_container_width=True)
                if cn3:
                    st.session_state.edit_pag_id=None; st.rerun()
                if sv3:
                    if not validate_required(new_pname):
                        show_error("O nome não pode estar em branco.")
                    else:
                        res=run_query("UPDATE pagamentos SET nome=? WHERE id=?",(new_pname.strip(),pid))
                        if res is True:
                            show_success("Forma de pagamento atualizada!")
                            st.session_state.edit_pag_id=None; st.rerun()
                        elif res=="duplicate":
                            show_error("Já existe uma forma de pagamento com esse nome.")
                        else:
                            show_error("Não foi possível salvar.")
            else:
                col_n,col_e,col_t=st.columns([7,1,1])
                col_n.markdown(f'<div class="aux-row{inativo_cls}"><span class="ar-name">💳 {pnome}</span>&nbsp;{sb}</div>',unsafe_allow_html=True)
                if col_e.button("✏️",key=f"edit_pag_{pid}",help="Editar nome"):
                    st.session_state.edit_pag_id=pid; st.rerun()
                if col_t.button("🔴" if pativo else "🟢",key=f"tog_pag_{pid}",help="Inativar" if pativo else "Ativar"):
                    run_query("UPDATE pagamentos SET ativo=? WHERE id=?",(0 if pativo else 1,pid)); st.rerun()
    else:
        show_info("Nenhuma forma de pagamento cadastrada.","Adicione uma acima.")


# ════════════════════════════════════════════════════════════
#  8. CONFIGURAÇÕES
# ════════════════════════════════════════════════════════════
elif menu == "Configurações":
    page_header("⚙️","Configurações","Personalize o sistema")
    with st.form("f_config"):
        st.markdown("#### Empresa")
        c1,c2=st.columns(2)
        new_n   =c1.text_input("Nome da Empresa *",value=config['nome_empresa'])
        logo_url=c2.text_input("URL do Logo (opcional)",value=config.get('logo_url',''))
        st.markdown("#### Preferências")
        new_curr=st.selectbox("Moeda",["R$","$","€","£"],index=["R$","$","€","£"].index(config.get("currency","R$")))
        saved=st.form_submit_button("💾  Salvar Configurações",use_container_width=True)
    if saved:
        if not validate_required(new_n):
            show_error("O nome da empresa não pode estar em branco.")
        else:
            config.update({"nome_empresa":new_n.strip(),"logo_url":logo_url.strip(),"currency":new_curr})
            save_config(config)
            show_success("Configurações salvas!","Recarregue a página para ver as alterações.")
            st.rerun()