# ╔══════════════════════════════════════════════════════════════╗
#  ERP SaaS Multi-tenant  |  PostgreSQL  |  v9.0
#  Design minimalista azul/branco/preto | Performance otimizada
# ╚══════════════════════════════════════════════════════════════╝

import streamlit as st
import psycopg2
from psycopg2 import pool as pg_pool, errors as pg_errors
import hashlib, re, io, csv
from datetime import datetime, date, timedelta
from contextlib import contextmanager

try:
    import pandas as pd
    import openpyxl
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

st.set_page_config(
    page_title="ERP Pro",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={"About": "ERP SaaS v9.0"}
)

# ── CSS Minimalista: Azul / Branco / Preto ──────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
@import url('https://fonts.googleapis.com/css2?family=Material+Symbols+Outlined:opsz,wght,FILL,GRAD@20..48,100..700,0..1,-50..200');

html, body, [class*="css"] {
  font-family: 'Inter', sans-serif;
  background: #f8fafc;
  color: #0f172a;
}

/* ── Sidebar ── */
section[data-testid="stSidebar"] {
  background: #0f172a;
  border-right: 1px solid #1e293b;
  min-width: 220px !important;
  max-width: 220px !important;
}
section[data-testid="stSidebar"] * { color: #94a3b8 !important; }
section[data-testid="stSidebar"] .stButton > button {
  width: 100%; text-align: left; padding: .38rem .75rem;
  border-radius: 6px; border: none; background: transparent;
  color: #94a3b8 !important; font-size: .82rem; font-weight: 400;
  transition: background .12s, color .12s; margin-bottom: 1px;
  display: flex; align-items: center; gap: .45rem;
}
section[data-testid="stSidebar"] .stButton > button:hover {
  background: #1e293b !important; color: #e2e8f0 !important;
}
.nav-active button {
  background: #1d4ed8 !important;
  color: #ffffff !important;
  font-weight: 600 !important;
}
.nav-active button:hover { background: #1e40af !important; }
.nav-group {
  font-size: .62rem; font-weight: 700; letter-spacing: .1em;
  text-transform: uppercase; color: #475569 !important;
  padding: .9rem .75rem .25rem; display: block;
}
.nav-brand {
  font-size: 1rem; font-weight: 700; color: #fff !important;
  padding: 1rem .75rem .5rem; display: block; letter-spacing: -.02em;
}
.nav-brand span { color: #3b82f6 !important; }
.nav-tenant {
  font-size: .72rem; color: #475569 !important;
  padding: 0 .75rem .75rem; display: block;
}
.nav-user {
  font-size: .75rem; color: #64748b !important;
  padding: .5rem .75rem 0; display: block;
}
.logout-btn button {
  color: #f87171 !important;
  margin-top: .25rem;
}
.notif-dot {
  width: 7px; height: 7px; background: #ef4444;
  border-radius: 50%; display: inline-block; margin-left: 6px;
  vertical-align: middle;
}

/* ── Página ── */
.page-header {
  display: flex; align-items: center; gap: .75rem;
  margin-bottom: 1.5rem; padding-bottom: 1rem;
  border-bottom: 1px solid #e2e8f0;
}
.ph-icon {
  width: 36px; height: 36px; border-radius: 8px;
  background: #1d4ed8; display: flex; align-items: center;
  justify-content: center; flex-shrink: 0;
}
.ph-icon svg { width: 18px; height: 18px; fill: #fff; }
.ph-icon .material-symbols-outlined {
  font-size: 18px; color: #fff !important;
  font-variation-settings: 'FILL' 1, 'wght' 400;
}
.page-header h1 {
  font-size: 1.15rem; font-weight: 700; margin: 0;
  color: #0f172a; letter-spacing: -.02em;
}
.page-header p { font-size: .8rem; color: #64748b; margin: 0; }

/* ── Cards ── */
.card {
  background: #fff; border: 1px solid #e2e8f0;
  border-radius: 10px; padding: .75rem 1rem;
  margin-bottom: 6px; transition: border-color .12s, box-shadow .12s;
}
.card:hover { border-color: #93c5fd; box-shadow: 0 1px 6px rgba(29,78,216,.07); }
.card.inativo { opacity: .5; border-style: dashed; }
.card-label { font-size: .68rem; color: #94a3b8; margin-bottom: 2px; }
.card-title { font-size: .9rem; font-weight: 600; color: #0f172a; margin: 0; }
.card-sub { font-size: .75rem; color: #64748b; margin-top: 2px; }
.card-val { font-size: .95rem; font-weight: 700; color: #1d4ed8; white-space: nowrap; }

/* ── Badges ── */
.badge {
  display: inline-block; padding: 1px 8px; border-radius: 99px;
  font-size: .7rem; font-weight: 600; line-height: 1.6;
}
.b-blue   { background: #dbeafe; color: #1e40af; }
.b-green  { background: #dcfce7; color: #166534; }
.b-red    { background: #fee2e2; color: #991b1b; }
.b-yellow { background: #fef9c3; color: #854d0e; }
.b-gray   { background: #f1f5f9; color: #475569; }
.b-orange { background: #ffedd5; color: #9a3412; }

/* ── Métricas ── */
[data-testid="stMetric"] {
  background: #fff; border: 1px solid #e2e8f0;
  border-radius: 10px; padding: .9rem 1.1rem;
}
[data-testid="stMetricValue"] {
  font-size: 1.4rem; font-weight: 700; color: #0f172a;
}
[data-testid="stMetricLabel"] { font-size: .8rem; color: #64748b; }

/* ── Formulários ── */
[data-testid="stForm"] {
  background: #fff; border: 1px solid #e2e8f0;
  border-radius: 10px; padding: 1.1rem;
}
[data-testid="stExpander"] {
  border: 1px solid #e2e8f0 !important;
  border-radius: 10px !important;
  background: #fff !important;
}
.stTextInput input, .stNumberInput input, .stTextArea textarea,
.stSelectbox > div { border-radius: 7px !important; }

/* ── Toasts ── */
.toast {
  padding: .65rem .9rem; border-radius: 8px;
  font-size: .84rem; margin: .3rem 0;
  display: flex; align-items: flex-start; gap: .5rem;
}
.toast.ok  { background: #f0fdf4; border-left: 3px solid #22c55e; color: #166534; }
.toast.err { background: #fef2f2; border-left: 3px solid #ef4444; color: #b91c1c; }
.toast.wrn { background: #fefce8; border-left: 3px solid #eab308; color: #854d0e; }
.toast.inf { background: #eff6ff; border-left: 3px solid #3b82f6; color: #1e40af; }
.toast b { font-weight: 600; }

/* ── Cart ── */
.cart-row {
  display: flex; justify-content: space-between; align-items: center;
  padding: .5rem .75rem; background: #f8fafc; border: 1px solid #e2e8f0;
  border-radius: 8px; margin-bottom: 4px; font-size: .85rem;
}
.cart-total-bar {
  display: flex; justify-content: space-between; align-items: center;
  padding: .65rem .9rem; background: #1d4ed8; color: #fff;
  border-radius: 8px; margin-top: .6rem;
}
.cart-total-bar span { font-size: .82rem; opacity: .85; }
.cart-total-bar strong { font-size: 1.05rem; font-weight: 700; }

/* ── Divisor ── */
hr.erp { border: none; border-top: 1px solid #e2e8f0; margin: 1rem 0; }

/* ── Login ── */
.login-box {
  max-width: 400px; margin: 3rem auto;
  background: #fff; border: 1px solid #e2e8f0;
  border-radius: 14px; padding: 2rem;
}
.login-title {
  text-align: center; font-size: 1.35rem; font-weight: 700;
  color: #0f172a; margin-bottom: .2rem;
}
.login-title span { color: #1d4ed8; }
.login-sub {
  text-align: center; font-size: .82rem;
  color: #64748b; margin-bottom: 1.4rem;
}

/* ── Notif ── */
.notif-item {
  padding: .65rem .9rem; border-radius: 9px;
  margin-bottom: 5px; border-left: 3px solid;
}
.notif-item.estoque { background: #fefce8; border-color: #eab308; }
.notif-item.receber { background: #eff6ff; border-color: #3b82f6; }
.notif-item.info    { background: #f8fafc; border-color: #94a3b8; }

/* ── DRE ── */
.dre-row {
  display: flex; justify-content: space-between;
  padding: .4rem .75rem; border-radius: 7px; margin-bottom: 3px;
  font-size: .88rem;
}
.dre-receita { background: #f0fdf4; }
.dre-desc    { background: #fefce8; }
.dre-cmv     { background: #fff7ed; }
.dre-lucro   { background: #eff6ff; font-weight: 600; }
.dre-desp    { background: #fef2f2; }
.dre-total   { background: #0f172a; color: #fff; font-weight: 700; font-size: .95rem; }

/* ── Meta bar ── */
.meta-bar-wrap { background: #e2e8f0; border-radius: 99px; height: 8px; margin: .3rem 0; }
.meta-bar { background: #1d4ed8; border-radius: 99px; height: 8px; }

/* ── Misc ── */
.aux-row {
  display: flex; align-items: center; justify-content: space-between;
  padding: .5rem .8rem; border-radius: 8px; margin-bottom: 4px;
  background: #fff; border: 1px solid #e2e8f0; font-size: .86rem;
}
.aux-row.inativo { opacity: .5; border-style: dashed; }
.search-result {
  display: flex; align-items: center; gap: .6rem;
  padding: .5rem .8rem; border-radius: 8px; margin-bottom: 4px;
  background: #fff; border: 1px solid #e2e8f0;
}
.sr-type {
  font-size: .68rem; font-weight: 700; color: #1d4ed8;
  background: #dbeafe; padding: 2px 7px; border-radius: 6px;
  white-space: nowrap;
}
@media (max-width: 640px) {
  .page-header h1 { font-size: 1rem; }
  [data-testid="stMetricValue"] { font-size: 1.15rem; }
}
</style>
""", unsafe_allow_html=True)

# ── Ícone SVG helper (inline, sem emoji) ──────────────────────
ICONS = {
    "dashboard":  '<svg viewBox="0 0 24 24"><path d="M3 13h8V3H3v10zm0 8h8v-6H3v6zm10 0h8V11h-8v10zm0-18v6h8V3h-8z"/></svg>',
    "search":     '<svg viewBox="0 0 24 24"><path d="M15.5 14h-.79l-.28-.27A6.47 6.47 0 0 0 16 9.5 6.5 6.5 0 1 0 9.5 16a6.47 6.47 0 0 0 4.23-1.57l.27.28v.79l5 4.99L20.49 19l-4.99-5zm-6 0C7.01 14 5 11.99 5 9.5S7.01 5 9.5 5 14 7.01 14 9.5 11.99 14 9.5 14z"/></svg>',
    "cart":       '<svg viewBox="0 0 24 24"><path d="M7 18c-1.1 0-1.99.9-1.99 2S5.9 22 7 22s2-.9 2-2-.9-2-2-2zm10 0c-1.1 0-1.99.9-1.99 2S15.9 22 17 22s2-.9 2-2-.9-2-2-2zM7.17 14.75L7.2 14.63 8.1 13h7.45c.75 0 1.41-.41 1.75-1.03l3.86-7.01L19.42 4h-.01L18.24 6l-2.87 5.21H8.53L8.4 10.9 6.96 7.28 6.41 6 5.87 4.72H2V6h2l3.6 7.59L6.25 16H19v-2H7.42c-.13 0-.25-.06-.25-.25z"/></svg>',
    "quote":      '<svg viewBox="0 0 24 24"><path d="M14 2H6c-1.1 0-1.99.9-1.99 2L4 20c0 1.1.89 2 1.99 2H18c1.1 0 2-.9 2-2V8l-6-6zm2 16H8v-2h8v2zm0-4H8v-2h8v2zm-3-5V3.5L18.5 9H13z"/></svg>',
    "history":    '<svg viewBox="0 0 24 24"><path d="M13 3a9 9 0 1 0 9 9h-2a7 7 0 1 1-7-7v4l5-5-5-5v4z"/></svg>',
    "receivable": '<svg viewBox="0 0 24 24"><path d="M11.8 10.9c-2.27-.59-3-1.2-3-2.15 0-1.09 1.01-1.85 2.7-1.85 1.78 0 2.44.85 2.5 2.1h2.21c-.07-1.72-1.12-3.3-3.21-3.81V3h-3v2.16c-1.94.42-3.5 1.68-3.5 3.61 0 2.31 1.91 3.46 4.7 4.13 2.5.6 3 1.48 3 2.41 0 .69-.49 1.79-2.7 1.79-2.06 0-2.87-.92-2.98-2.1h-2.2c.12 2.19 1.76 3.42 3.68 3.83V21h3v-2.15c1.95-.37 3.5-1.5 3.5-3.55 0-2.84-2.43-3.81-4.7-4.4z"/></svg>',
    "expenses":   '<svg viewBox="0 0 24 24"><path d="M20 4H4c-1.11 0-2 .89-2 2v12c0 1.11.89 2 2 2h16c1.11 0 2-.89 2-2V6c0-1.11-.89-2-2-2zm0 14H4v-6h16v6zm0-10H4V6h16v2z"/></svg>',
    "categories": '<svg viewBox="0 0 24 24"><path d="M12 2l-5.5 9h11L12 2zm0 3.84L13.93 9h-3.87L12 5.84zM17.5 13c-2.49 0-4.5 2.01-4.5 4.5S15.01 22 17.5 22s4.5-2.01 4.5-4.5S19.99 13 17.5 13zm0 7c-1.38 0-2.5-1.12-2.5-2.5S16.12 15 17.5 15s2.5 1.12 2.5 2.5S18.88 20 17.5 20zM3 21.5h8v-8H3v8zm2-6h4v4H5v-4z"/></svg>',
    "payment":    '<svg viewBox="0 0 24 24"><path d="M20 4H4c-1.11 0-2 .89-2 2v12c0 1.11.89 2 2 2h16c1.11 0 2-.89 2-2V6c0-1.11-.89-2-2-2zm0 14H4v-6h16v6zm0-10H4V6h16v2z"/></svg>',
    "groups":     '<svg viewBox="0 0 24 24"><path d="M16 11c1.66 0 2.99-1.34 2.99-3S17.66 5 16 5c-1.66 0-3 1.34-3 3s1.34 3 3 3zm-8 0c1.66 0 2.99-1.34 2.99-3S9.66 5 8 5C6.34 5 5 6.34 5 8s1.34 3 3 3zm0 2c-2.33 0-7 1.17-7 3.5V19h14v-2.5c0-2.33-4.67-3.5-7-3.5zm8 0c-.29 0-.62.02-.97.05 1.16.84 1.97 1.97 1.97 3.45V19h6v-2.5c0-2.33-4.67-3.5-7-3.5z"/></svg>',
    "reps":       '<svg viewBox="0 0 24 24"><path d="M12 12c2.21 0 4-1.79 4-4s-1.79-4-4-4-4 1.79-4 4 1.79 4 4 4zm0 2c-2.67 0-8 1.34-8 4v2h16v-2c0-2.66-5.33-4-8-4z"/></svg>',
    "supervisor": '<svg viewBox="0 0 24 24"><path d="M16 11c1.66 0 2.99-1.34 2.99-3S17.66 5 16 5c-1.66 0-3 1.34-3 3s1.34 3 3 3zm-8 0c1.66 0 2.99-1.34 2.99-3S9.66 5 8 5C6.34 5 5 6.34 5 8s1.34 3 3 3zm0 2c-2.33 0-7 1.17-7 3.5V19h14v-2.5c0-2.33-4.67-3.5-7-3.5zm8 0c-.29 0-.62.02-.97.05 1.16.84 1.97 1.97 1.97 3.45V19h6v-2.5c0-2.33-4.67-3.5-7-3.5z"/></svg>',
    "supplier":   '<svg viewBox="0 0 24 24"><path d="M20 8h-3V4H3c-1.1 0-2 .9-2 2v11h2c0 1.66 1.34 3 3 3s3-1.34 3-3h6c0 1.66 1.34 3 3 3s3-1.34 3-3h2v-5l-3-4zM6 18.5c-.83 0-1.5-.67-1.5-1.5S5.17 15.5 6 15.5s1.5.67 1.5 1.5-.67 1.5-1.5 1.5zm13.5-9.5 1.96 2.5H17V9h2.5zm-1.5 9.5c-.83 0-1.5-.67-1.5-1.5s.67-1.5 1.5-1.5 1.5.67 1.5 1.5-.67 1.5-1.5 1.5z"/></svg>',
    "clients":    '<svg viewBox="0 0 24 24"><path d="M12 12c2.21 0 4-1.79 4-4s-1.79-4-4-4-4 1.79-4 4 1.79 4 4 4zm0 2c-2.67 0-8 1.34-8 4v2h16v-2c0-2.66-5.33-4-8-4z"/></svg>',
    "stock":      '<svg viewBox="0 0 24 24"><path d="M20 6h-2.18c.07-.44.18-.88.18-1.35C18 2.99 16.41 1.5 14.5 1.5c-1.74 0-2.87 1.02-3.5 2.23C10.37 2.52 9.24 1.5 7.5 1.5 5.59 1.5 4 2.99 4 4.65c0 .47.11.91.18 1.35H2c-1.1 0-2 .9-2 2v14c0 1.1.9 2 2 2h18c1.1 0 2-.9 2-2V8c0-1.1-.9-2-2-2z"/></svg>',
    "nf":         '<svg viewBox="0 0 24 24"><path d="M19 3H5c-1.11 0-2 .9-2 2v14c0 1.1.89 2 2 2h14c1.1 0 2-.9 2-2V5c0-1.1-.9-2-2-2zm-7 3c1.93 0 3.5 1.57 3.5 3.5S13.93 13 12 13s-3.5-1.57-3.5-3.5S10.07 6 12 6zm7 13H5v-.23c0-.62.28-1.2.76-1.58C7.47 15.82 9.64 15 12 15s4.53.82 6.24 2.19c.48.38.76.97.76 1.58V19z"/></svg>',
    "commission": '<svg viewBox="0 0 24 24"><path d="M12 1L3 5v6c0 5.55 3.84 10.74 9 12 5.16-1.26 9-6.45 9-12V5l-9-4zm-1 6h2v2h-2V7zm0 4h2v6h-2v-6z"/></svg>',
    "notif":      '<svg viewBox="0 0 24 24"><path d="M12 22c1.1 0 2-.9 2-2h-4c0 1.1.9 2 2 2zm6-6v-5c0-3.07-1.64-5.64-4.5-6.32V4c0-.83-.67-1.5-1.5-1.5s-1.5.67-1.5 1.5v.68C7.63 5.36 6 7.92 6 11v5l-2 2v1h16v-1l-2-2z"/></svg>',
    "log":        '<svg viewBox="0 0 24 24"><path d="M14 2H6c-1.1 0-2 .9-2 2v16c0 1.1.9 2 2 2h12c1.1 0 2-.9 2-2V8l-6-6zm2 16H8v-2h8v2zm0-4H8v-2h8v2zm-3-5V3.5L18.5 9H13z"/></svg>',
    "settings":   '<svg viewBox="0 0 24 24"><path d="M19.14 12.94c.04-.3.06-.61.06-.94 0-.32-.02-.64-.07-.94l2.03-1.58c.18-.14.23-.41.12-.61l-1.92-3.32c-.12-.22-.37-.29-.59-.22l-2.39.96c-.5-.38-1.03-.7-1.62-.94l-.36-2.54c-.04-.24-.24-.41-.48-.41h-3.84c-.24 0-.43.17-.47.41l-.36 2.54c-.59.24-1.13.57-1.62.94l-2.39-.96c-.22-.08-.47 0-.59.22L2.74 8.87c-.12.21-.08.47.12.61l2.03 1.58c-.05.3-.09.63-.09.94s.02.64.07.94l-2.03 1.58c-.18.14-.23.41-.12.61l1.92 3.32c.12.22.37.29.59.22l2.39-.96c.5.38 1.03.7 1.62.94l.36 2.54c.05.24.24.41.48.41h3.84c.24 0 .44-.17.47-.41l.36-2.54c.59-.24 1.13-.56 1.62-.94l2.39.96c.22.08.47 0 .59-.22l1.92-3.32c.12-.22.07-.47-.12-.61l-2.01-1.58zM12 15.6c-1.98 0-3.6-1.62-3.6-3.6s1.62-3.6 3.6-3.6 3.6 1.62 3.6 3.6-1.62 3.6-3.6 3.6z"/></svg>',
    "edit":       '<svg viewBox="0 0 24 24"><path d="M3 17.25V21h3.75L17.81 9.94l-3.75-3.75L3 17.25zM20.71 7.04c.39-.39.39-1.02 0-1.41l-2.34-2.34c-.39-.39-1.02-.39-1.41 0l-1.83 1.83 3.75 3.75 1.83-1.83z"/></svg>',
    "delete":     '<svg viewBox="0 0 24 24"><path d="M6 19c0 1.1.9 2 2 2h8c1.1 0 2-.9 2-2V7H6v12zM19 4h-3.5l-1-1h-5l-1 1H5v2h14V4z"/></svg>',
    "check":      '<svg viewBox="0 0 24 24"><path d="M9 16.17L4.83 12l-1.42 1.41L9 19 21 7l-1.41-1.41L9 16.17z"/></svg>',
    "close":      '<svg viewBox="0 0 24 24"><path d="M19 6.41L17.59 5 12 10.59 6.41 5 5 6.41 10.59 12 5 17.59 6.41 19 12 13.41 17.59 19 19 17.59 13.41 12 19 6.41z"/></svg>',
    "add":        '<svg viewBox="0 0 24 24"><path d="M19 13h-6v6h-2v-6H5v-2h6V5h2v6h6v2z"/></svg>',
    "download":   '<svg viewBox="0 0 24 24"><path d="M5 20h14v-2H5v2zM19 9h-4V3H9v6H5l7 7 7-7z"/></svg>',
    "upload":     '<svg viewBox="0 0 24 24"><path d="M9 16h6v-6h4l-7-7-7 7h4v6zm-4 2h14v2H5v-2z"/></svg>',
    "cancel":     '<svg viewBox="0 0 24 24"><path d="M12 2C6.47 2 2 6.47 2 12s4.47 10 10 10 10-4.47 10-10S17.53 2 12 2zm5 13.59L15.59 17 12 13.41 8.41 17 7 15.59 10.59 12 7 8.41 8.41 7 12 10.59 15.59 7 17 8.41 13.41 12 17 15.59z"/></svg>',
    "reactivate": '<svg viewBox="0 0 24 24"><path d="M12 5V1L7 6l5 5V7c3.31 0 6 2.69 6 6s-2.69 6-6 6-6-2.69-6-6H4c0 4.42 3.58 8 8 8s8-3.58 8-8-3.58-8-8-8z"/></svg>',
    "convert":    '<svg viewBox="0 0 24 24"><path d="M19 8l-4 4h3c0 3.31-2.69 6-6 6-1.01 0-1.97-.25-2.8-.7l-1.46 1.46C8.97 19.54 10.43 20 12 20c4.42 0 8-3.58 8-8h3l-4-4zM6 12c0-3.31 2.69-6 6-6 1.01 0 1.97.25 2.8.7l1.46-1.46C15.03 4.46 13.57 4 12 4c-4.42 0-8 3.58-8 8H1l4 4 4-4H6z"/></svg>',
    "inactive":   '<svg viewBox="0 0 24 24"><circle cx="12" cy="12" r="10" fill="none" stroke="currentColor" stroke-width="2"/><path d="M8 12h8"/></svg>',
    "active":     '<svg viewBox="0 0 24 24"><circle cx="12" cy="12" r="10"/><path d="M9 12l2 2 4-4" stroke="#fff" stroke-width="2" fill="none"/></svg>',
}

def icon_btn(key, icon_name, tip=""):
    """Retorna se o botão SVG foi clicado"""
    return st.button(f'<span title="{tip}">{ICONS.get(icon_name,"")}</span>',
                     key=key, help=tip, unsafe_allow_html=True)

def icon_html(name, size=18, color="#1d4ed8"):
    svg = ICONS.get(name, "")
    return f'<span style="display:inline-flex;align-items:center;width:{size}px;height:{size}px;fill:{color}">{svg}</span>'

def ph_icon(name):
    return f'<div class="ph-icon">{icon_html(name,18,"#fff")}</div>'

# ── UI helpers ──────────────────────────────────────────────────
def show_ok(t, h=""):   st.markdown(f'<div class="toast ok"><b>{t}</b> {h}</div>', unsafe_allow_html=True)
def show_err(t, h=""):  st.markdown(f'<div class="toast err"><b>{t}</b> {h}</div>', unsafe_allow_html=True)
def show_wrn(t, h=""):  st.markdown(f'<div class="toast wrn"><b>{t}</b> {h}</div>', unsafe_allow_html=True)
def show_inf(t, h=""):  st.markdown(f'<div class="toast inf"><b>{t}</b> {h}</div>', unsafe_allow_html=True)

def page_header(icon_name, title, subtitle=""):
    sub = f"<p>{subtitle}</p>" if subtitle else ""
    st.markdown(
        f'<div class="page-header">{ph_icon(icon_name)}'
        f'<div><h1>{title}</h1>{sub}</div></div>',
        unsafe_allow_html=True)

def hr(): st.markdown('<hr class="erp">', unsafe_allow_html=True)

def badge(txt, cls="b-gray"):
    return f'<span class="badge {cls}">{txt}</span>'

# ── Pool de conexões (INALTERADO) ───────────────────────────────
@st.cache_resource
def get_pool():
    db = st.secrets["db"]
    dsn = (f"postgresql://{db['user']}:{db['password']}"
           f"@{db['host']}:{db.get('port',5432)}/{db['dbname']}"
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
                if fetch:     r = cur.fetchall(); conn.commit(); return r
                if returning: r = cur.fetchone(); conn.commit(); return r
                conn.commit(); return True
    except pg_errors.UniqueViolation: return "duplicate"
    except Exception as e: st.session_state["_dberr"] = str(e); return False

# ── Helpers ─────────────────────────────────────────────────────
def eid():
    v = st.session_state.get("empresa_id")
    if not v: st.error("Sessão inválida."); st.stop()
    return v

def qry(sql, params=(), fetch=False, returning=False):
    return run_query(sql, params, fetch=fetch, returning=returning)

def validate_doc(d): return len(re.sub(r'\D','',d)) in (11,14)
def validate_required(*f): return all(x is not None and str(x).strip() for x in f)
def hash_pw(s): return hashlib.sha256(s.encode()).hexdigest()

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

def log_acao(acao, det=""):
    qry("INSERT INTO log_acoes(empresa_id,usuario_nome,acao,detalhes) VALUES(%s,%s,%s,%s)",
        (eid(), st.session_state.get("usuario_nome","?"), acao, det))

def criar_notif(titulo, msg, tipo="info"):
    qry("INSERT INTO notificacoes(empresa_id,titulo,mensagem,tipo) VALUES(%s,%s,%s,%s)",
        (eid(), titulo, msg, tipo))

def pode(perm):
    perfil = st.session_state.get("usuario_perfil","operador")
    if perfil == "admin": return True
    return perm not in ["log","despesas_del","comissoes_pagar"]

# ── @st.cache_data para dados relativamente estáticos ──────────
# TTL curto (30s) para equilibrar frescor vs performance
@st.cache_data(ttl=30)
def get_categorias(empresa_id):
    return qry("SELECT nome FROM categorias WHERE empresa_id=%s AND ativo=TRUE ORDER BY nome",
               (empresa_id,), fetch=True) or []

@st.cache_data(ttl=30)
def get_pagamentos(empresa_id):
    return qry("SELECT nome FROM pagamentos WHERE empresa_id=%s AND ativo=TRUE ORDER BY nome",
               (empresa_id,), fetch=True) or []

@st.cache_data(ttl=30)
def get_supervisores(empresa_id):
    return qry("SELECT id,nome,comissao_pct FROM supervisores WHERE empresa_id=%s AND ativo=TRUE ORDER BY nome",
               (empresa_id,), fetch=True) or []

@st.cache_data(ttl=30)
def get_representantes(empresa_id):
    return qry("SELECT id,nome,comissao_pct FROM representantes WHERE empresa_id=%s AND ativo=TRUE ORDER BY nome",
               (empresa_id,), fetch=True) or []

@st.cache_data(ttl=30)
def get_grupos(empresa_id):
    return qry("SELECT id,nome,desconto_padrao FROM grupos_clientes WHERE empresa_id=%s AND ativo=TRUE ORDER BY nome",
               (empresa_id,), fetch=True) or []

@st.cache_data(ttl=15)
def get_produtos_ativos(empresa_id):
    return qry("""SELECT id,sku,nome,categoria,preco_custo,preco_venda,
                         estoque_atual,estoque_minimo,ativo,codigo_barras
                  FROM produtos WHERE empresa_id=%s ORDER BY nome""",
               (empresa_id,), fetch=True) or []

@st.cache_data(ttl=15)
def get_clientes_ativos(empresa_id):
    return qry("""SELECT c.id,c.nome,c.documento,c.telefone,c.email,
                         c.rua,c.numero,c.complemento,c.bairro,c.cidade,
                         c.estado,c.cep,c.ativo,COALESCE(g.nome,'—')
                  FROM clientes c LEFT JOIN grupos_clientes g ON g.id=c.grupo_id
                  WHERE c.empresa_id=%s ORDER BY c.nome""",
               (empresa_id,), fetch=True) or []

def invalidar_cache():
    """Invalida caches de dados mutáveis após escrita."""
    get_produtos_ativos.clear()
    get_clientes_ativos.clear()
    get_categorias.clear()
    get_pagamentos.clear()
    get_supervisores.clear()
    get_representantes.clear()
    get_grupos.clear()

# ── Init DB ─────────────────────────────────────────────────────
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
        """CREATE TABLE IF NOT EXISTS movimentacoes_estoque(id SERIAL PRIMARY KEY,empresa_id INTEGER NOT NULL REFERENCES empresas(id),produto_id INTEGER NOT NULL REFERENCES produtos(id),tipo TEXT NOT NULL,quantidade INTEGER NOT NULL,motivo TEXT,usuario_nome TEXT,criado_em TIMESTAMP DEFAULT NOW())""",
        """CREATE TABLE IF NOT EXISTS vendas(id SERIAL PRIMARY KEY,empresa_id INTEGER NOT NULL REFERENCES empresas(id),data TIMESTAMP NOT NULL DEFAULT NOW(),cliente_name TEXT,valor_bruto NUMERIC(12,2) DEFAULT 0,desconto_pct NUMERIC(5,2) DEFAULT 0,desconto_val NUMERIC(12,2) DEFAULT 0,valor_total NUMERIC(12,2),pagamento TEXT,status TEXT DEFAULT 'Pago',observacao TEXT DEFAULT '',supervisor_id INTEGER REFERENCES supervisores(id),representante_id INTEGER REFERENCES representantes(id),tipo TEXT DEFAULT 'Venda',vencimento DATE,parcelas INTEGER DEFAULT 1,comissao_supervisor NUMERIC(12,2) DEFAULT 0,comissao_representante NUMERIC(12,2) DEFAULT 0,comissao_status TEXT DEFAULT 'Pendente')""",
        """CREATE TABLE IF NOT EXISTS itens_venda(id SERIAL PRIMARY KEY,empresa_id INTEGER NOT NULL REFERENCES empresas(id),venda_id INTEGER NOT NULL REFERENCES vendas(id),produto_nome TEXT,quantidade INTEGER,preco_unit NUMERIC(12,2))""",
        """CREATE TABLE IF NOT EXISTS despesas(id SERIAL PRIMARY KEY,empresa_id INTEGER NOT NULL REFERENCES empresas(id),descricao TEXT NOT NULL,categoria TEXT,valor NUMERIC(12,2) NOT NULL,data DATE NOT NULL DEFAULT CURRENT_DATE,status TEXT DEFAULT 'Pago',observacao TEXT)""",
        """CREATE TABLE IF NOT EXISTS contas_receber(id SERIAL PRIMARY KEY,empresa_id INTEGER NOT NULL REFERENCES empresas(id),venda_id INTEGER REFERENCES vendas(id),cliente_name TEXT,descricao TEXT,valor NUMERIC(12,2) NOT NULL,vencimento DATE NOT NULL,status TEXT DEFAULT 'Pendente',data_pagamento DATE,parcela INTEGER DEFAULT 1,total_parcelas INTEGER DEFAULT 1)""",
        """CREATE TABLE IF NOT EXISTS entradas_nf(id SERIAL PRIMARY KEY,empresa_id INTEGER NOT NULL REFERENCES empresas(id),numero_nf TEXT,fornecedor_nome TEXT,data DATE NOT NULL DEFAULT CURRENT_DATE,valor_total NUMERIC(12,2) DEFAULT 0,observacao TEXT,criado_em TIMESTAMP DEFAULT NOW())""",
        """CREATE TABLE IF NOT EXISTS itens_entrada_nf(id SERIAL PRIMARY KEY,empresa_id INTEGER NOT NULL REFERENCES empresas(id),entrada_id INTEGER NOT NULL REFERENCES entradas_nf(id),produto_id INTEGER REFERENCES produtos(id),produto_nome TEXT,quantidade INTEGER,preco_custo NUMERIC(12,2))""",
        """CREATE TABLE IF NOT EXISTS log_acoes(id SERIAL PRIMARY KEY,empresa_id INTEGER NOT NULL,usuario_nome TEXT,acao TEXT NOT NULL,detalhes TEXT,criado_em TIMESTAMP DEFAULT NOW())""",
        """CREATE TABLE IF NOT EXISTS notificacoes(id SERIAL PRIMARY KEY,empresa_id INTEGER NOT NULL,titulo TEXT NOT NULL,mensagem TEXT,tipo TEXT DEFAULT 'info',lida BOOLEAN DEFAULT FALSE,criado_em TIMESTAMP DEFAULT NOW())""",
        """CREATE TABLE IF NOT EXISTS recuperacao_senha(id SERIAL PRIMARY KEY,email TEXT NOT NULL,token TEXT NOT NULL,expira_em TIMESTAMP NOT NULL,usado BOOLEAN DEFAULT FALSE)""",
    ]
    migracoes = [
        ("vendas","valor_bruto","NUMERIC(12,2) DEFAULT 0"),
        ("vendas","desconto_pct","NUMERIC(5,2) DEFAULT 0"),
        ("vendas","desconto_val","NUMERIC(12,2) DEFAULT 0"),
        ("vendas","supervisor_id","INTEGER"),
        ("vendas","representante_id","INTEGER"),
        ("vendas","tipo","TEXT DEFAULT 'Venda'"),
        ("vendas","vencimento","DATE"),
        ("vendas","parcelas","INTEGER DEFAULT 1"),
        ("vendas","comissao_supervisor","NUMERIC(12,2) DEFAULT 0"),
        ("vendas","comissao_representante","NUMERIC(12,2) DEFAULT 0"),
        ("vendas","comissao_status","TEXT DEFAULT 'Pendente'"),
        ("empresas","moeda","TEXT DEFAULT 'R$'"),
        ("supervisores","comissao_pct","NUMERIC(5,2) DEFAULT 0"),
        ("representantes","comissao_pct","NUMERIC(5,2) DEFAULT 0"),
        ("representantes","regiao","TEXT"),
        ("clientes","grupo_id","INTEGER"),
        ("produtos","codigo_barras","TEXT"),
        ("contas_receber","parcela","INTEGER DEFAULT 1"),
        ("contas_receber","total_parcelas","INTEGER DEFAULT 1"),
    ]
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                for s in stmts: cur.execute(s)
                for tbl, col, tipo in migracoes:
                    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name=%s AND column_name=%s",(tbl,col))
                    if not cur.fetchone(): cur.execute(f"ALTER TABLE {tbl} ADD COLUMN {col} {tipo}")
            conn.commit()
    except Exception as e: st.error(f"Erro banco: {e}"); st.stop()

# ── Session State defaults ───────────────────────────────────────
_D = {
    "logado": False,"usuario_id": None,"empresa_id": None,
    "usuario_nome": "","empresa_nome": "","empresa_moeda": "R$",
    "usuario_perfil": "operador","active_menu": "Dashboard",
    "cart": [],"nf_itens": [],
    "editing_prod_id": None,"adj_prod": None,
    "edit_cat_id": None,"edit_pag_id": None,
    "edit_cli_id": None,"edit_sup_id": None,"edit_rep_id": None,
    "edit_forn_id": None,"edit_grupo_id": None,
    "edit_orc_id": None,
}
for k, v in _D.items():
    if k not in st.session_state: st.session_state[k] = v

def reset_editing():
    for k in ["editing_prod_id","adj_prod","edit_cat_id","edit_pag_id",
              "edit_cli_id","edit_sup_id","edit_rep_id","edit_forn_id",
              "edit_grupo_id","edit_orc_id"]:
        st.session_state[k] = None

# ══════════════════════════════════════
# TELA DE LOGIN
# ══════════════════════════════════════
def tela_login():
    st.markdown('<div class="login-box"><div class="login-title">ERP <span>Pro</span></div><div class="login-sub">Acesse sua conta</div></div>', unsafe_allow_html=True)
    _, col, _ = st.columns([1, 2, 1])
    with col:
        tab_login, tab_reset, tab_cadastro = st.tabs(["Entrar", "Recuperar senha", "Novo cliente"])

        with tab_login:
            with st.form("form_login"):
                email = st.text_input("E-mail")
                senha = st.text_input("Senha", type="password")
                entrar = st.form_submit_button("Entrar", use_container_width=True)
            if entrar:
                if not validate_required(email, senha): show_err("Preencha e-mail e senha."); return
                row = run_query(
                    "SELECT u.id,u.nome,u.perfil,u.ativo,e.id,e.nome,e.ativo,COALESCE(e.moeda,'R$') "
                    "FROM usuarios u JOIN empresas e ON e.id=u.empresa_id "
                    "WHERE u.email=%s AND u.senha_hash=%s",
                    (email.strip().lower(), hash_pw(senha)), fetch=True)
                if not row: show_err("E-mail ou senha incorretos."); return
                uid, unome, uperfil, uativo, empid, empnome, empativo, moeda = row[0]
                if not uativo: show_err("Usuário inativo."); return
                if not empativo: show_err("Empresa inativa."); return
                st.session_state.update({
                    "logado": True,"usuario_id": uid,"empresa_id": empid,
                    "usuario_nome": unome,"empresa_nome": empnome,
                    "empresa_moeda": moeda,"usuario_perfil": uperfil
                })
                st.rerun()

        with tab_reset:
            st.caption("Informe o e-mail. O administrador usará o token gerado para redefinir a senha em Configurações.")
            with st.form("form_reset"):
                email_r = st.text_input("E-mail cadastrado")
                gerar = st.form_submit_button("Gerar token", use_container_width=True)
            if gerar:
                usr = run_query("SELECT id FROM usuarios WHERE email=%s AND ativo=TRUE", (email_r.strip().lower(),), fetch=True)
                if usr:
                    import secrets
                    tok = secrets.token_hex(8)
                    expira = datetime.now() + timedelta(hours=2)
                    run_query("INSERT INTO recuperacao_senha(email,token,expira_em) VALUES(%s,%s,%s)", (email_r.strip().lower(), tok, expira))
                    show_ok(f"Token: {tok}", "— Válido por 2h.")
                else: show_err("E-mail não encontrado.")

        with tab_cadastro:
            st.caption("Cadastro de nova empresa e usuário administrador.")
            with st.form("form_novo_cliente"):
                nc1, nc2 = st.columns(2)
                emp_nome = nc1.text_input("Nome da empresa *")
                adm_nome = nc2.text_input("Seu nome *")
                adm_email = st.text_input("E-mail do admin *")
                adm_senha = st.text_input("Senha *", type="password")
                criar = st.form_submit_button("Criar conta", use_container_width=True)
            if criar:
                if not validate_required(emp_nome, adm_nome, adm_email, adm_senha):
                    show_err("Preencha todos os campos."); return
                emp_row = run_query("INSERT INTO empresas(nome) VALUES(%s) RETURNING id", (emp_nome.strip(),), returning=True)
                if emp_row:
                    eid_new = emp_row[0]
                    res = run_query("INSERT INTO usuarios(empresa_id,nome,email,senha_hash,perfil) VALUES(%s,%s,%s,%s,'admin')",
                                    (eid_new, adm_nome.strip(), adm_email.strip().lower(), hash_pw(adm_senha)))
                    if res is True: show_ok("Conta criada!", "Faça login na aba Entrar.")
                    elif res == "duplicate": show_err("E-mail já cadastrado.")
                    else: show_err("Erro ao criar usuário.")
                else: show_err("Erro ao criar empresa.")

init_db()
if not st.session_state.logado:
    tela_login(); st.stop()

EMPRESA_ID = eid()
cur_sym = st.session_state.empresa_moeda

# ── Alertas automáticos (1x por sessão via cache) ───────────────
@st.cache_data(ttl=300)
def _verificar_alertas(empresa_id):
    low = qry("SELECT nome FROM produtos WHERE empresa_id=%s AND ativo=TRUE AND estoque_atual<=estoque_minimo", (empresa_id,), fetch=True)
    if low:
        ex = qry("SELECT COUNT(*) FROM notificacoes WHERE empresa_id=%s AND tipo='estoque' AND lida=FALSE", (empresa_id,), fetch=True)
        if ex and ex[0][0] == 0:
            qry("INSERT INTO notificacoes(empresa_id,titulo,mensagem,tipo) VALUES(%s,%s,%s,'estoque')",
                (empresa_id, f"{len(low)} produto(s) com estoque baixo", ", ".join(p[0] for p in low[:5])))
    venc = qry("SELECT COUNT(*) FROM contas_receber WHERE empresa_id=%s AND status='Pendente' AND vencimento<=CURRENT_DATE", (empresa_id,), fetch=True)
    if venc and venc[0][0] > 0:
        ex2 = qry("SELECT COUNT(*) FROM notificacoes WHERE empresa_id=%s AND tipo='receber' AND lida=FALSE", (empresa_id,), fetch=True)
        if ex2 and ex2[0][0] == 0:
            qry("INSERT INTO notificacoes(empresa_id,titulo,mensagem,tipo) VALUES(%s,%s,%s,'receber')",
                (empresa_id, f"{venc[0][0]} conta(s) a receber vencida(s)", "Verifique Contas a Receber."))
    return True

_verificar_alertas(EMPRESA_ID)

notif_count_r = qry("SELECT COUNT(*) FROM notificacoes WHERE empresa_id=%s AND lida=FALSE", (EMPRESA_ID,), fetch=True)
NOTIF_COUNT = notif_count_r[0][0] if notif_count_r else 0

# ── Sidebar ─────────────────────────────────────────────────────
MENU_STRUCTURE = [
    ("Vendas", [
        ("Dashboard",           "dashboard"),
        ("Busca Global",        "search"),
        ("Pedidos",             "cart"),
        ("Orçamentos",          "quote"),
        ("Histórico de Pedidos","history"),
    ]),
    ("Financeiro", [
        ("Contas a Receber",    "receivable"),
        ("Despesas",            "expenses"),
    ]),
    ("Cadastros", [
        ("Categorias",          "categories"),
        ("Formas de Pagamento", "payment"),
        ("Grupos de Clientes",  "groups"),
        ("Representantes",      "reps"),
        ("Supervisores",        "supervisor"),
        ("Fornecedores",        "supplier"),
        ("Clientes",            "clients"),
    ]),
    ("Movimentação", [
        ("Estoque",             "stock"),
        ("Entrada NF",          "nf"),
        ("Comissões",           "commission"),
    ]),
    ("Sistema", [
        ("Notificações",        "notif"),
        ("Log de Ações",        "log"),
        ("Configurações",       "settings"),
    ]),
]

with st.sidebar:
    st.markdown(f'<span class="nav-brand">ERP <span>Pro</span></span>', unsafe_allow_html=True)
    st.markdown(f'<span class="nav-tenant">{st.session_state.empresa_nome}</span>', unsafe_allow_html=True)

    for group_label, items in MENU_STRUCTURE:
        st.markdown(f'<span class="nav-group">{group_label}</span>', unsafe_allow_html=True)
        for label, icon_key in items:
            is_active = st.session_state.active_menu == label
            dot = '<span class="notif-dot"></span>' if label == "Notificações" and NOTIF_COUNT > 0 else ""
            with st.container():
                if is_active: st.markdown('<div class="nav-active">', unsafe_allow_html=True)
                btn_label = f"{label}{dot}"
                if st.button(btn_label, key=f"nav_{icon_key}", use_container_width=True):
                    st.session_state.active_menu = label
                    reset_editing()
                    st.rerun()
                if is_active: st.markdown('</div>', unsafe_allow_html=True)

    n_cart = len(st.session_state.cart)
    if n_cart:
        st.markdown(f'<div style="margin:.5rem .75rem;padding:.35rem .6rem;background:#1e293b;border-radius:6px;font-size:.78rem;color:#93c5fd!important">{n_cart} item(s) no carrinho</div>', unsafe_allow_html=True)

    st.divider()
    st.markdown(f'<span class="nav-user">{st.session_state.usuario_nome} · {st.session_state.usuario_perfil}</span>', unsafe_allow_html=True)
    with st.container():
        st.markdown('<div class="logout-btn">', unsafe_allow_html=True)
        if st.button("Sair", key="btn_logout", use_container_width=True):
            for k in list(st.session_state.keys()): del st.session_state[k]
            st.rerun()
        st.markdown('</div>', unsafe_allow_html=True)
    st.markdown(f"<small style='color:#334155;padding:.5rem .75rem;display:block'>v9.0</small>", unsafe_allow_html=True)

menu = st.session_state.active_menu

# ══════════════════════════════════════
# DASHBOARD
# ══════════════════════════════════════
if menu == "Dashboard":
    page_header("dashboard", "Dashboard", f"Bem-vindo, {st.session_state.usuario_nome}")

    # Busca todos os KPIs em UMA query agregada
    kpi = qry("""
        SELECT
          COALESCE(SUM(CASE WHEN status='Pago' AND tipo='Venda' THEN valor_total ELSE 0 END),0) AS fat,
          COUNT(CASE WHEN status='Pago' AND tipo='Venda' THEN 1 END) AS qtd
        FROM vendas WHERE empresa_id=%s
    """, (EMPRESA_ID,), fetch=True)
    tf = float(kpi[0][0]) if kpi else 0.0
    qp = int(kpi[0][1]) if kpi else 0
    at = tf/qp if qp else 0.0

    desp = qry("SELECT COALESCE(SUM(valor),0) FROM despesas WHERE empresa_id=%s AND status='Pago'", (EMPRESA_ID,), fetch=True)
    total_desp = float(desp[0][0]) if desp else 0.0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Faturamento Total", f"{cur_sym} {tf:,.2f}")
    c2.metric("Pedidos", qp)
    c3.metric("Ticket Médio", f"{cur_sym} {at:,.2f}")
    c4.metric("Lucro Líq. Est.", f"{cur_sym} {(tf-total_desp):,.2f}")

    hr()

    if HAS_PANDAS:
        meses = qry("""
            SELECT TO_CHAR(data,'MM/YYYY'), SUM(valor_total)
            FROM vendas WHERE empresa_id=%s AND status='Pago' AND tipo='Venda'
            AND data >= NOW() - INTERVAL '6 months'
            GROUP BY TO_CHAR(data,'MM/YYYY'), DATE_TRUNC('month',data)
            ORDER BY DATE_TRUNC('month',data)
        """, (EMPRESA_ID,), fetch=True)
        if meses:
            st.markdown("**Faturamento — últimos 6 meses**")
            df_m = pd.DataFrame(meses, columns=["Mês","Valor"])
            st.bar_chart(df_m.set_index("Mês"))

    hr()
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Últimas vendas**")
        ul = qry("""SELECT v.id,v.cliente_name,v.valor_total,
                           STRING_AGG(i.produto_nome||' x'||i.quantidade,', '),
                           TO_CHAR(v.data,'DD/MM HH24:MI')
                    FROM vendas v
                    LEFT JOIN itens_venda i ON i.venda_id=v.id AND i.empresa_id=v.empresa_id
                    WHERE v.empresa_id=%s AND v.status='Pago' AND v.tipo='Venda'
                    GROUP BY v.id ORDER BY v.data DESC LIMIT 5""", (EMPRESA_ID,), fetch=True)
        if ul:
            for vid, cli, val, ps, df_v in ul:
                st.markdown(f'''<div class="card">
                  <div style="display:flex;justify-content:space-between;align-items:center">
                    <div>
                      <div class="card-label">#{vid} · {df_v}</div>
                      <div class="card-title">{cli}</div>
                      <div class="card-sub">{ps or "—"}</div>
                    </div>
                    <div class="card-val">{cur_sym} {float(val):,.2f}</div>
                  </div></div>''', unsafe_allow_html=True)
        else: show_inf("Nenhuma venda registrada ainda.")

    with col2:
        st.markdown("**Contas a receber próximas**")
        cr = qry("""SELECT cliente_name,valor,vencimento FROM contas_receber
                    WHERE empresa_id=%s AND status='Pendente'
                    ORDER BY vencimento LIMIT 5""", (EMPRESA_ID,), fetch=True)
        if cr:
            for cli, val, venc_d in cr:
                dias = (venc_d - date.today()).days if venc_d else 0
                cls = "b-red" if dias < 0 else ("b-yellow" if dias <= 3 else "b-green")
                lbl = "Vencido" if dias < 0 else f"{dias}d"
                st.markdown(f'''<div class="card">
                  <div style="display:flex;justify-content:space-between;align-items:center">
                    <div>
                      <div class="card-title">{cli}</div>
                      <div class="card-sub">Vence {venc_d.strftime("%d/%m/%Y") if venc_d else "—"} {badge(lbl,cls)}</div>
                    </div>
                    <div class="card-val">{cur_sym} {float(val):,.2f}</div>
                  </div></div>''', unsafe_allow_html=True)
        else: show_inf("Nenhuma pendência.")

    low = qry("SELECT nome FROM produtos WHERE empresa_id=%s AND ativo=TRUE AND estoque_atual<=estoque_minimo", (EMPRESA_ID,), fetch=True)
    if low: hr(); show_wrn(f"{len(low)} produto(s) com estoque baixo.", "Verifique Estoque.")

# ══════════════════════════════════════
# BUSCA GLOBAL
# ══════════════════════════════════════
elif menu == "Busca Global":
    page_header("search","Busca Global","Encontre qualquer registro no sistema")
    busca = st.text_input("Buscar", placeholder="Nome, SKU, CPF, número do pedido...")

    if busca and len(busca) >= 2:
        b = f"%{busca}%"
        resultados = []
        # Executa todas as buscas de uma vez
        clis = qry("SELECT nome,documento,cidade FROM clientes WHERE empresa_id=%s AND (nome ILIKE %s OR documento ILIKE %s)", (EMPRESA_ID,b,b), fetch=True)
        for cn,cdoc,ccid in (clis or []): resultados.append(("Cliente",cn,f"{cdoc} · {ccid or '—'}","Clientes"))
        prods = qry("SELECT nome,sku,categoria FROM produtos WHERE empresa_id=%s AND (nome ILIKE %s OR sku ILIKE %s)", (EMPRESA_ID,b,b), fetch=True)
        for pn,psk,pcat in (prods or []): resultados.append(("Produto",pn,f"SKU: {psk} · {pcat}","Estoque"))
        if busca.isdigit():
            vs = qry("SELECT id,cliente_name,valor_total,status FROM vendas WHERE empresa_id=%s AND id=%s", (EMPRESA_ID,int(busca)), fetch=True)
            for vid,vcli,vval,vst in (vs or []): resultados.append(("Pedido",f"#{vid} — {vcli}",f"{cur_sym} {float(vval):,.2f} · {vst}","Histórico de Pedidos"))
        fns = qry("SELECT nome,email FROM fornecedores WHERE empresa_id=%s AND nome ILIKE %s", (EMPRESA_ID,b), fetch=True)
        for fn,fe in (fns or []): resultados.append(("Fornecedor",fn,fe or "—","Fornecedores"))

        if resultados:
            st.markdown(f"**{len(resultados)} resultado(s)**")
            for tipo, nome, sub, dest in resultados:
                cr_col, cb_col = st.columns([9,1])
                cr_col.markdown(f'<div class="search-result"><span class="sr-type">{tipo}</span><div><b style="font-size:.88rem">{nome}</b><div style="font-size:.73rem;color:#64748b">{sub}</div></div></div>', unsafe_allow_html=True)
                if cb_col.button("Ir", key=f"goto_{tipo}_{nome}"):
                    st.session_state.active_menu = dest; st.rerun()
        else:
            show_inf("Nenhum resultado.", f"Buscou por: '{busca}'")

# ══════════════════════════════════════
# PEDIDOS
# ══════════════════════════════════════
elif menu == "Pedidos":
    page_header("cart","Pedidos","Monte e finalize pedidos")

    # Carrega dados via cache
    clis_raw = get_clientes_ativos(EMPRESA_ID)
    clis = [c for c in clis_raw if c[12]]  # apenas ativos
    prods_raw = get_produtos_ativos(EMPRESA_ID)
    prods = [p for p in prods_raw if p[8] and p[6] > 0]
    pags = get_pagamentos(EMPRESA_ID)
    sups = get_supervisores(EMPRESA_ID)
    reps = get_representantes(EMPRESA_ID)

    if not clis: show_err("Nenhum cliente ativo.","Cadastre um cliente."); st.stop()
    if not prods: show_err("Nenhum produto em estoque.","Cadastre produtos."); st.stop()
    if not pags: show_err("Nenhuma forma de pagamento.","Cadastre uma."); st.stop()

    cart = st.session_state.cart
    col_form, col_cart = st.columns([3,2])

    with col_cart:
        st.markdown("**Carrinho**")
        if not cart:
            show_inf("Carrinho vazio.","Adicione produtos ao lado.")
        else:
            subtotal = sum(x["preco"]*x["qtd"] for x in cart)
            for i, item in enumerate(cart):
                sub = item["preco"]*item["qtd"]
                ci, cr = st.columns([6,1])
                ci.markdown(f'<div class="cart-row"><div><b>{item["nome"]}</b><br><span style="font-size:.75rem;color:#64748b">{item["qtd"]} × {cur_sym} {item["preco"]:.2f}</span></div><b style="color:#1d4ed8">{cur_sym} {sub:.2f}</b></div>', unsafe_allow_html=True)
                if cr.button("X", key=f"rem_{i}"):
                    st.session_state.cart.pop(i); st.rerun()

            st.markdown("**Desconto**")
            dc1, dc2 = st.columns(2)
            desc_tipo = dc1.selectbox("Tipo", ["Sem desconto","% Percentual","R$ Valor fixo"], key="desc_tipo", label_visibility="collapsed")
            desc_val_inp = dc2.number_input("Desconto", min_value=0.0, step=0.01, format="%.2f", key="desc_val", label_visibility="collapsed")
            d_pct = 0.0; d_reais = 0.0
            if desc_tipo == "% Percentual": d_pct = min(desc_val_inp,100); d_reais = subtotal*d_pct/100
            elif desc_tipo == "R$ Valor fixo": d_reais = min(desc_val_inp,subtotal); d_pct = (d_reais/subtotal*100) if subtotal else 0
            total_final = subtotal - d_reais
            if d_reais > 0: st.caption(f"Desconto: {cur_sym} {d_reais:.2f} ({d_pct:.1f}%)")
            st.markdown(f'<div class="cart-total-bar"><span>Total</span><strong>{cur_sym} {total_final:,.2f}</strong></div>', unsafe_allow_html=True)

    with col_form:
        pn_list = [p[2] for p in prods]
        with st.form("form_add_item", clear_on_submit=True):
            st.markdown("**Adicionar produto**")
            pi = st.selectbox("Produto *", range(len(pn_list)), format_func=lambda i: pn_list[i])
            ps = prods[pi]
            ed = int(ps[6]); nc = sum(x["qtd"] for x in cart if x["id"]==ps[0]); dp = max(0, ed-nc)
            st.caption(f"Disponível: {ed} · No carrinho: {nc} · Pode adicionar: {dp}")
            qa = st.number_input("Quantidade *", min_value=1, max_value=dp if dp>0 else 1, step=1, value=1, disabled=(dp==0))
            ab = st.form_submit_button("Adicionar ao carrinho", use_container_width=True, disabled=(dp==0))
        if ab:
            if dp == 0: show_err(f"Estoque esgotado para '{ps[2]}'.")
            else:
                ex = next((x for x in cart if x["id"]==ps[0]), None)
                if ex: ex["qtd"] += qa
                else: st.session_state.cart.append({"id":ps[0],"nome":ps[2],"preco":float(ps[5]),"qtd":qa})
                st.rerun()

        hr()
        if cart:
            with st.form("form_finalizar"):
                st.markdown("**Finalizar pedido**")
                cf1, cf2 = st.columns(2)
                cliente_sel = cf1.selectbox("Cliente *", [c[1] for c in clis])
                forma = cf2.selectbox("Pagamento *", [p[0] for p in pags])
                tipo_ped = st.selectbox("Tipo", ["Venda","Orçamento"])
                sf1, sf2 = st.columns(2)
                sup_opts = ["(nenhum)"] + [s[1] for s in sups]
                rep_opts = ["(nenhum)"] + [r[1] for r in reps]
                sup_sel = sf1.selectbox("Supervisor", sup_opts)
                rep_sel = sf2.selectbox("Representante", rep_opts)

                a_prazo = st.checkbox("Pagamento a prazo")
                if a_prazo:
                    vf1, vf2 = st.columns(2)
                    vencimento = vf1.date_input("1º vencimento", value=date.today()+timedelta(days=30))
                    parcelas_n = vf2.selectbox("Parcelas", [1,2,3,4,5,6,9,12])
                else:
                    vencimento = None; parcelas_n = 1

                obs = st.text_area("Observação", height=55)
                fin = st.form_submit_button("Finalizar", use_container_width=True)

            if fin:
                erros = []
                if tipo_ped == "Venda":
                    for item in cart:
                        er = get_estoque(item["id"])
                        if er < item["qtd"]: erros.append(f"Estoque insuficiente: '{item['nome']}' (disponível: {er}).")
                if erros:
                    for e in erros: show_err(e)
                else:
                    sb_v = sum(x["preco"]*x["qtd"] for x in cart)
                    dt = st.session_state.get("desc_tipo","Sem desconto")
                    dv = st.session_state.get("desc_val",0.0)
                    if dt == "% Percentual": dp2=min(dv,100); dr=sb_v*dp2/100
                    elif dt == "R$ Valor fixo": dr=min(dv,sb_v); dp2=(dr/sb_v*100) if sb_v else 0
                    else: dp2=0.0; dr=0.0
                    tv = max(sb_v-dr,0)
                    sup_id = next((s[0] for s in sups if s[1]==sup_sel), None)
                    rep_id = next((r[0] for r in reps if r[1]==rep_sel), None)
                    com_sup = tv*float(next((s[2] for s in sups if s[1]==sup_sel),0) or 0)/100
                    com_rep = tv*float(next((r[2] for r in reps if r[1]==rep_sel),0) or 0)/100
                    status_v = "Orçamento" if tipo_ped=="Orçamento" else "Pago"

                    row = qry("""INSERT INTO vendas(empresa_id,data,cliente_name,valor_bruto,desconto_pct,
                        desconto_val,valor_total,pagamento,status,observacao,supervisor_id,representante_id,
                        tipo,vencimento,parcelas,comissao_supervisor,comissao_representante)
                        VALUES(%s,NOW(),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id""",
                        (EMPRESA_ID,cliente_sel,sb_v,dp2,dr,tv,forma,status_v,obs.strip(),
                         sup_id,rep_id,tipo_ped,vencimento,parcelas_n,com_sup,com_rep), returning=True)
                    if row:
                        vid = row[0]
                        for item in cart:
                            qry("INSERT INTO itens_venda(empresa_id,venda_id,produto_nome,quantidade,preco_unit) VALUES(%s,%s,%s,%s,%s)",
                                (EMPRESA_ID,vid,item["nome"],item["qtd"],item["preco"]))
                            if tipo_ped == "Venda":
                                qry("UPDATE produtos SET estoque_atual=estoque_atual-%s WHERE id=%s AND empresa_id=%s",
                                    (item["qtd"],item["id"],EMPRESA_ID))
                                qry("INSERT INTO movimentacoes_estoque(empresa_id,produto_id,tipo,quantidade,motivo,usuario_nome) VALUES(%s,%s,'Saída',%s,%s,%s)",
                                    (EMPRESA_ID,item["id"],item["qtd"],f"Venda #{vid}",st.session_state.usuario_nome))

                        # Gerar parcelas de contas a receber
                        if a_prazo and vencimento and tipo_ped=="Venda":
                            val_parcela = tv/parcelas_n
                            for p_n in range(parcelas_n):
                                venc_p = vencimento + timedelta(days=30*p_n)
                                qry("""INSERT INTO contas_receber(empresa_id,venda_id,cliente_name,descricao,valor,vencimento,parcela,total_parcelas)
                                       VALUES(%s,%s,%s,%s,%s,%s,%s,%s)""",
                                    (EMPRESA_ID,vid,cliente_sel,f"Venda #{vid}",round(val_parcela,2),venc_p,p_n+1,parcelas_n))

                        invalidar_cache()
                        log_acao(f"{tipo_ped} #{vid}", f"Cliente: {cliente_sel} · {cur_sym} {tv:.2f}")
                        show_ok(f"{tipo_ped} finalizada!", f"{len(cart)} produto(s) · {cur_sym} {tv:.2f}")
                        st.session_state.cart = []
                        if tipo_ped=="Venda": st.balloons()
                        st.rerun()
                    else: show_err("Não foi possível salvar.","Tente novamente.")
        else:
            show_inf("Carrinho vazio.","Adicione produtos para liberar finalização.")

# ══════════════════════════════════════
# ORÇAMENTOS (com edição)
# ══════════════════════════════════════
elif menu == "Orçamentos":
    page_header("quote","Orçamentos","Gerencie propostas em aberto")
    orcs = qry("""SELECT v.id,TO_CHAR(v.data,'DD/MM/YYYY'),v.cliente_name,v.valor_total,v.pagamento,
        v.observacao,STRING_AGG(i.produto_nome||' x'||i.quantidade,' | ')
        FROM vendas v LEFT JOIN itens_venda i ON i.venda_id=v.id AND i.empresa_id=v.empresa_id
        WHERE v.empresa_id=%s AND v.status='Orçamento'
        GROUP BY v.id ORDER BY v.data DESC""", (EMPRESA_ID,), fetch=True)

    if not orcs:
        show_inf("Nenhum orçamento aberto.","Crie um pedido do tipo Orçamento.")
    else:
        clis_raw = get_clientes_ativos(EMPRESA_ID)
        clis_ativos = [c[1] for c in clis_raw if c[12]]
        prods_raw = get_produtos_ativos(EMPRESA_ID)
        prods_ativos = [p for p in prods_raw if p[8]]
        pags = get_pagamentos(EMPRESA_ID)

        st.markdown(f"**{len(orcs)} orçamento(s) aberto(s)**")
        for oid, odata, ocli, oval, opag, oobs, oitens in orcs:
            with st.expander(f"#{oid} · {ocli} · {cur_sym} {float(oval):,.2f} · {odata}"):
                st.markdown(f"**Itens:** {oitens or '—'}")
                st.markdown(f"**Pagamento:** {opag} · **Obs:** {oobs or '—'}")

                col_oc1, col_oc2, col_oc3 = st.columns(3)

                # Editar orçamento
                if col_oc1.button("Editar orçamento", key=f"edit_orc_{oid}"):
                    st.session_state.edit_orc_id = oid
                if st.session_state.edit_orc_id == oid:
                    st.markdown("---")
                    st.markdown("**Editar orçamento**")
                    # Remover itens antigos e lançar novos
                    itens_orc = qry("SELECT produto_nome,quantidade,preco_unit FROM itens_venda WHERE venda_id=%s AND empresa_id=%s",
                                    (oid,EMPRESA_ID), fetch=True)
                    if "orc_cart" not in st.session_state:
                        st.session_state.orc_cart = [{"nome":n,"qtd":q,"preco":float(p)} for n,q,p in (itens_orc or [])]

                    # Mini-carrinho do orçamento
                    for j, it in enumerate(st.session_state.orc_cart):
                        oc1, oc2 = st.columns([7,1])
                        oc1.markdown(f"- **{it['nome']}** · {it['qtd']} un × {cur_sym} {it['preco']:.2f}")
                        if oc2.button("X", key=f"rem_orc_item_{j}"):
                            st.session_state.orc_cart.pop(j); st.rerun()

                    with st.form(f"f_add_orc_{oid}"):
                        ap1, ap2, ap3 = st.columns(3)
                        psel = ap1.selectbox("Produto", [p[2] for p in prods_ativos], key=f"orc_prod_{oid}")
                        aqtd = ap2.number_input("Qtd", min_value=1, step=1, key=f"orc_qtd_{oid}")
                        if ap3.form_submit_button("Adicionar item"):
                            p_obj = next((p for p in prods_ativos if p[2]==psel), None)
                            if p_obj:
                                st.session_state.orc_cart.append({"nome":p_obj[2],"qtd":aqtd,"preco":float(p_obj[5])})
                                st.rerun()

                    with st.form(f"f_edit_orc_{oid}"):
                        ef1, ef2 = st.columns(2)
                        ecli = ef1.selectbox("Cliente", clis_ativos, index=clis_ativos.index(ocli) if ocli in clis_ativos else 0)
                        epag = ef2.selectbox("Pagamento", [p[0] for p in pags], index=[p[0] for p in pags].index(opag) if opag in [p[0] for p in pags] else 0)
                        eobs = st.text_area("Obs", value=oobs or "", height=55)
                        es1, es2 = st.columns(2)
                        salvar_orc = es1.form_submit_button("Salvar alterações", use_container_width=True)
                        cancel_orc = es2.form_submit_button("Cancelar edição", use_container_width=True)

                    if cancel_orc:
                        st.session_state.edit_orc_id = None
                        if "orc_cart" in st.session_state: del st.session_state["orc_cart"]
                        st.rerun()
                    if salvar_orc:
                        if not st.session_state.orc_cart:
                            show_err("Adicione pelo menos um item.")
                        else:
                            novo_total = sum(it["preco"]*it["qtd"] for it in st.session_state.orc_cart)
                            qry("UPDATE vendas SET cliente_name=%s,pagamento=%s,observacao=%s,valor_total=%s,valor_bruto=%s WHERE id=%s AND empresa_id=%s",
                                (ecli,epag,eobs,novo_total,novo_total,oid,EMPRESA_ID))
                            qry("DELETE FROM itens_venda WHERE venda_id=%s AND empresa_id=%s",(oid,EMPRESA_ID))
                            for it in st.session_state.orc_cart:
                                qry("INSERT INTO itens_venda(empresa_id,venda_id,produto_nome,quantidade,preco_unit) VALUES(%s,%s,%s,%s,%s)",
                                    (EMPRESA_ID,oid,it["nome"],it["qtd"],it["preco"]))
                            show_ok(f"Orçamento #{oid} atualizado!")
                            st.session_state.edit_orc_id = None
                            if "orc_cart" in st.session_state: del st.session_state["orc_cart"]
                            st.rerun()

                # Converter
                if col_oc2.button("Converter em venda", key=f"conv_{oid}"):
                    idb = qry("SELECT iv.produto_nome,iv.quantidade,p.id FROM itens_venda iv LEFT JOIN produtos p ON p.nome=iv.produto_nome AND p.empresa_id=iv.empresa_id WHERE iv.venda_id=%s AND iv.empresa_id=%s",(oid,EMPRESA_ID),fetch=True)
                    errs = []
                    for pn,pq,pid_oc in (idb or []):
                        if pid_oc:
                            er = get_estoque(pid_oc)
                            if er < pq: errs.append(f"Estoque insuficiente: '{pn}'.")
                    if errs:
                        for e in errs: show_err(e)
                    else:
                        for pn,pq,pid_oc in (idb or []):
                            if pid_oc:
                                qry("UPDATE produtos SET estoque_atual=estoque_atual-%s WHERE id=%s AND empresa_id=%s",(pq,pid_oc,EMPRESA_ID))
                        qry("UPDATE vendas SET status='Pago',tipo='Venda' WHERE id=%s AND empresa_id=%s",(oid,EMPRESA_ID))
                        invalidar_cache()
                        show_ok(f"Orçamento #{oid} convertido!"); st.rerun()

                # Descartar
                if col_oc3.button("Descartar", key=f"disc_{oid}"):
                    qry("UPDATE vendas SET status='Cancelado' WHERE id=%s AND empresa_id=%s",(oid,EMPRESA_ID))
                    show_ok(f"Orçamento #{oid} descartado."); st.rerun()

# ══════════════════════════════════════
# HISTÓRICO DE PEDIDOS
# ══════════════════════════════════════
elif menu == "Histórico de Pedidos":
    page_header("history","Histórico de Pedidos","Consulte e exporte transações")

    with st.expander("Filtros", expanded=True):
        hf1, hf2, hf3 = st.columns(3)
        fs = hf1.selectbox("Status", ["Todos","Pago","Cancelado"], key="filtro_hist")
        hf4, hf5 = st.columns(2)

        st.markdown("**Data inicial**")
        di = hf2.date_input("Data inicial", value=date.today()-timedelta(days=30), key="hist_ini", label_visibility="collapsed")
        st.markdown("**Data final**")
        df2 = hf3.date_input("Data final", value=date.today(), key="hist_fim", label_visibility="collapsed")
        st.markdown("**Cliente**")
        bch = hf4.text_input("Cliente", placeholder="Nome do cliente...", key="busca_hist_cli", label_visibility="collapsed")
        st.markdown("**Representante**")
        rep_f = hf5.text_input("Representante", placeholder="Nome do representante...", key="busca_hist_rep", label_visibility="collapsed")

    wp = ["v.empresa_id=%s","v.data>=%s","v.data<%s","v.tipo='Venda'"]
    ph = [EMPRESA_ID, datetime.combine(di,datetime.min.time()), datetime.combine(df2+timedelta(days=1),datetime.min.time())]
    if fs != "Todos": wp.append("v.status=%s"); ph.append(fs)
    if bch: wp.append("v.cliente_name ILIKE %s"); ph.append(f"%{bch}%")
    if rep_f: wp.append("rep.nome ILIKE %s"); ph.append(f"%{rep_f}%")
    ws = " AND ".join(wp)

    vendas = qry(f"""SELECT v.id,TO_CHAR(v.data,'DD/MM/YYYY HH24:MI'),v.cliente_name,
        v.valor_bruto,v.desconto_val,v.valor_total,v.pagamento,v.status,
        STRING_AGG(i.produto_nome||' x'||i.quantidade,' | '),
        COALESCE(v.observacao,''),COALESCE(sup.nome,'—'),COALESCE(rep.nome,'—'),
        COALESCE(v.parcelas,1)
        FROM vendas v
        LEFT JOIN itens_venda i ON i.venda_id=v.id AND i.empresa_id=v.empresa_id
        LEFT JOIN supervisores sup ON sup.id=v.supervisor_id
        LEFT JOIN representantes rep ON rep.id=v.representante_id
        WHERE {ws}
        GROUP BY v.id,sup.nome,rep.nome ORDER BY v.data DESC""", tuple(ph), fetch=True)

    if vendas and HAS_PANDAS:
        xb = to_excel(vendas, ["#","Data","Cliente","Bruto","Desconto","Total","Pagamento","Status","Produtos","Obs","Supervisor","Representante","Parcelas"])
        if xb:
            st.download_button("Exportar Excel", data=xb, file_name=f"pedidos_{date.today()}.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    if not vendas:
        show_inf("Nenhum pedido encontrado.","Ajuste os filtros.")
    else:
        total_p = sum(float(v[5]) for v in vendas if v[7]=="Pago")
        st.markdown(f"**{len(vendas)} pedido(s)** · Total: **{cur_sym} {total_p:,.2f}**")
        for row in vendas:
            vid,dfmt,cli,bruto,dv,val,pag,status,itens,obs,sup_n,rep_n,parc = row
            itens = itens or "—"
            cls_map = {"Pago":"b-blue","Cancelado":"b-red"}
            sb = badge(status, cls_map.get(status,"b-gray"))
            parc_info = f" · {parc}x" if parc and int(parc)>1 else ""
            dh = f" · Desc: {cur_sym} {float(dv):.2f}" if dv and float(dv)>0 else ""
            ci2, cv2, ca2 = st.columns([5,2,2])
            ci2.markdown(f'''<div class="card">
              <div class="card-label">#{vid} · {dfmt} · {pag}{parc_info} · {sup_n} · {rep_n}</div>
              <div class="card-title">{cli} &nbsp;{sb}</div>
              <div class="card-sub">{itens}{dh}</div>
              {f'<div class="card-sub" style="font-style:italic">{obs}</div>' if obs else ""}
            </div>''', unsafe_allow_html=True)
            cv2.markdown(f'<div style="padding-top:14px;font-weight:700;color:#1d4ed8">{cur_sym} {float(val):,.2f}</div>', unsafe_allow_html=True)
            with ca2:
                if status == "Pago":
                    if st.button("Cancelar", key=f"cancel_{vid}"):
                        idb = qry("SELECT produto_nome,quantidade FROM itens_venda WHERE venda_id=%s AND empresa_id=%s",(vid,EMPRESA_ID),fetch=True)
                        for pn,pq in (idb or []):
                            qry("UPDATE produtos SET estoque_atual=estoque_atual+%s WHERE nome=%s AND empresa_id=%s",(pq,pn,EMPRESA_ID))
                        qry("UPDATE vendas SET status='Cancelado' WHERE id=%s AND empresa_id=%s",(vid,EMPRESA_ID))
                        invalidar_cache()
                        log_acao(f"Cancelou #{vid}", f"Cliente: {cli}")
                        show_ok(f"Pedido #{vid} cancelado."); st.rerun()
                elif status == "Cancelado":
                    if st.button("Reativar", key=f"reat_{vid}"):
                        idb = qry("SELECT produto_nome,quantidade FROM itens_venda WHERE venda_id=%s AND empresa_id=%s",(vid,EMPRESA_ID),fetch=True)
                        errs = []
                        for pn,pq in (idb or []):
                            er = qry("SELECT estoque_atual FROM produtos WHERE nome=%s AND empresa_id=%s",(pn,EMPRESA_ID),fetch=True)
                            ev = er[0][0] if er else 0
                            if ev < pq: errs.append(f"Estoque insuficiente: '{pn}'.")
                        if errs:
                            for e in errs: show_err(e)
                        else:
                            for pn,pq in (idb or []):
                                qry("UPDATE produtos SET estoque_atual=estoque_atual-%s WHERE nome=%s AND empresa_id=%s",(pq,pn,EMPRESA_ID))
                            qry("UPDATE vendas SET status='Pago' WHERE id=%s AND empresa_id=%s",(vid,EMPRESA_ID))
                            invalidar_cache()
                            show_ok(f"Pedido #{vid} reativado!"); st.rerun()
            st.markdown('<div style="border-top:1px solid #f1f5f9;margin:4px 0"></div>', unsafe_allow_html=True)

# ══════════════════════════════════════
# CONTAS A RECEBER
# ══════════════════════════════════════
elif menu == "Contas a Receber":
    page_header("receivable","Contas a Receber","Controle de cobranças")

    with st.expander("Lançar conta manual"):
        with st.form("f_cr"):
            cr1, cr2 = st.columns(2)
            cli_cr = cr1.text_input("Cliente *")
            desc_cr = cr2.text_input("Descrição *")
            cr3, cr4 = st.columns(2)
            val_cr = cr3.number_input(f"Valor ({cur_sym}) *", min_value=0.01, step=0.01, format="%.2f")
            venc_cr = cr4.date_input("Vencimento *")
            sv = st.form_submit_button("Salvar", use_container_width=True)
        if sv:
            if not validate_required(cli_cr, desc_cr): show_err("Preencha todos os campos.")
            else:
                qry("INSERT INTO contas_receber(empresa_id,cliente_name,descricao,valor,vencimento) VALUES(%s,%s,%s,%s,%s)",
                    (EMPRESA_ID,cli_cr,desc_cr,val_cr,venc_cr))
                show_ok("Conta lançada!"); st.rerun()

    hr()
    filtro_cr = st.selectbox("Status", ["Todos","Pendente","Recebido"], key="filtro_cr")
    cr_q = "SELECT id,cliente_name,descricao,valor,vencimento,status,data_pagamento,parcela,total_parcelas FROM contas_receber WHERE empresa_id=%s"
    cr_p = [EMPRESA_ID]
    if filtro_cr != "Todos": cr_q += " AND status=%s"; cr_p.append(filtro_cr)
    cr_q += " ORDER BY vencimento"
    contas = qry(cr_q, tuple(cr_p), fetch=True)

    total_pend = sum(float(c[3]) for c in (contas or []) if c[5]=="Pendente")
    total_rec  = sum(float(c[3]) for c in (contas or []) if c[5]=="Recebido")
    m1, m2 = st.columns(2)
    m1.metric("A receber", f"{cur_sym} {total_pend:,.2f}")
    m2.metric("Recebido", f"{cur_sym} {total_rec:,.2f}")

    if HAS_PANDAS and contas:
        xb = to_excel(contas, ["ID","Cliente","Descrição","Valor","Vencimento","Status","Data Pgto","Parcela","Total Parc"])
        if xb: st.download_button("Exportar Excel", data=xb, file_name="contas_receber.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    hr()
    for c in (contas or []):
        cid,ccli,cdesc,cval,cvenc,cstatus,cdata_p,cparc,ctotal_parc = c
        hoje = date.today()
        dias = (cvenc-hoje).days if cvenc else 0
        cor = "b-red" if (dias<0 and cstatus=="Pendente") else ("b-yellow" if dias<=3 and cstatus=="Pendente" else "b-green")
        lbl = "Vencido" if dias<0 and cstatus=="Pendente" else (f"{dias}d" if cstatus=="Pendente" else "Recebido")
        sb = badge(cstatus, "b-blue" if cstatus=="Recebido" else "b-orange")
        parc_info = f" · Parcela {cparc}/{ctotal_parc}" if ctotal_parc and int(ctotal_parc)>1 else ""
        ci2, ca2 = st.columns([8,2])
        ci2.markdown(f'''<div class="card">
          <div class="card-title">{ccli} &nbsp;{sb}</div>
          <div class="card-sub">{cdesc} · Vence {cvenc.strftime("%d/%m/%Y") if cvenc else "—"}{parc_info} {badge(lbl,cor)}</div>
          <div class="card-val">{cur_sym} {float(cval):,.2f}</div>
        </div>''', unsafe_allow_html=True)
        with ca2:
            if cstatus == "Pendente":
                if st.button("Recebido", key=f"rec_{cid}"):
                    qry("UPDATE contas_receber SET status='Recebido',data_pagamento=CURRENT_DATE WHERE id=%s AND empresa_id=%s",(cid,EMPRESA_ID))
                    show_ok("Marcado como recebido!"); st.rerun()
            else:
                st.caption(f"Recebido em {cdata_p.strftime('%d/%m/%Y') if cdata_p else '—'}")
        st.markdown('<div style="border-top:1px solid #f1f5f9;margin:4px 0"></div>', unsafe_allow_html=True)

# ══════════════════════════════════════
# DESPESAS
# ══════════════════════════════════════
elif menu == "Despesas":
    page_header("expenses","Despesas","Registre e controle custos operacionais")

    with st.expander("Nova despesa"):
        with st.form("f_desp"):
            d1, d2 = st.columns(2)
            desc_d = d1.text_input("Descrição *")
            cat_d  = d2.text_input("Categoria", placeholder="Aluguel, Luz...")
            d3, d4, d5 = st.columns(3)
            val_d    = d3.number_input(f"Valor ({cur_sym}) *", min_value=0.01, step=0.01, format="%.2f")
            data_d   = d4.date_input("Data *", value=date.today())
            status_d = d5.selectbox("Status", ["Pago","Pendente"])
            obs_d    = st.text_area("Observação", height=50)
            sv = st.form_submit_button("Salvar", use_container_width=True)
        if sv:
            if not validate_required(desc_d): show_err("Descrição obrigatória.")
            else:
                qry("INSERT INTO despesas(empresa_id,descricao,categoria,valor,data,status,observacao) VALUES(%s,%s,%s,%s,%s,%s,%s)",
                    (EMPRESA_ID,desc_d,cat_d,val_d,data_d,status_d,obs_d))
                show_ok("Despesa registrada!"); st.rerun()

    hr()
    df1, df2 = st.columns(2)
    fil_desp = df1.selectbox("Status", ["Todos","Pago","Pendente"], key="fil_desp")
    mes_desp = df2.date_input("A partir de", value=date.today().replace(day=1), key="mes_desp")
    dq = "SELECT id,descricao,categoria,valor,data,status FROM despesas WHERE empresa_id=%s AND data>=%s"
    dp_p = [EMPRESA_ID, mes_desp]
    if fil_desp != "Todos": dq += " AND status=%s"; dp_p.append(fil_desp)
    dq += " ORDER BY data DESC"
    desps = qry(dq, tuple(dp_p), fetch=True)

    total_d = sum(float(d[3]) for d in (desps or []))
    total_pago_d = sum(float(d[3]) for d in (desps or []) if d[5]=="Pago")
    m1, m2 = st.columns(2)
    m1.metric("Total período", f"{cur_sym} {total_d:,.2f}")
    m2.metric("Pagas", f"{cur_sym} {total_pago_d:,.2f}")

    if HAS_PANDAS and desps:
        xb = to_excel(desps, ["ID","Descrição","Categoria","Valor","Data","Status"])
        if xb: st.download_button("Exportar Excel", data=xb, file_name="despesas.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    hr()
    for d in (desps or []):
        did,ddesc,dcat,dval,ddata,dstatus = d
        sb = badge(dstatus, "b-blue" if dstatus=="Pago" else "b-orange")
        di2, da2 = st.columns([8,2])
        di2.markdown(f'''<div class="card">
          <div class="card-title">{ddesc} &nbsp;{sb}</div>
          <div class="card-sub">{dcat or "—"} · {ddata.strftime("%d/%m/%Y") if ddata else "—"}</div>
          <div class="card-val">{cur_sym} {float(dval):,.2f}</div>
        </div>''', unsafe_allow_html=True)
        with da2:
            if dstatus == "Pendente":
                if st.button("Pagar", key=f"pag_d_{did}"):
                    qry("UPDATE despesas SET status='Pago' WHERE id=%s AND empresa_id=%s",(did,EMPRESA_ID)); st.rerun()
            if pode("despesas_del"):
                if st.button("Excluir", key=f"del_d_{did}"):
                    qry("DELETE FROM despesas WHERE id=%s AND empresa_id=%s",(did,EMPRESA_ID)); st.rerun()
        st.markdown('<div style="border-top:1px solid #f1f5f9;margin:4px 0"></div>', unsafe_allow_html=True)

# ══════════════════════════════════════
# CATEGORIAS
# ══════════════════════════════════════
elif menu == "Categorias":
    page_header("categories","Categorias","Organize produtos por categoria")
    c1, c2 = st.columns([3,1])
    nc = c1.text_input("Nova categoria", label_visibility="collapsed", placeholder="Ex: Eletrônicos...")
    if c2.button("Adicionar", use_container_width=True):
        if not validate_required(nc): show_err("Digite o nome.")
        else:
            res = qry("INSERT INTO categorias(empresa_id,nome) VALUES(%s,%s)",(EMPRESA_ID,nc.strip()))
            if res is True: invalidar_cache(); show_ok(f"'{nc}' criada."); st.rerun()
            elif res=="duplicate": show_err("Categoria já existe.")
            else: show_err("Erro ao salvar.")
    bc = st.text_input("Buscar", placeholder="Nome...", key="busca_cat")
    cats = qry("SELECT id,nome,ativo FROM categorias WHERE empresa_id=%s ORDER BY nome",(EMPRESA_ID,),fetch=True)
    if cats:
        if bc: b=bc.lower(); cats=[c for c in cats if b in (c[1] or "").lower()]
        hr(); st.markdown(f"**{len(cats)} categoria(s)**")
        for cid,cnome,cativo in cats:
            ic = "" if cativo else " inativo"
            if st.session_state.edit_cat_id == cid:
                with st.form(f"f_edit_cat_{cid}"):
                    nn = st.text_input("Nome *", value=cnome)
                    cs2,cc2 = st.columns(2)
                    sv = cs2.form_submit_button("Salvar", use_container_width=True)
                    cn2 = cc2.form_submit_button("Cancelar", use_container_width=True)
                if cn2: st.session_state.edit_cat_id=None; st.rerun()
                if sv:
                    if not validate_required(nn): show_err("Nome obrigatório.")
                    else:
                        res = qry("UPDATE categorias SET nome=%s WHERE id=%s AND empresa_id=%s",(nn.strip(),cid,EMPRESA_ID))
                        if res is True:
                            qry("UPDATE produtos SET categoria=%s WHERE categoria=%s AND empresa_id=%s",(nn.strip(),cnome,EMPRESA_ID))
                            invalidar_cache(); show_ok("Categoria atualizada!"); st.session_state.edit_cat_id=None; st.rerun()
                        elif res=="duplicate": show_err("Nome já existe.")
                        else: show_err("Erro ao salvar.")
            else:
                cn2,ce2,ct2 = st.columns([7,1,1])
                cn2.markdown(f'<div class="aux-row{ic}"><span>{cnome}</span>{badge("Ativo","b-green") if cativo else badge("Inativo","b-gray")}</div>',unsafe_allow_html=True)
                if ce2.button("Ed", key=f"edit_cat_{cid}"): st.session_state.edit_cat_id=cid; st.rerun()
                if ct2.button("On" if not cativo else "Off", key=f"tog_cat_{cid}"):
                    qry("UPDATE categorias SET ativo=%s WHERE id=%s AND empresa_id=%s",(not cativo,cid,EMPRESA_ID)); invalidar_cache(); st.rerun()
    else: show_inf("Nenhuma categoria.","Adicione uma acima.")

# ══════════════════════════════════════
# FORMAS DE PAGAMENTO
# ══════════════════════════════════════
elif menu == "Formas de Pagamento":
    page_header("payment","Formas de Pagamento","Gerencie as formas aceitas")
    c1, c2 = st.columns([3,1])
    np2 = c1.text_input("Nova forma", label_visibility="collapsed", placeholder="Pix, Dinheiro, Cartão...")
    if c2.button("Adicionar", use_container_width=True):
        if not validate_required(np2): show_err("Digite o nome.")
        else:
            res = qry("INSERT INTO pagamentos(empresa_id,nome) VALUES(%s,%s)",(EMPRESA_ID,np2.strip()))
            if res is True: invalidar_cache(); show_ok(f"'{np2}' criada."); st.rerun()
            elif res=="duplicate": show_err("Já existe.")
            else: show_err("Erro ao salvar.")
    bp = st.text_input("Buscar", placeholder="Nome...", key="busca_pag")
    pags = qry("SELECT id,nome,ativo FROM pagamentos WHERE empresa_id=%s ORDER BY nome",(EMPRESA_ID,),fetch=True)
    if pags:
        if bp: b=bp.lower(); pags=[p for p in pags if b in (p[1] or "").lower()]
        hr(); st.markdown(f"**{len(pags)} forma(s)**")
        for pid,pnome,pativo in pags:
            ic = "" if pativo else " inativo"
            if st.session_state.edit_pag_id == pid:
                with st.form(f"f_edit_pag_{pid}"):
                    nn = st.text_input("Nome *", value=pnome)
                    cs3,cc3 = st.columns(2)
                    sv3 = cs3.form_submit_button("Salvar", use_container_width=True)
                    cn3 = cc3.form_submit_button("Cancelar", use_container_width=True)
                if cn3: st.session_state.edit_pag_id=None; st.rerun()
                if sv3:
                    if not validate_required(nn): show_err("Nome obrigatório.")
                    else:
                        res = qry("UPDATE pagamentos SET nome=%s WHERE id=%s AND empresa_id=%s",(nn.strip(),pid,EMPRESA_ID))
                        if res is True: invalidar_cache(); show_ok("Atualizado!"); st.session_state.edit_pag_id=None; st.rerun()
                        elif res=="duplicate": show_err("Já existe.")
                        else: show_err("Erro.")
            else:
                pn2,pe2,pt2 = st.columns([7,1,1])
                pn2.markdown(f'<div class="aux-row{ic}"><span>{pnome}</span>{badge("Ativo","b-green") if pativo else badge("Inativo","b-gray")}</div>',unsafe_allow_html=True)
                if pe2.button("Ed", key=f"edit_pag_{pid}"): st.session_state.edit_pag_id=pid; st.rerun()
                if pt2.button("On" if not pativo else "Off", key=f"tog_pag_{pid}"):
                    qry("UPDATE pagamentos SET ativo=%s WHERE id=%s AND empresa_id=%s",(not pativo,pid,EMPRESA_ID)); invalidar_cache(); st.rerun()
    else: show_inf("Nenhuma forma de pagamento.","Adicione uma acima.")

# ══════════════════════════════════════
# GRUPOS DE CLIENTES
# ══════════════════════════════════════
elif menu == "Grupos de Clientes":
    page_header("groups","Grupos de Clientes","Segmente sua base de clientes")
    with st.expander("Novo grupo"):
        with st.form("f_grp"):
            g1,g2 = st.columns(2)
            gn = g1.text_input("Nome *")
            gd = g2.number_input("Desconto padrão (%)", min_value=0.0, max_value=100.0, step=0.5, format="%.2f")
            sv = st.form_submit_button("Salvar", use_container_width=True)
        if sv:
            if not validate_required(gn): show_err("Nome obrigatório.")
            else:
                res = qry("INSERT INTO grupos_clientes(empresa_id,nome,desconto_padrao) VALUES(%s,%s,%s)",(EMPRESA_ID,gn.strip(),gd))
                if res is True: invalidar_cache(); show_ok(f"Grupo '{gn}' criado!"); st.rerun()
                elif res=="duplicate": show_err("Já existe.")
                else: show_err("Erro.")
    hr()
    grps = qry("SELECT id,nome,desconto_padrao,ativo FROM grupos_clientes WHERE empresa_id=%s ORDER BY nome",(EMPRESA_ID,),fetch=True)
    if grps:
        for gid,gnome,gdesc,gativo in grps:
            if st.session_state.edit_grupo_id == gid:
                with st.form(f"f_edit_grp_{gid}"):
                    eg1,eg2 = st.columns(2)
                    en = eg1.text_input("Nome *", value=gnome)
                    ed = eg2.number_input("Desconto (%)", value=float(gdesc or 0), min_value=0.0, max_value=100.0, step=0.5, format="%.2f")
                    cs,cc = st.columns(2)
                    sv2 = cs.form_submit_button("Salvar", use_container_width=True)
                    cn2 = cc.form_submit_button("Cancelar", use_container_width=True)
                if cn2: st.session_state.edit_grupo_id=None; st.rerun()
                if sv2:
                    qry("UPDATE grupos_clientes SET nome=%s,desconto_padrao=%s WHERE id=%s AND empresa_id=%s",(en.strip(),ed,gid,EMPRESA_ID))
                    invalidar_cache(); show_ok("Atualizado!"); st.session_state.edit_grupo_id=None; st.rerun()
            else:
                gi2,ge2,gt2 = st.columns([7,1,1])
                gi2.markdown(f'<div class="aux-row"><span>{gnome} · {float(gdesc or 0):.1f}% desc.</span>{badge("Ativo","b-green") if gativo else badge("Inativo","b-gray")}</div>',unsafe_allow_html=True)
                if ge2.button("Ed", key=f"edit_grp_{gid}"): st.session_state.edit_grupo_id=gid; st.rerun()
                if gt2.button("On" if not gativo else "Off", key=f"tog_grp_{gid}"):
                    qry("UPDATE grupos_clientes SET ativo=%s WHERE id=%s AND empresa_id=%s",(not gativo,gid,EMPRESA_ID)); invalidar_cache(); st.rerun()
    else: show_inf("Nenhum grupo cadastrado.")

# ══════════════════════════════════════
# REPRESENTANTES
# ══════════════════════════════════════
elif menu == "Representantes":
    page_header("reps","Representantes","Gerencie a equipe de representantes")
    sups_opts = get_supervisores(EMPRESA_ID)
    with st.expander("Novo representante"):
        with st.form("f_rep"):
            r1,r2 = st.columns(2); rn=r1.text_input("Nome *"); re2=r2.text_input("E-mail")
            r3,r4,r5 = st.columns(3); rt=r3.text_input("Telefone"); rreg=r4.text_input("Região")
            rcom_pct = r5.number_input("Comissão %", min_value=0.0, max_value=100.0, step=0.5, format="%.2f")
            sl = ["(nenhum)"]+[s[1] for s in sups_opts]; rs=st.selectbox("Supervisor",sl)
            sv = st.form_submit_button("Salvar", use_container_width=True)
        if sv:
            if not validate_required(rn): show_err("Nome obrigatório.")
            else:
                si = next((s[0] for s in sups_opts if s[1]==rs), None)
                res = qry("INSERT INTO representantes(empresa_id,nome,email,telefone,supervisor_id,comissao_pct,regiao) VALUES(%s,%s,%s,%s,%s,%s,%s)",
                          (EMPRESA_ID,rn.strip(),re2,rt,si,rcom_pct,rreg))
                if res is True: invalidar_cache(); show_ok(f"'{rn}' adicionado!"); st.rerun()
                elif res=="duplicate": show_err("Já existe.")
                else: show_err("Erro.")
    hr()
    br = st.text_input("Buscar", placeholder="Nome ou região...", key="busca_rep")
    reps = qry("""SELECT r.id,r.nome,r.email,r.telefone,r.ativo,
                         COALESCE(s.nome,'—'),r.comissao_pct,r.regiao
                  FROM representantes r LEFT JOIN supervisores s ON s.id=r.supervisor_id
                  WHERE r.empresa_id=%s ORDER BY r.nome""", (EMPRESA_ID,), fetch=True)
    if reps:
        if br: b=br.lower(); reps=[r for r in reps if b in (r[1] or "").lower() or b in (r[7] or "").lower()]
        if HAS_PANDAS:
            xb = to_excel(reps,["ID","Nome","Email","Tel","Ativo","Supervisor","Comissão%","Região"])
            if xb: st.download_button("Exportar Excel",data=xb,file_name="representantes.xlsx",mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        st.markdown(f"**{len(reps)} representante(s)**")
        for rid,rn2,re3,rt2,ra,rsn,rcom2,rreg2 in reps:
            if st.session_state.edit_rep_id == rid:
                with st.form(f"f_edit_rep_{rid}"):
                    er1,er2 = st.columns(2); en=er1.text_input("Nome *",value=rn2 or ""); em=er2.text_input("E-mail",value=re3 or "")
                    er3,er4,er5 = st.columns(3); et=er3.text_input("Telefone",value=rt2 or ""); ereg=er4.text_input("Região",value=rreg2 or "")
                    ecom = er5.number_input("Comissão %", value=float(rcom2 or 0), min_value=0.0, max_value=100.0, step=0.5, format="%.2f")
                    sl2=["(nenhum)"]+[s[1] for s in sups_opts]
                    ci2=sl2.index(rsn) if rsn in sl2 else 0; es=st.selectbox("Supervisor",sl2,index=ci2)
                    cs2,cc2 = st.columns(2); sv2=cs2.form_submit_button("Salvar",use_container_width=True); cn2=cc2.form_submit_button("Cancelar",use_container_width=True)
                if cn2: st.session_state.edit_rep_id=None; st.rerun()
                if sv2:
                    if not validate_required(en): show_err("Nome obrigatório.")
                    else:
                        si2 = next((s[0] for s in sups_opts if s[1]==es), None)
                        res = qry("UPDATE representantes SET nome=%s,email=%s,telefone=%s,supervisor_id=%s,comissao_pct=%s,regiao=%s WHERE id=%s AND empresa_id=%s",
                                  (en.strip(),em,et,si2,ecom,ereg,rid,EMPRESA_ID))
                        if res is True: invalidar_cache(); show_ok("Atualizado!"); st.session_state.edit_rep_id=None; st.rerun()
                        elif res=="duplicate": show_err("Já existe.")
                        else: show_err("Erro.")
            else:
                ci2,ce2,ct2 = st.columns([8,1,1])
                ci2.markdown(f'''<div class="card">
                  <div class="card-title">{rn2} {badge("Ativo","b-green") if ra else badge("Inativo","b-gray")}</div>
                  <div class="card-sub">{re3 or "—"} · {rt2 or "—"} · Região: {rreg2 or "—"} · Comissão: {float(rcom2 or 0):.1f}% · Sup: {rsn}</div>
                </div>''',unsafe_allow_html=True)
                if ce2.button("Ed",key=f"edit_rep_{rid}"): st.session_state.edit_rep_id=rid; st.rerun()
                if ct2.button("On" if not ra else "Off",key=f"tog_rep_{rid}"):
                    qry("UPDATE representantes SET ativo=%s WHERE id=%s AND empresa_id=%s",(not ra,rid,EMPRESA_ID)); invalidar_cache(); st.rerun()
            st.markdown('<div style="border-top:1px solid #f1f5f9;margin:4px 0"></div>',unsafe_allow_html=True)
    else: show_inf("Nenhum representante.","Adicione um acima.")

# ══════════════════════════════════════
# SUPERVISORES
# ══════════════════════════════════════
elif menu == "Supervisores":
    page_header("supervisor","Supervisores","Gerencie a equipe de supervisores")
    with st.expander("Novo supervisor"):
        with st.form("f_sup"):
            s1,s2 = st.columns(2); sn=s1.text_input("Nome *"); se=s2.text_input("E-mail")
            s3,s4 = st.columns(2); st2=s3.text_input("Telefone")
            scom_pct = s4.number_input("Comissão %", min_value=0.0, max_value=100.0, step=0.5, format="%.2f")
            sv = st.form_submit_button("Salvar", use_container_width=True)
        if sv:
            if not validate_required(sn): show_err("Nome obrigatório.")
            else:
                res = qry("INSERT INTO supervisores(empresa_id,nome,email,telefone,comissao_pct) VALUES(%s,%s,%s,%s,%s)",(EMPRESA_ID,sn.strip(),se,st2,scom_pct))
                if res is True: invalidar_cache(); show_ok(f"'{sn}' adicionado!"); st.rerun()
                elif res=="duplicate": show_err("Já existe.")
                else: show_err("Erro.")
    hr()
    if HAS_PANDAS:
        sups_exp = qry("SELECT id,nome,email,telefone,comissao_pct,ativo FROM supervisores WHERE empresa_id=%s ORDER BY nome",(EMPRESA_ID,),fetch=True)
        if sups_exp:
            xb = to_excel(sups_exp,["ID","Nome","Email","Tel","Comissão%","Ativo"])
            if xb: st.download_button("Exportar Excel",data=xb,file_name="supervisores.xlsx",mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    bs = st.text_input("Buscar", placeholder="Nome...", key="busca_sup")
    sups = qry("SELECT id,nome,email,telefone,comissao_pct,ativo FROM supervisores WHERE empresa_id=%s ORDER BY nome",(EMPRESA_ID,),fetch=True)
    if sups:
        if bs: b=bs.lower(); sups=[s for s in sups if b in (s[1] or "").lower()]
        st.markdown(f"**{len(sups)} supervisor(es)**")
        for sid,sn2,se2,st3,scom2,sa in sups:
            if st.session_state.edit_sup_id == sid:
                with st.form(f"f_edit_sup_{sid}"):
                    es1,es2 = st.columns(2); en=es1.text_input("Nome *",value=sn2 or ""); em=es2.text_input("E-mail",value=se2 or "")
                    es3,es4 = st.columns(2); et=es3.text_input("Telefone",value=st3 or "")
                    ecom = es4.number_input("Comissão %", value=float(scom2 or 0), min_value=0.0, max_value=100.0, step=0.5, format="%.2f")
                    cs2,cc2 = st.columns(2); sv2=cs2.form_submit_button("Salvar",use_container_width=True); cn2=cc2.form_submit_button("Cancelar",use_container_width=True)
                if cn2: st.session_state.edit_sup_id=None; st.rerun()
                if sv2:
                    if not validate_required(en): show_err("Nome obrigatório.")
                    else:
                        res = qry("UPDATE supervisores SET nome=%s,email=%s,telefone=%s,comissao_pct=%s WHERE id=%s AND empresa_id=%s",(en.strip(),em,et,ecom,sid,EMPRESA_ID))
                        if res is True: invalidar_cache(); show_ok("Atualizado!"); st.session_state.edit_sup_id=None; st.rerun()
                        elif res=="duplicate": show_err("Já existe.")
                        else: show_err("Erro.")
            else:
                ci2,ce2,ct2 = st.columns([8,1,1])
                ci2.markdown(f'''<div class="card">
                  <div class="card-title">{sn2} {badge("Ativo","b-green") if sa else badge("Inativo","b-gray")}</div>
                  <div class="card-sub">{se2 or "—"} · {st3 or "—"} · Comissão: {float(scom2 or 0):.1f}%</div>
                </div>''',unsafe_allow_html=True)
                if ce2.button("Ed",key=f"edit_sup_{sid}"): st.session_state.edit_sup_id=sid; st.rerun()
                if ct2.button("On" if not sa else "Off",key=f"tog_sup_{sid}"):
                    qry("UPDATE supervisores SET ativo=%s WHERE id=%s AND empresa_id=%s",(not sa,sid,EMPRESA_ID)); invalidar_cache(); st.rerun()
            st.markdown('<div style="border-top:1px solid #f1f5f9;margin:4px 0"></div>',unsafe_allow_html=True)
    else: show_inf("Nenhum supervisor.","Adicione um acima.")

# ══════════════════════════════════════
# FORNECEDORES
# ══════════════════════════════════════
elif menu == "Fornecedores":
    page_header("supplier","Fornecedores","Gerencie seus fornecedores")
    with st.expander("Novo fornecedor"):
        with st.form("f_forn"):
            f1,f2 = st.columns(2); fn=f1.text_input("Nome *"); fdoc=f2.text_input("CNPJ")
            f3,f4,f5 = st.columns(3); ftel=f3.text_input("Telefone"); femail=f4.text_input("E-mail"); fcontato=f5.text_input("Contato")
            sv = st.form_submit_button("Salvar", use_container_width=True)
        if sv:
            if not validate_required(fn): show_err("Nome obrigatório.")
            else:
                res = qry("INSERT INTO fornecedores(empresa_id,nome,documento,telefone,email,contato) VALUES(%s,%s,%s,%s,%s,%s)",
                          (EMPRESA_ID,fn.strip(),fdoc,ftel,femail,fcontato))
                if res is True: show_ok(f"'{fn}' adicionado!"); st.rerun()
                elif res=="duplicate": show_err("Já existe.")
                else: show_err("Erro.")
    hr()
    busca_f = st.text_input("Buscar", placeholder="Nome...", key="busca_forn")
    forns = qry("SELECT id,nome,documento,telefone,email,contato,ativo FROM fornecedores WHERE empresa_id=%s ORDER BY nome",(EMPRESA_ID,),fetch=True)
    if forns:
        if busca_f: b=busca_f.lower(); forns=[f for f in forns if b in (f[1] or "").lower()]
        if HAS_PANDAS:
            xb = to_excel(forns,["ID","Nome","CNPJ","Tel","Email","Contato","Ativo"])
            if xb: st.download_button("Exportar Excel",data=xb,file_name="fornecedores.xlsx",mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        st.markdown(f"**{len(forns)} fornecedor(es)**")
        for fid,fnome,fdoc2,ftel2,femail2,fcont,fativo in forns:
            if st.session_state.edit_forn_id == fid:
                with st.form(f"f_edit_forn_{fid}"):
                    ef1,ef2 = st.columns(2); en=ef1.text_input("Nome *",value=fnome or ""); edoc=ef2.text_input("CNPJ",value=fdoc2 or "")
                    ef3,ef4,ef5 = st.columns(3); etel=ef3.text_input("Tel",value=ftel2 or ""); eemail=ef4.text_input("E-mail",value=femail2 or ""); econt=ef5.text_input("Contato",value=fcont or "")
                    cs,cc = st.columns(2); sv2=cs.form_submit_button("Salvar",use_container_width=True); cn2=cc.form_submit_button("Cancelar",use_container_width=True)
                if cn2: st.session_state.edit_forn_id=None; st.rerun()
                if sv2:
                    if not validate_required(en): show_err("Nome obrigatório.")
                    else:
                        qry("UPDATE fornecedores SET nome=%s,documento=%s,telefone=%s,email=%s,contato=%s WHERE id=%s AND empresa_id=%s",(en.strip(),edoc,etel,eemail,econt,fid,EMPRESA_ID))
                        show_ok("Atualizado!"); st.session_state.edit_forn_id=None; st.rerun()
            else:
                fi2,fe2,ft2 = st.columns([8,1,1])
                fi2.markdown(f'''<div class="card">
                  <div class="card-title">{fnome} {badge("Ativo","b-green") if fativo else badge("Inativo","b-gray")}</div>
                  <div class="card-sub">{fdoc2 or "—"} · {ftel2 or "—"} · {femail2 or "—"}</div>
                </div>''',unsafe_allow_html=True)
                if fe2.button("Ed",key=f"edit_forn_{fid}"): st.session_state.edit_forn_id=fid; st.rerun()
                if ft2.button("On" if not fativo else "Off",key=f"tog_forn_{fid}"):
                    qry("UPDATE fornecedores SET ativo=%s WHERE id=%s AND empresa_id=%s",(not fativo,fid,EMPRESA_ID)); st.rerun()
            st.markdown('<div style="border-top:1px solid #f1f5f9;margin:4px 0"></div>',unsafe_allow_html=True)
    else: show_inf("Nenhum fornecedor.","Adicione um acima.")

# ══════════════════════════════════════
# CLIENTES
# ══════════════════════════════════════
elif menu == "Clientes":
    page_header("clients","Clientes","Gerencie sua base de clientes")
    grupos = get_grupos(EMPRESA_ID)
    with st.expander("Novo cliente"):
        with st.form("f_cli"):
            c1,c2 = st.columns(2); nome=c1.text_input("Nome *"); doc=c2.text_input("CPF/CNPJ *")
            c3,c4,c5 = st.columns(3); tel=c3.text_input("Telefone"); email_c=c4.text_input("E-mail")
            grp_opts=["(sem grupo)"]+[g[1] for g in grupos]
            grp_sel=c5.selectbox("Grupo",grp_opts)
            ea,eb,ec = st.columns([3,1,2]); rua=ea.text_input("Rua"); num=eb.text_input("Nº"); comp=ec.text_input("Compl.")
            ed,ee,ef,eg = st.columns([2,2,1,2]); bairro=ed.text_input("Bairro"); cidade=ee.text_input("Cidade"); estado=ef.text_input("UF",max_chars=2); cep=eg.text_input("CEP")
            sv = st.form_submit_button("Salvar", use_container_width=True)
        if sv:
            if not validate_required(nome,doc): show_err("Nome e CPF/CNPJ obrigatórios.")
            elif not validate_doc(doc): show_err("CPF/CNPJ inválido.")
            else:
                grp_id = next((g[0] for g in grupos if g[1]==grp_sel), None)
                res = qry("INSERT INTO clientes(empresa_id,nome,documento,telefone,email,rua,numero,complemento,bairro,cidade,estado,cep,grupo_id) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                          (EMPRESA_ID,nome.strip(),re.sub(r'\D','',doc),tel,email_c,rua,num,comp,bairro,cidade,estado.upper() if estado else "",cep,grp_id))
                if res is True: invalidar_cache(); show_ok(f"'{nome}' cadastrado!"); st.rerun()
                elif res=="duplicate": show_err("CPF/CNPJ já cadastrado.")
                else: show_err("Erro ao salvar.")

    with st.expander("Importar clientes via CSV"):
        st.caption("Formato: nome, documento, telefone, email, cidade, estado")
        csv_cli = st.file_uploader("CSV de clientes", type=["csv"], key="csv_cli")
        if csv_cli and st.button("Importar", key="btn_imp_cli"):
            content = csv_cli.read().decode("utf-8").splitlines()
            reader = csv.DictReader(content)
            ok=erros=0
            for row_c in reader:
                dc = re.sub(r'\D','',row_c.get("documento",""))
                if not dc: erros+=1; continue
                res = qry("INSERT INTO clientes(empresa_id,nome,documento,telefone,email,cidade,estado) VALUES(%s,%s,%s,%s,%s,%s,%s)",
                          (EMPRESA_ID,row_c.get("nome",""),dc,row_c.get("telefone",""),row_c.get("email",""),row_c.get("cidade",""),row_c.get("estado","")))
                if res is True: ok+=1
                else: erros+=1
            invalidar_cache(); show_ok(f"{ok} importado(s)!", f"{erros} erro(s).")

    hr()
    with st.expander("Filtros", expanded=True):
        cb2,cf2,cg2 = st.columns(3)
        busca_cli = cb2.text_input("Buscar", placeholder="Nome, CPF ou cidade...", key="busca_cli", label_visibility="collapsed")
        filtro_cli = cf2.selectbox("Status", ["Ativos","Inativos","Todos"], key="filtro_cli", label_visibility="collapsed")
        filtro_grp = cg2.selectbox("Grupo", ["Todos"]+[g[1] for g in grupos], key="filtro_grp_cli", label_visibility="collapsed")

    # Usa cache para a lista
    cf_ = get_clientes_ativos(EMPRESA_ID)
    cv = list(cf_)
    if filtro_cli == "Ativos":   cv = [c for c in cv if c[12]]
    if filtro_cli == "Inativos": cv = [c for c in cv if not c[12]]
    if busca_cli:
        b = busca_cli.lower()
        cv = [c for c in cv if b in (c[1] or "").lower() or b in (c[2] or "").lower() or b in (c[9] or "").lower()]
    if filtro_grp != "Todos":
        cv = [c for c in cv if c[13] == filtro_grp]

    if HAS_PANDAS and cv:
        xb = to_excel(cv,["ID","Nome","Doc","Tel","Email","Rua","Nº","Comp","Bairro","Cidade","UF","CEP","Ativo","Grupo"])
        if xb: st.download_button("Exportar Excel",data=xb,file_name="clientes.xlsx",mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    if not cv:
        show_inf("Nenhum cliente encontrado.","Ajuste os filtros.")
    else:
        st.markdown(f"**{len(cv)} cliente(s)**")
        for cli_row in cv:
            cid,cnome,cdoc,ctel,cemail,crua,cnum,ccomp,cbairro,ccidade,cestado,ccep,cativo,cgrp = cli_row
            # badge construído em Python, não como HTML raw do banco
            status_txt = "Ativo" if cativo else "Inativo"
            status_cls = "b-green" if cativo else "b-gray"

            with st.expander(f"{cnome} — {cdoc} · {ccidade or '—'} · {cgrp}"):
                st.markdown(f"{badge(status_txt, status_cls)}  Tel: {ctel or '—'} · Email: {cemail or '—'}", unsafe_allow_html=True)

                col_ed, col_tog = st.columns([1,1])
                if col_ed.button("Editar", key=f"edit_cli_{cid}"):
                    st.session_state.edit_cli_id = cid; st.rerun()
                if col_tog.button("Inativar" if cativo else "Ativar", key=f"tog_cli_{cid}"):
                    qry("UPDATE clientes SET ativo=%s WHERE id=%s AND empresa_id=%s",(not cativo,cid,EMPRESA_ID))
                    invalidar_cache(); st.rerun()

                if st.session_state.edit_cli_id == cid:
                    with st.form(f"f_edit_cli_{cid}"):
                        ec1,ec2 = st.columns(2); en=ec1.text_input("Nome *",value=cnome or ""); edo=ec2.text_input("CPF/CNPJ *",value=cdoc or "")
                        ec3,ec4 = st.columns(2); etl=ec3.text_input("Telefone",value=ctel or ""); eml=ec4.text_input("E-mail",value=cemail or "")
                        eea,eeb,eec = st.columns([3,1,2]); erua=eea.text_input("Rua",value=crua or ""); enum=eeb.text_input("Nº",value=cnum or ""); ecomp=eec.text_input("Compl.",value=ccomp or "")
                        eed,eee,eef,eeg = st.columns([2,2,1,2]); ebairro=eed.text_input("Bairro",value=cbairro or ""); ecidade=eee.text_input("Cidade",value=ccidade or ""); eestado=eef.text_input("UF",value=cestado or "",max_chars=2); ecep=eeg.text_input("CEP",value=ccep or "")
                        cs_e,cc_e = st.columns(2); sv_e=cs_e.form_submit_button("Salvar",use_container_width=True); cn_e=cc_e.form_submit_button("Cancelar",use_container_width=True)
                    if cn_e: st.session_state.edit_cli_id=None; st.rerun()
                    if sv_e:
                        if not validate_required(en,edo): show_err("Campos obrigatórios.")
                        elif not validate_doc(edo): show_err("CPF/CNPJ inválido.")
                        else:
                            res = qry("UPDATE clientes SET nome=%s,documento=%s,telefone=%s,email=%s,rua=%s,numero=%s,complemento=%s,bairro=%s,cidade=%s,estado=%s,cep=%s WHERE id=%s AND empresa_id=%s",
                                      (en.strip(),re.sub(r'\D','',edo),etl,eml,erua,enum,ecomp,ebairro,ecidade,eestado.upper() if eestado else "",ecep,cid,EMPRESA_ID))
                            if res is True: invalidar_cache(); show_ok("Atualizado!"); st.session_state.edit_cli_id=None; st.rerun()
                            elif res=="duplicate": show_err("CPF/CNPJ já existe.")
                            else: show_err("Erro.")

                # Histórico de compras — lazy: só busca ao expandir
                hist_cli = qry("""SELECT v.id,TO_CHAR(v.data,'DD/MM/YYYY'),v.valor_total,v.status
                                  FROM vendas v WHERE v.empresa_id=%s AND v.cliente_name=%s AND v.tipo='Venda'
                                  ORDER BY v.data DESC LIMIT 10""", (EMPRESA_ID,cnome), fetch=True)
                if hist_cli:
                    total_cli = sum(float(h[2]) for h in hist_cli if h[3]=="Pago")
                    st.markdown(f"**Histórico** · Total gasto: **{cur_sym} {total_cli:,.2f}**")
                    for hid,hdata,hval,hst in hist_cli:
                        hcls = "b-blue" if hst=="Pago" else "b-red"
                        st.markdown(f'<div style="display:flex;justify-content:space-between;padding:.3rem 0;font-size:.82rem;border-bottom:1px solid #f1f5f9"><span>#{hid} · {hdata} {badge(hst,hcls)}</span><b style="color:#1d4ed8">{cur_sym} {float(hval):,.2f}</b></div>', unsafe_allow_html=True)
                else:
                    st.caption("Nenhuma compra registrada.")

# ══════════════════════════════════════
# ESTOQUE
# ══════════════════════════════════════
elif menu == "Estoque":
    page_header("stock","Estoque","Gerencie seu catálogo de produtos")
    cat_opts_raw = get_categorias(EMPRESA_ID)
    cat_opts = [c[0] for c in cat_opts_raw]

    with st.expander("Adicionar produto"):
        if not cat_opts: show_wrn("Nenhuma categoria ativa.","Acesse Categorias e crie uma.")
        else:
            with st.form("f_prod"):
                c1,c2,c3 = st.columns([1,2,1])
                sku=c1.text_input("SKU *"); nome_p=c2.text_input("Nome *"); cat=c3.selectbox("Categoria *",cat_opts)
                c4,c5,c6,c7,c8 = st.columns(5)
                pc=c4.number_input(f"Custo ({cur_sym})",min_value=0.0,step=0.01,format="%.2f")
                pv=c5.number_input(f"Venda ({cur_sym}) *",min_value=0.0,step=0.01,format="%.2f")
                est=c6.number_input("Estoque",min_value=0,step=1)
                emin=c7.number_input("Est.Mín.",min_value=0,step=1,value=2)
                cb_p=c8.text_input("Cód.Barras")
                sv = st.form_submit_button("Salvar", use_container_width=True)
            if sv:
                if not validate_required(sku,nome_p): show_err("SKU e Nome obrigatórios.")
                elif pv==0: show_err("Preço de venda não pode ser zero.")
                else:
                    res = qry("INSERT INTO produtos(empresa_id,sku,nome,categoria,preco_custo,preco_venda,estoque_atual,estoque_minimo,codigo_barras) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                              (EMPRESA_ID,sku.strip(),nome_p.strip(),cat,pc,pv,est,emin,cb_p.strip() or None))
                    if res is True:
                        if est>0:
                            pid_n = qry("SELECT id FROM produtos WHERE empresa_id=%s AND sku=%s",(EMPRESA_ID,sku.strip()),fetch=True)
                            if pid_n:
                                qry("INSERT INTO movimentacoes_estoque(empresa_id,produto_id,tipo,quantidade,motivo,usuario_nome) VALUES(%s,%s,'Entrada',%s,'Estoque inicial',%s)",
                                    (EMPRESA_ID,pid_n[0][0],est,st.session_state.usuario_nome))
                        invalidar_cache(); show_ok(f"'{nome_p}' cadastrado!"); st.rerun()
                    elif res=="duplicate": show_err(f"SKU '{sku}' já existe.")
                    else: show_err("Erro ao salvar.")

    with st.expander("Importar via CSV"):
        st.caption("Formato: sku, nome, categoria, preco_custo, preco_venda, estoque_inicial, estoque_minimo")
        csv_p = st.file_uploader("CSV", type=["csv"], key="csv_prod")
        if csv_p and st.button("Importar", key="btn_imp_prod"):
            content = csv_p.read().decode("utf-8").splitlines()
            reader = csv.DictReader(content)
            ok=erros=0
            for row_c in reader:
                try:
                    res = qry("INSERT INTO produtos(empresa_id,sku,nome,categoria,preco_custo,preco_venda,estoque_atual,estoque_minimo) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)",
                              (EMPRESA_ID,row_c["sku"],row_c["nome"],row_c.get("categoria","Geral"),
                               float(row_c.get("preco_custo",0)),float(row_c.get("preco_venda",0)),
                               int(row_c.get("estoque_inicial",0)),int(row_c.get("estoque_minimo",2))))
                    if res is True: ok+=1
                    else: erros+=1
                except: erros+=1
            invalidar_cache(); show_ok(f"{ok} importado(s)!",f"{erros} erro(s).")

    hr()
    with st.expander("Filtros", expanded=True):
        fb,ff = st.columns([3,1])
        busca_p = fb.text_input("Buscar",placeholder="Nome ou SKU...",key="busca_prod",label_visibility="collapsed")
        mostrar = ff.selectbox("Status",["Ativos","Inativos","Todos"],key="filtro_prod",label_visibility="collapsed")

    # Usa cache
    pr = get_produtos_ativos(EMPRESA_ID)
    fl = list(pr)
    if mostrar=="Ativos":   fl=[p for p in fl if p[8]]
    if mostrar=="Inativos": fl=[p for p in fl if not p[8]]
    if busca_p:
        b=busca_p.lower(); fl=[p for p in fl if b in p[2].lower() or b in p[1].lower()]

    if not fl:
        show_inf("Nenhum produto.","Adicione um acima.")
    else:
        if HAS_PANDAS:
            xb = to_excel(fl,["ID","SKU","Nome","Cat","Custo","Venda","Estoque","Est.Mín","Ativo","Cód.Barras"])
            if xb: st.download_button("Exportar Excel",data=xb,file_name="produtos.xlsx",mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

        tabs_est = st.tabs(["Lista","Movimentações"])

        with tabs_est[0]:
            st.markdown(f"**{len(fl)} produto(s)**")
            for prod in fl:
                pid,sv2,nm,cat_v,pc_v,pv_v,est_v,emin_v,ativo_v,cb_v = prod
                est_cls = "b-red" if est_v==0 else ("b-yellow" if est_v<=emin_v else "b-green")
                est_lbl = "Zerado" if est_v==0 else ("Baixo" if est_v<=emin_v else "OK")
                margem = ((float(pv_v)-float(pc_v))/float(pv_v)*100) if float(pv_v)>0 else 0
                ic = "" if ativo_v else " inativo"
                cc2,ce,ca,ct = st.columns([5,1,1,1])
                cc2.markdown(f'''<div class="card{ic}">
                  <div class="card-label">SKU: {sv2}{f" · {cb_v}" if cb_v else ""}</div>
                  <div class="card-title">{nm}</div>
                  <div class="card-sub">{cat_v} · Estoque: {est_v} {badge(est_lbl,est_cls)} · Margem: {margem:.1f}%</div>
                  <div class="card-val">{cur_sym} {float(pv_v):.2f}</div>
                </div>''',unsafe_allow_html=True)
                if ce.button("Ed",key=f"edit_{pid}"):
                    st.session_state.editing_prod_id=pid; st.session_state.adj_prod=None; st.rerun()
                if ca.button("Adj",key=f"adj_{pid}"):
                    st.session_state.adj_prod=prod; st.session_state.editing_prod_id=None; st.rerun()
                if ct.button("On" if not ativo_v else "Off",key=f"tog_{pid}"):
                    qry("UPDATE produtos SET ativo=%s WHERE id=%s AND empresa_id=%s",(not ativo_v,pid,EMPRESA_ID)); invalidar_cache(); st.rerun()

            if st.session_state.editing_prod_id:
                pid_e = st.session_state.editing_prod_id
                pd_ = next((p for p in pr if p[0]==pid_e), None)
                if pd_:
                    _,sku_e,nm_e,cat_e,pc_e,pv_e,est_e,emin_e,_,cb_e = pd_
                    hr(); st.markdown(f"**Editando: {nm_e}**")
                    with st.form("f_edit_prod"):
                        ce1,ce2,ce3 = st.columns([1,2,1])
                        ns=ce1.text_input("SKU *",value=sku_e); nn=ce2.text_input("Nome *",value=nm_e)
                        ci_=cat_opts.index(cat_e) if cat_e in cat_opts else 0
                        nc2=ce3.selectbox("Categoria *",cat_opts or [cat_e],index=ci_)
                        ce4,ce5,ce6,ce7,ce8 = st.columns(5)
                        np2=ce4.number_input(f"Custo ({cur_sym})",value=float(pc_e),min_value=0.0,step=0.01,format="%.2f")
                        npv=ce5.number_input(f"Venda ({cur_sym}) *",value=float(pv_e),min_value=0.0,step=0.01,format="%.2f")
                        ne=ce6.number_input("Estoque",value=int(est_e),min_value=0,step=1)
                        nem=ce7.number_input("Est.Mín.",value=int(emin_e),min_value=0,step=1)
                        ncb=ce8.text_input("Cód.Barras",value=cb_e or "")
                        cs,cc3 = st.columns(2)
                        se=cs.form_submit_button("Salvar",use_container_width=True)
                        ce_b=cc3.form_submit_button("Cancelar",use_container_width=True)
                    if ce_b: st.session_state.editing_prod_id=None; st.rerun()
                    if se:
                        if not validate_required(ns,nn): show_err("SKU e Nome obrigatórios.")
                        elif npv==0: show_err("Preço não pode ser zero.")
                        else:
                            res = qry("UPDATE produtos SET sku=%s,nome=%s,categoria=%s,preco_custo=%s,preco_venda=%s,estoque_atual=%s,estoque_minimo=%s,codigo_barras=%s WHERE id=%s AND empresa_id=%s",
                                      (ns.strip(),nn.strip(),nc2,np2,npv,ne,nem,ncb.strip() or None,pid_e,EMPRESA_ID))
                            if res is True: invalidar_cache(); show_ok("Produto atualizado!"); st.session_state.editing_prod_id=None; st.rerun()
                            elif res=="duplicate": show_err("SKU já existe.")
                            else: show_err("Erro.")

            if st.session_state.adj_prod:
                adj=st.session_state.adj_prod; adj_id=adj[0]; adj_nm=adj[2]; adj_est=int(adj[6])
                hr(); st.markdown(f"**Ajustar estoque: {adj_nm}** (atual: {adj_est})")
                with st.form("f_adj"):
                    co,cq = st.columns(2)
                    op=co.selectbox("Operação",["Adicionar","Remover","Definir exato"])
                    qa2=cq.number_input("Quantidade",min_value=1,step=1)
                    motivo_adj=st.text_input("Motivo (opcional)")
                    ca2,cb2 = st.columns(2)
                    ap=ca2.form_submit_button("Aplicar",use_container_width=True)
                    cn=cb2.form_submit_button("Cancelar",use_container_width=True)
                if cn: st.session_state.adj_prod=None; st.rerun()
                if ap:
                    if op=="Adicionar": sq="UPDATE produtos SET estoque_atual=estoque_atual+%s WHERE id=%s AND empresa_id=%s"; nv=adj_est+qa2; tm="Entrada"
                    elif op=="Remover":
                        if qa2>adj_est: show_err(f"Máximo: {adj_est} un."); st.stop()
                        sq="UPDATE produtos SET estoque_atual=estoque_atual-%s WHERE id=%s AND empresa_id=%s"; nv=adj_est-qa2; tm="Saída"
                    else: sq="UPDATE produtos SET estoque_atual=%s WHERE id=%s AND empresa_id=%s"; nv=qa2; tm="Ajuste"
                    par=(qa2 if op!="Definir exato" else nv,adj_id,EMPRESA_ID)
                    if qry(sq,par) is True:
                        qry("INSERT INTO movimentacoes_estoque(empresa_id,produto_id,tipo,quantidade,motivo,usuario_nome) VALUES(%s,%s,%s,%s,%s,%s)",
                            (EMPRESA_ID,adj_id,tm,qa2,motivo_adj or op,st.session_state.usuario_nome))
                        invalidar_cache(); show_ok("Estoque ajustado!",f"'{adj_nm}' agora tem {nv} un.")
                        st.session_state.adj_prod=None; st.rerun()
                    else: show_err("Erro ao ajustar.")

        with tabs_est[1]:
            prod_filtro = st.selectbox("Produto",["Todos"]+[p[2] for p in fl],key="mov_prod_fil")
            movs_q = "SELECT m.criado_em,p.nome,m.tipo,m.quantidade,m.motivo,m.usuario_nome FROM movimentacoes_estoque m JOIN produtos p ON p.id=m.produto_id WHERE m.empresa_id=%s"
            movs_p = [EMPRESA_ID]
            if prod_filtro!="Todos":
                pid_f=next((p[0] for p in fl if p[2]==prod_filtro),None)
                if pid_f: movs_q+=" AND m.produto_id=%s"; movs_p.append(pid_f)
            movs_q+=" ORDER BY m.criado_em DESC LIMIT 100"
            movs=qry(movs_q,tuple(movs_p),fetch=True)
            if movs:
                if HAS_PANDAS:
                    xb=to_excel(movs,["Data","Produto","Tipo","Qtd","Motivo","Usuário"])
                    if xb: st.download_button("Exportar",data=xb,file_name="movimentacoes.xlsx",mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                for data_m,pnome,tipo_m,qtd_m,mot_m,usr_m in movs:
                    cor = "b-green" if tipo_m=="Entrada" else ("b-red" if tipo_m=="Saída" else "b-yellow")
                    data_fmt=data_m.strftime("%d/%m/%Y %H:%M") if data_m else "—"
                    st.markdown(f'''<div class="card">
                      <div class="card-label">{data_fmt} · {usr_m or "—"}</div>
                      <div style="display:flex;justify-content:space-between">
                        <div><div class="card-title">{pnome}</div><div class="card-sub">{mot_m or "—"}</div></div>
                        <div>{badge(tipo_m,cor)} <b>{qtd_m}</b></div>
                      </div></div>''',unsafe_allow_html=True)
            else: show_inf("Nenhuma movimentação.")

# ══════════════════════════════════════
# ENTRADA NF
# ══════════════════════════════════════
elif menu == "Entrada NF":
    page_header("nf","Entrada de Nota Fiscal","Registre entradas de mercadoria")
    forns_nf = qry("SELECT nome FROM fornecedores WHERE empresa_id=%s AND ativo=TRUE ORDER BY nome",(EMPRESA_ID,),fetch=True)
    prods_nf = get_produtos_ativos(EMPRESA_ID)
    prods_ativos_nf = [p for p in prods_nf if p[8]]

    with st.form("f_nf_item", clear_on_submit=True):
        st.markdown("**Adicionar item**")
        n1,n2,n3 = st.columns([3,1,1])
        pn_nf = n1.selectbox("Produto",[p[2] for p in prods_ativos_nf]) if prods_ativos_nf else None
        qtd_nf=n2.number_input("Qtd *",min_value=1,step=1)
        custo_nf=n3.number_input(f"Custo ({cur_sym})",min_value=0.0,step=0.01,format="%.2f")
        add_nf=st.form_submit_button("Adicionar",use_container_width=True)
    if add_nf and pn_nf:
        prod_sel=next((p for p in prods_ativos_nf if p[2]==pn_nf),None)
        if prod_sel:
            st.session_state.nf_itens.append({"id":prod_sel[0],"nome":prod_sel[2],"qtd":qtd_nf,"custo":custo_nf})
            st.rerun()

    if st.session_state.nf_itens:
        st.markdown("**Itens da NF:**")
        total_nf=0
        for i,it in enumerate(st.session_state.nf_itens):
            sub=it["custo"]*it["qtd"]; total_nf+=sub
            ci,cr=st.columns([8,1])
            ci.markdown(f'<div class="cart-row"><b>{it["nome"]}</b> · {it["qtd"]} un × {cur_sym} {it["custo"]:.2f} = <b>{cur_sym} {sub:.2f}</b></div>',unsafe_allow_html=True)
            if cr.button("X",key=f"rem_nf_{i}"): st.session_state.nf_itens.pop(i); st.rerun()
        st.markdown(f'<div class="cart-total-bar"><span>Total NF</span><strong>{cur_sym} {total_nf:,.2f}</strong></div>',unsafe_allow_html=True)

        with st.form("f_nf_final"):
            fn1,fn2=st.columns(2)
            num_nf=fn1.text_input("Número da NF")
            forn_sel=fn2.selectbox("Fornecedor",["(sem fornecedor)"]+([f[0] for f in forns_nf] if forns_nf else []))
            obs_nf=st.text_area("Observação",height=50)
            salvar_nf=st.form_submit_button("Registrar Entrada",use_container_width=True)
        if salvar_nf:
            row_nf=qry("INSERT INTO entradas_nf(empresa_id,numero_nf,fornecedor_nome,valor_total,observacao) VALUES(%s,%s,%s,%s,%s) RETURNING id",
                       (EMPRESA_ID,num_nf or None,forn_sel if forn_sel!="(sem fornecedor)" else None,total_nf,obs_nf),returning=True)
            if row_nf:
                eid_nf=row_nf[0]
                for it in st.session_state.nf_itens:
                    qry("INSERT INTO itens_entrada_nf(empresa_id,entrada_id,produto_id,produto_nome,quantidade,preco_custo) VALUES(%s,%s,%s,%s,%s,%s)",
                        (EMPRESA_ID,eid_nf,it["id"],it["nome"],it["qtd"],it["custo"]))
                    qry("UPDATE produtos SET estoque_atual=estoque_atual+%s,preco_custo=%s WHERE id=%s AND empresa_id=%s",
                        (it["qtd"],it["custo"],it["id"],EMPRESA_ID))
                    qry("INSERT INTO movimentacoes_estoque(empresa_id,produto_id,tipo,quantidade,motivo,usuario_nome) VALUES(%s,%s,'Entrada',%s,%s,%s)",
                        (EMPRESA_ID,it["id"],it["qtd"],f"NF {num_nf or eid_nf}",st.session_state.usuario_nome))
                invalidar_cache()
                log_acao(f"Entrada NF #{eid_nf}",f"NF: {num_nf} · {cur_sym} {total_nf:.2f}")
                show_ok("Entrada registrada!",f"{len(st.session_state.nf_itens)} produto(s) atualizados.")
                st.session_state.nf_itens=[]; st.rerun()

    hr(); st.markdown("**Entradas recentes**")
    ents=qry("SELECT id,data,numero_nf,fornecedor_nome,valor_total FROM entradas_nf WHERE empresa_id=%s ORDER BY criado_em DESC LIMIT 20",(EMPRESA_ID,),fetch=True)
    for eid_e,data_e,nf_e,forn_e,val_e in (ents or []):
        st.markdown(f'''<div class="card">
          <div class="card-label">NF {nf_e or "s/n"} · {data_e.strftime("%d/%m/%Y") if data_e else "—"}</div>
          <div style="display:flex;justify-content:space-between">
            <div class="card-title">{forn_e or "Sem fornecedor"}</div>
            <div class="card-val">{cur_sym} {float(val_e):,.2f}</div>
          </div></div>''',unsafe_allow_html=True)

# ══════════════════════════════════════
# COMISSÕES (com edição de % inline)
# ══════════════════════════════════════
elif menu == "Comissões":
    page_header("commission","Comissões","Gerencie e acompanhe comissões")
    c1,c2=st.columns(2)
    com_ini=c1.date_input("Início",value=date.today().replace(day=1),key="com_ini")
    com_fim=c2.date_input("Fim",value=date.today(),key="com_fim")
    di_c=datetime.combine(com_ini,datetime.min.time())
    df_c=datetime.combine(com_fim,datetime.max.time())

    tab_sup,tab_rep=st.tabs(["Supervisores","Representantes"])

    with tab_sup:
        sups_com=qry("""
            SELECT s.id,s.nome,s.comissao_pct,
                   COUNT(v.id) FILTER(WHERE v.status='Pago'),
                   COALESCE(SUM(v.valor_total) FILTER(WHERE v.status='Pago'),0),
                   COALESCE(SUM(v.comissao_supervisor) FILTER(WHERE v.status='Pago'),0)
            FROM supervisores s
            LEFT JOIN vendas v ON v.supervisor_id=s.id AND v.data>=%s AND v.data<=%s
            WHERE s.empresa_id=%s
            GROUP BY s.id,s.nome,s.comissao_pct ORDER BY s.nome
        """,(di_c,df_c,EMPRESA_ID),fetch=True)

        for row_s in (sups_com or []):
            sid_c,snome,spct,sped,stot,scom=row_s
            stot=float(stot or 0); scom=float(scom or 0)
            with st.expander(f"{snome} · Comissão: {float(spct):.1f}% · {sped or 0} pedido(s) · {cur_sym} {scom:,.2f}"):
                st.markdown(f"**Faturou:** {cur_sym} {stot:,.2f} · **Comissão calculada:** {cur_sym} {scom:,.2f}")
                # Edição inline da % de comissão
                with st.form(f"f_edit_com_sup_{sid_c}"):
                    nova_pct=st.number_input("Alterar comissão (%)",value=float(spct),min_value=0.0,max_value=100.0,step=0.5,format="%.2f")
                    if st.form_submit_button("Salvar %"):
                        qry("UPDATE supervisores SET comissao_pct=%s WHERE id=%s AND empresa_id=%s",(nova_pct,sid_c,EMPRESA_ID))
                        invalidar_cache(); show_ok("Comissão atualizada!"); st.rerun()

    with tab_rep:
        reps_com=qry("""
            SELECT r.id,r.nome,r.comissao_pct,r.regiao,
                   COUNT(v.id) FILTER(WHERE v.status='Pago'),
                   COALESCE(SUM(v.valor_total) FILTER(WHERE v.status='Pago'),0),
                   COALESCE(SUM(v.comissao_representante) FILTER(WHERE v.status='Pago'),0)
            FROM representantes r
            LEFT JOIN vendas v ON v.representante_id=r.id AND v.data>=%s AND v.data<=%s
            WHERE r.empresa_id=%s
            GROUP BY r.id,r.nome,r.comissao_pct,r.regiao ORDER BY r.nome
        """,(di_c,df_c,EMPRESA_ID),fetch=True)

        for row_r in (reps_com or []):
            rid_c,rnome,rpct,rregiao,rped,rtot,rcom=row_r
            rtot=float(rtot or 0); rcom=float(rcom or 0)
            with st.expander(f"{rnome} · {rregiao or '—'} · Comissão: {float(rpct):.1f}% · {rped or 0} pedido(s) · {cur_sym} {rcom:,.2f}"):
                st.markdown(f"**Faturou:** {cur_sym} {rtot:,.2f} · **Comissão calculada:** {cur_sym} {rcom:,.2f}")
                with st.form(f"f_edit_com_rep_{rid_c}"):
                    nova_pct_r=st.number_input("Alterar comissão (%)",value=float(rpct),min_value=0.0,max_value=100.0,step=0.5,format="%.2f")
                    if st.form_submit_button("Salvar %"):
                        qry("UPDATE representantes SET comissao_pct=%s WHERE id=%s AND empresa_id=%s",(nova_pct_r,rid_c,EMPRESA_ID))
                        invalidar_cache(); show_ok("Comissão atualizada!"); st.rerun()

    if pode("comissoes_pagar"):
        hr()
        if st.button("Marcar todas como Pagas no período"):
            qry("UPDATE vendas SET comissao_status='Pago' WHERE empresa_id=%s AND status='Pago' AND data>=%s AND data<=%s",(EMPRESA_ID,di_c,df_c))
            show_ok("Comissões pagas!"); st.rerun()

# ══════════════════════════════════════
# NOTIFICAÇÕES
# ══════════════════════════════════════
elif menu == "Notificações":
    page_header("notif","Notificações",f"{NOTIF_COUNT} não lida(s)")
    if NOTIF_COUNT>0:
        if st.button("Marcar todas como lidas"):
            qry("UPDATE notificacoes SET lida=TRUE WHERE empresa_id=%s",(EMPRESA_ID,)); st.rerun()
    notifs=qry("SELECT id,titulo,mensagem,tipo,lida,criado_em FROM notificacoes WHERE empresa_id=%s ORDER BY criado_em DESC LIMIT 50",(EMPRESA_ID,),fetch=True)
    if not notifs: show_inf("Nenhuma notificação.")
    else:
        for nid,ntit,nmsg,ntipo,nlida,ncriado in notifs:
            data_n=ncriado.strftime("%d/%m/%Y %H:%M") if ncriado else "—"
            op=0.55 if nlida else 1.0
            tipo_cls={"estoque":"estoque","receber":"receber"}.get(ntipo,"info")
            st.markdown(f'<div class="notif-item {tipo_cls}" style="opacity:{op}"><b>{"" if nlida else "• "}{ntit}</b><div style="font-size:.79rem;color:#555">{nmsg or ""}</div><div style="font-size:.7rem;color:#94a3b8">{data_n}</div></div>',unsafe_allow_html=True)
            if not nlida:
                if st.button("Marcar lida",key=f"lida_{nid}"):
                    qry("UPDATE notificacoes SET lida=TRUE WHERE id=%s AND empresa_id=%s",(nid,EMPRESA_ID)); st.rerun()

# ══════════════════════════════════════
# LOG DE AÇÕES
# ══════════════════════════════════════
elif menu == "Log de Ações":
    if not pode("log"): show_err("Acesso restrito a administradores."); st.stop()
    page_header("log","Log de Ações","Auditoria de operações")
    l1,l2=st.columns(2)
    log_ini=l1.date_input("Início",value=date.today()-timedelta(days=7),key="log_ini")
    log_usr=l2.text_input("Usuário",placeholder="Nome...",key="log_usr")
    log_q="SELECT usuario_nome,acao,detalhes,TO_CHAR(criado_em,'DD/MM/YYYY HH24:MI') FROM log_acoes WHERE empresa_id=%s AND criado_em>=%s"
    log_p=[EMPRESA_ID,datetime.combine(log_ini,datetime.min.time())]
    if log_usr: log_q+=" AND usuario_nome ILIKE %s"; log_p.append(f"%{log_usr}%")
    log_q+=" ORDER BY criado_em DESC LIMIT 200"
    logs=qry(log_q,tuple(log_p),fetch=True)
    if logs:
        if HAS_PANDAS:
            xb=to_excel(logs,["Usuário","Ação","Detalhes","Data"])
            if xb: st.download_button("Exportar Log",data=xb,file_name="log.xlsx",mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        st.markdown(f"**{len(logs)} registro(s)**")
        for usr,acao,det,data_l in logs:
            st.markdown(f'''<div class="card">
              <div class="card-label">{data_l} · {usr or "—"}</div>
              <div class="card-title">{acao}</div>
              <div class="card-sub">{det or ""}</div>
            </div>''',unsafe_allow_html=True)
    else: show_inf("Nenhum registro.")

# ══════════════════════════════════════
# CONFIGURAÇÕES
# ══════════════════════════════════════
elif menu == "Configurações":
    page_header("settings","Configurações","Empresa, usuários e senha")
    tabs_cfg=st.tabs(["Empresa","Redefinir Senha","Usuários"])

    with tabs_cfg[0]:
        er=qry("SELECT nome,moeda FROM empresas WHERE id=%s",(EMPRESA_ID,),fetch=True)
        ena=er[0][0] if er else ""; ema=er[0][1] if er and er[0][1] else "R$"
        with st.form("f_config"):
            nn=st.text_input("Nome da Empresa *",value=ena)
            ms=["R$","$","€","£"]; im=ms.index(ema) if ema in ms else 0
            nc2=st.selectbox("Moeda padrão",ms,index=im)
            saved=st.form_submit_button("Salvar",use_container_width=True)
        if saved:
            if not validate_required(nn): show_err("Nome obrigatório.")
            else:
                qry("UPDATE empresas SET nome=%s,moeda=%s WHERE id=%s",(nn.strip(),nc2,EMPRESA_ID))
                st.session_state.empresa_nome=nn.strip(); st.session_state.empresa_moeda=nc2
                show_ok("Salvo!","Alterações ativas."); st.rerun()

    with tabs_cfg[1]:
        with st.form("f_redef"):
            rd1,rd2=st.columns(2); email_rd=rd1.text_input("E-mail *"); token_rd=rd2.text_input("Token *")
            nova_senha=st.text_input("Nova senha *",type="password")
            sv_rd=st.form_submit_button("Redefinir senha",use_container_width=True)
        if sv_rd:
            tok_row=run_query("SELECT id FROM recuperacao_senha WHERE email=%s AND token=%s AND expira_em>NOW() AND usado=FALSE",
                              (email_rd.strip().lower(),token_rd.strip()),fetch=True)
            if tok_row:
                run_query("UPDATE usuarios SET senha_hash=%s WHERE email=%s",(hash_pw(nova_senha),email_rd.strip().lower()))
                run_query("UPDATE recuperacao_senha SET usado=TRUE WHERE email=%s AND token=%s",(email_rd.strip().lower(),token_rd.strip()))
                show_ok("Senha redefinida!")
            else: show_err("Token inválido ou expirado.")

    with tabs_cfg[2]:
        if st.session_state.usuario_perfil!="admin":
            show_wrn("Acesso restrito a administradores.")
        else:
            with st.expander("Novo usuário"):
                with st.form("f_novo_user"):
                    cu1,cu2=st.columns(2); un=cu1.text_input("Nome *"); ue=cu2.text_input("E-mail *")
                    cu3,cu4=st.columns(2); us=cu3.text_input("Senha *",type="password"); up=cu4.selectbox("Perfil",["operador","admin"])
                    au=st.form_submit_button("Criar Usuário",use_container_width=True)
                if au:
                    if not validate_required(un,ue,us): show_err("Todos os campos são obrigatórios.")
                    else:
                        res=qry("INSERT INTO usuarios(empresa_id,nome,email,senha_hash,perfil) VALUES(%s,%s,%s,%s,%s)",
                                (EMPRESA_ID,un.strip(),ue.strip().lower(),hash_pw(us),up))
                        if res is True: show_ok(f"'{un}' criado!"); st.rerun()
                        elif res=="duplicate": show_err("E-mail já existe.")
                        else: show_err("Erro.")
            usuarios=qry("SELECT id,nome,email,perfil,ativo FROM usuarios WHERE empresa_id=%s ORDER BY nome",(EMPRESA_ID,),fetch=True)
            if usuarios:
                for uid2,un2,ue2,up2,ua2 in usuarios:
                    sb2=badge("Ativo","b-green") if ua2 else badge("Inativo","b-gray")
                    cu,cut=st.columns([9,1])
                    cu.markdown(f'<div class="card"><div class="card-title">{un2} &nbsp;{sb2}</div><div class="card-sub">{ue2} · {up2}</div></div>',unsafe_allow_html=True)
                    if uid2!=st.session_state.usuario_id:
                        if cut.button("On" if not ua2 else "Off",key=f"tog_u_{uid2}"):
                            qry("UPDATE usuarios SET ativo=%s WHERE id=%s AND empresa_id=%s",(not ua2,uid2,EMPRESA_ID)); st.rerun()