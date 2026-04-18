-- ═══════════════════════════════════════════════════════════
--  ERP SaaS — Setup inicial do banco PostgreSQL
--  Execute este script UMA vez após criar o banco vazio.
--  Depois disso, o init_db() do app cuida das tabelas.
-- ═══════════════════════════════════════════════════════════

-- 1. Adicionar coluna moeda à tabela empresas (caso init_db já tenha rodado)
ALTER TABLE empresas ADD COLUMN IF NOT EXISTS moeda TEXT DEFAULT 'R$';

-- 2. Criar empresa de exemplo
INSERT INTO empresas (nome, plano, ativo, moeda)
VALUES ('Minha Empresa Ltda', 'basico', TRUE, 'R$')
ON CONFLICT DO NOTHING;

-- 3. Criar usuário admin para essa empresa
--    Senha: admin123  (SHA-256 abaixo)
--    TROQUE A SENHA EM PRODUÇÃO!
INSERT INTO usuarios (empresa_id, nome, email, senha_hash, perfil, ativo)
VALUES (
    (SELECT id FROM empresas WHERE nome = 'Minha Empresa Ltda' LIMIT 1),
    'Administrador',
    'admin@minhaempresa.com',
    '240be518fabd2724ddb6f04eeb1da5967448d7e831c08c8fa822809f74c720a9', -- admin123
    'admin',
    TRUE
)
ON CONFLICT (email) DO NOTHING;

-- ───────────────────────────────────────────────────────────
--  Para adicionar mais empresas e usuários:
-- ───────────────────────────────────────────────────────────
-- INSERT INTO empresas (nome, plano, ativo, moeda)
-- VALUES ('Outra Empresa SA', 'pro', TRUE, 'R$');
--
-- INSERT INTO usuarios (empresa_id, nome, email, senha_hash, perfil, ativo)
-- VALUES (
--     (SELECT id FROM empresas WHERE nome = 'Outra Empresa SA' LIMIT 1),
--     'Gestor',
--     'gestor@outraempresa.com',
--     'hash_da_senha_aqui',   -- use: SELECT encode(digest('sua_senha', 'sha256'), 'hex');
--     'admin',
--     TRUE
-- );
-- ───────────────────────────────────────────────────────────
--  Hash SHA-256 via SQL:
--  SELECT encode(digest('minha_senha', 'sha256'), 'hex');
--  (requer extensão pgcrypto: CREATE EXTENSION IF NOT EXISTS pgcrypto;)
-- ═══════════════════════════════════════════════════════════
