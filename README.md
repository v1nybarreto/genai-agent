# Agente GenAI — Prefeitura do Rio (Desafio Técnico)

Agente de dados inteligente em **LangGraph**, capaz de transformar perguntas em linguagem natural em **insights acionáveis** a partir do BigQuery público (`datario`). Ele gera **SQL otimizado**, valida via **dry-run**, executa consultas de forma segura e sintetiza respostas em **português** — com uso opcional de **LLM** para maior naturalidade.

---

## Tabelas centrais
- `datario.adm_central_atendimento_1746.chamado` — fatos (chamados), milhões de linhas, particionada por `data_particao`
- `datario.dados_mestres.bairro` — dimensão de bairros, usada em JOINs

---

## Stack
- Python 3.12  
- LangGraph / LangChain  
- Google BigQuery (Sandbox, com Application Default Credentials)  
- Pandas e PyArrow  
- OpenAI GPT (`gpt-4o-mini`)  
- Ruff, pytest, pre-commit  
- Streamlit (UI profissional para interação via navegador)

---

## Setup rápido

```bash
# Criar ambiente virtual
python3 -m venv .venv
source .venv/bin/activate

# Instalar dependências
pip install -r requirements.txt

# Copiar variáveis de ambiente
cp .env.example .env.local
# edite .env.local e insira sua OPENAI_API_KEY (opcional)

# Instalar hooks de pre-commit
pre-commit install
```

### Variáveis de ambiente
No `.env.local`:
```bash
PROJECT_ID=genai-rio
BQ_LOCATION=US
BQ_MAX_BYTES_BILLED=2000000000   
BQ_QUERY_TIMEOUT=60             
LLM_PROVIDER=OPENAI
OPENAI_MODEL=gpt-4o-mini
LLM_USE_FOR_SYNTH=1
OPENAI_API_KEY=sk-xxxxxxx        
```

---

## Uso via CLI

```bash
# Rodar o agente diretamente
make ask Q="Quantos chamados foram abertos no dia 28/11/2024?"

# Testar as 6 perguntas do desafio com preview de DF e SQL
make accept

# Executar smoke test rápido
make smoke
```

---

## Interface Streamlit (UI)

```bash
# inicia a interface Streamlit
make ui

# inicia a interface Streamlit com autoreload
make ui-dev

# encerra processos e reinicia Streamlit
make ui-reset 
```

- Abre na porta **8501** → no Cloud Shell, use **Web Preview**.  
- Recursos da UI:
  - Entrada de pergunta em linguagem natural  
  - Botões com presets (perguntas do desafio)  
  - Resposta final (LLM ou fallback)  
  - SQL gerado e validação (dry-run)  
  - Preview do DataFrame e download CSV  
  - Gráfico automático (barras ou métricas)  
  - Observabilidade: intent, bytes estimados, latência, versão do grafo  
  - Toggle LLM on/off  

---

## Testes

```bash
# Checagem de estilo/lint
make lint

# Rodar toda a suíte de testes unitários e end-to-end
make test

# Executar apenas os testes de aceitação
make accept
```

A suíte cobre:
- Roteamento de intenção (dados vs chitchat)  
- Geração e validação de SQL (sem `SELECT *`, com filtros de partição)  
- Execução real no BigQuery (com dry-run + execução real)  
- Síntese de resposta via LLM (quando habilitado)  
- Fallback determinístico quando LLM não está ativo  
- Fluxo da interface Streamlit (robustez e integração)  

---

## Arquitetura do agente

Fluxo principal (**LangGraph**):

```mermaid
flowchart TD
    A[Usuário] --> B[Roteador de Intenção]
    B -->|Pergunta analítica| C[Gerador de SQL]
    B -->|Pergunta conversacional| F[Chitchat → LLM]
    C --> D[Validador / Guardas]
    D -->|Dry-run OK| E[Executor BigQuery]
    E --> G[Sintetizador de Resposta]
    D -->|Erro / Bloqueio| H[Fallback / Mensagem]
```

### Nós principais:
1. **Roteador de Intenção** → classifica pergunta como `data` ou `chitchat`.  
2. **Gerador de SQL** → cria query otimizada (sem `SELECT *`, filtros por data, JOIN com bairros se necessário).  
3. **Validador/Guardas** → faz `dry-run`, coleta `bytes_processed`, bloqueia DML/DDL, aplica `maximum_bytes_billed`.  
4. **Executor de SQL** → roda no BigQuery e retorna `DataFrame`.  
5. **Sintetizador de Resposta** → gera texto em português com LLM (opcional) ou fallback determinístico.  
6. **Interface Streamlit** → camada de interação amigável e observabilidade.  

### Extras implementados:
- **Janela temporal defensiva**: restringe a consultas dos **últimos 365 dias** se usuário não especificar período.  
- **Telemetria**: cada execução inclui `graph_version`, `latency_ms`, `dry_run_bytes`.  
- **Guardas de segurança**: apenas SELECT único; bloqueio de DML/DDL/multi-statement.  
- **UI em Streamlit**: caching de resultados, gráficos dinâmicos, toggle LLM on/off, sem exposição de segredos.  

---

## Resultados de aceitação (exemplos)

| Pergunta | Resposta esperada |
|----------|-------------------|
| Chamados em 28/11/2024 | **0 chamados** |
| Subtipo mais comum em Iluminação Pública | **Reparo de Luminária (~68.253)** |
| Top 3 bairros em reparo de buraco (2023) | **Campo Grande (9.836), Bangu (3.280), Santa Cruz (2.659)** |
| Unidade líder em fiscalização irregular | **GM-RIO — Guarda Municipal (~154.519)** |
| Saudação | Resposta simpática (LLM ou fallback) |
| Sugestões de brincadeiras para cachorro | Lista de atividades (LLM ou fallback) |

---

## Limites e Guardas

- **Dry-run obrigatório** antes de qualquer execução.  
- **Máximo de bytes processados** (`BQ_MAX_BYTES_BILLED`).  
- **Timeout configurável** (`BQ_QUERY_TIMEOUT`).  
- **Somente SELECT** de sentença única (sem DML, DDL, `EXECUTE IMMEDIATE`, multi-statement).  
- **Fallback determinístico** se LLM não estiver ativo.  

---

## Troubleshooting

- **Erro de autenticação GCP** → verifique `gcloud auth application-default login` e `PROJECT_ID`.  
- **Quota ou custo excedido** → ajuste `BQ_MAX_BYTES_BILLED` ou refine filtros de data.  
- **Chave LLM ausente** → o agente funciona em modo determinístico sem LLM.  
- **JOIN de bairros vazio** → confira se `id_bairro` precisa de `CAST` para `STRING`.  
- **Streamlit não abre** → use `make ui` e no Cloud Shell ative Web Preview na porta 8501.  

---

## Critérios atendidos

- [x] **SQL eficiente** (sem `SELECT *`, filtragem defensiva, partições)  
- [x] **Dry-run** para prever custo/latência  
- [x] **Fallback robusto** se LLM não estiver ativo  
- [x] **Testes automatizados** (unitários, integração e aceitação)  
- [x] **Qualidade de código** (lint + pre-commit)  
- [x] **Documentação completa**  
- [x] **Guardas de segurança** (custo, timeout, bloqueio de DML/DDL)  
- [x] **UI em Streamlit** para interação prática  

---

## Próximos passos

- Clarificação interativa quando período não for especificado.  
- Validações semânticas adicionais antes de dry-run.  
- Observabilidade (latência, bytes processados) em dashboard.  
- Cache para FAQs/resultados recorrentes.  
- CI/CD com execução automática de lint e testes.  

---

## Observação sobre `data_particao` vs `data_inicio`

A tabela `datario.adm_central_atendimento_1746.chamado` é particionada por  
`data_particao = TRUNC(DATE(data_inicio))`.  
Na prática, identificamos casos onde `DATE(data_inicio)` contém registros  
que **não aparecem na partição correspondente**.  

Exemplo real:  
- `DATE(data_inicio) = '2024-11-28'` → **2.885 registros**  
- `data_particao = '2024-11-28'` → **0 registros**

Esse descompasso pode ocorrer por atrasos de ingestão, backfill ou correções posteriores.

### Decisão do agente
- Sempre usar `data_particao` por padrão (requisito do desafio), pois garante **pruning de partição** e custo baixo.  
- Como efeito colateral, contagens pontuais em certos dias podem retornar 0 mesmo havendo registros em `data_inicio`.

### Possíveis soluções futuras
- Adicionar um “modo precisão” opcional que consulta `DATE(data_inicio)` (quando o usuário pedir dia específico), aceitando custo maior.  
- Avisar o usuário quando o resultado for 0, sugerindo consulta mais precisa.