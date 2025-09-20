# Makefile — atalhos para setup, lint, testes e acceptance
# Uso rápido:
#   make setup          # cria venv, instala deps e pre-commit
#   make lint           # ruff check
#   make fix            # ruff --fix
#   make test           # pytest completo
#   make smoke          # smoke_test.py (BigQuery smoke)
#   make accept         # scripts/acceptance_test.py
#   make accept-quiet   # acceptance em modo silencioso
#   make accept-llm-off # acceptance com LLM desativado
#   make ask Q="sua pergunta"  # roda o agente direto
#   make clean          # limpa caches
#   make env            # mostra variáveis de ambiente relevantes
#   make ui             # inicia a interface Streamlit (porta 8501)
#   make ui-dev         # inicia a interface Streamlit com autoreload (porta 8501)
#   make ui-reset       # encerra processos que estão usando a porta (default 8501)
#   make ui-restart     # ui-reset + ui

SHELL := /bin/bash
PY := python
PIP := pip
VENV := .venv
ACTIVATE := source $(VENV)/bin/activate

# Porta do Streamlit (pode sobrescrever: make ui PORT=8502)
PORT ?= 8501

# Detecta automaticamente onde está o app do Streamlit:
# - prioriza ui/streamlit_app.py
# - fallback para streamlit_app.py na raiz
APP := $(firstword $(wildcard ui/streamlit_app.py streamlit_app.py))

.PHONY: help setup venv install precommit-install lint fix test smoke accept accept-quiet accept-llm-off ask clean env ui ui-dev ui-reset ui-restart

help:
	@echo "Targets disponíveis:"
	@echo "  setup            - cria venv, instala deps e pre-commit"
	@echo "  lint             - ruff check em src, tests e scripts"
	@echo "  fix              - ruff check --fix"
	@echo "  test             - roda pytest completo (verbose, falha rápida)"
	@echo "  smoke            - BigQuery smoke test (smoke_test.py)"
	@echo "  accept           - acceptance_test.py (padrão do desafio)"
	@echo "  accept-quiet     - acceptance em modo silencioso"
	@echo "  accept-llm-off   - acceptance com LLM desativado"
	@echo "  ask Q='...'      - executa uma pergunta diretamente no agente"
	@echo "  clean            - remove caches de py/pytest/ruff"
	@echo "  env              - exibe variáveis de ambiente relevantes"
	@echo "  ui               - inicia a interface Streamlit (porta $(PORT))"
	@echo "  ui-dev           - inicia a interface Streamlit com autoreload (porta $(PORT))"
	@echo "  ui-reset         - encerra processos na porta $(PORT)"
	@echo "  ui-restart       - ui-reset + ui"

setup: venv install precommit-install

venv:
	@[ -d $(VENV) ] || python3 -m venv $(VENV)

install:
	@$(ACTIVATE) && $(PIP) install -U pip
	@$(ACTIVATE) && $(PIP) install -r requirements.txt

precommit-install:
	@$(ACTIVATE) && pre-commit install || true

lint:
	@$(ACTIVATE) && ruff check src tests scripts

fix:
	@$(ACTIVATE) && ruff check --fix src tests scripts

test:
	@$(ACTIVATE) && pytest -vv --maxfail=1

smoke:
	@$(ACTIVATE) && python smoke_test.py

accept:
	@$(ACTIVATE) && python scripts/acceptance_test.py

accept-quiet:
	@$(ACTIVATE) && python scripts/acceptance_test.py --quiet

accept-llm-off:
	@$(ACTIVATE) && LLM_USE_FOR_SYNTH=0 python scripts/acceptance_test.py

ask:
	@if [ -z "$(Q)" ]; then echo "Use: make ask Q='sua pergunta'"; exit 1; fi
	@$(ACTIVATE) && python -c "from src.agent.graph import run; print(run('$(Q)'))"

clean:
	@find . -name '__pycache__' -type d -prune -exec rm -rf {} +; true
	@find . -name '*.py[co]' -delete; true
	@rm -rf .pytest_cache .ruff_cache || true

env:
	@set | egrep '^(PROJECT_ID|OPENAI|LLM_|BQ_)' || true

ui:
	@if [ -z "$(APP)" ]; then echo "Arquivo do Streamlit não encontrado. Crie ui/streamlit_app.py ou streamlit_app.py na raiz."; exit 1; fi
	@echo ">> Iniciando Streamlit em $$PWD/$(APP) na porta $(PORT) (Cloud Shell: Web Preview → $(PORT))"
	@$(ACTIVATE) && PYTHONPATH=. streamlit run $(APP) --server.port $(PORT) --server.address 0.0.0.0

ui-dev:
	@if [ -z "$(APP)" ]; then echo "Arquivo do Streamlit não encontrado. Crie ui/streamlit_app.py ou streamlit_app.py na raiz."; exit 1; fi
	@echo ">> Iniciando Streamlit (DEV - autoreload) em $$PWD/$(APP) na porta $(PORT) (Cloud Shell: Web Preview → $(PORT))"
	@$(ACTIVATE) && PYTHONPATH=. STREAMLIT_SERVER_RUN_ON_SAVE=true streamlit run $(APP) --server.port $(PORT) --server.address 0.0.0.0

ui-reset:
	@echo ">> Encerrando processos na porta $(PORT) (fuser/lsof)…"
	@fuser -k $(PORT)/tcp 2>/dev/null || true
	@pids="`lsof -ti:$(PORT) 2>/dev/null || true`"; \
	if [ -n "$$pids" ]; then echo "Matando PIDs: $$pids"; kill -9 $$pids || true; else echo "Nenhum processo escutando na porta $(PORT)."; fi
	@sleep 1
	@echo ">> Porta $(PORT) provavelmente liberada (verifique com: lsof -i:$(PORT))"

ui-restart: ui-reset ui