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
#   make env            # mostra variáveis relevantes

SHELL := /bin/bash
PY := python
PIP := pip
VENV := .venv
ACTIVATE := source $(VENV)/bin/activate

.PHONY: help setup venv install precommit-install lint fix test smoke accept accept-quiet accept-llm-off ask clean env

help:
	@echo "Targets disponíveis:"
	@echo "  setup            - cria venv, instala deps e pre-commit"
	@echo "  lint             - ruff check em src, tests e scripts"
	@echo "  fix              - ruff check --fix"
	@echo "  test             - roda pytest completo (verbose, falha rápida)"
	@echo "  smoke            - BigQuery smoke test (some_test.py)"
	@echo "  accept           - acceptance_test.py (padrão do desafio)"
	@echo "  accept-quiet     - acceptance em modo silencioso"
	@echo "  accept-llm-off   - acceptance com LLM desativado"
	@echo "  ask Q='...'      - executa uma pergunta diretamente no agente"
	@echo "  clean            - remove caches de py/pytest/ruff"
	@echo "  env              - exibe variáveis de ambiente relevantes"

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
	@$(ACTIVATE) && python some_test.py

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
