#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Acceptance test do agente GenAI (Prefeitura do Rio).

Objetivo
--------
Executa as 6 perguntas do desafio e exibe, para cada uma:
- intent roteado
- SQL gerado (quando houver)
- status de validação (dry-run)
- bytes estimados do dry-run (quando fornecido pelo grafo)
- shape + prévia dos dados retornados
- resposta final sintetizada em PT-BR

Uso
---
# Execução padrão (todas as perguntas, com prévia de DF e SQL):
python scripts/acceptance_test.py

# Selecionar perguntas específicas (por índice 1..6):
python scripts/acceptance_test.py --only 1,3,5

# Ajustar preview (linhas e largura de truncagem):
python scripts/acceptance_test.py --max-rows 8 --width 160

# Suprimir SQL ou preview de DF:
python scripts/acceptance_test.py --no-sql --no-preview

# Saída mais silenciosa (mostra só índice, intent e answer):
python scripts/acceptance_test.py --quiet

# Forçar LLM on/off (sobrescreve env LLM_USE_FOR_SYNTH só nesta execução):
python scripts/acceptance_test.py --llm on
python scripts/acceptance_test.py --llm off
python scripts/acceptance_test.py --llm auto   # (default)

# Exportar um resumo em JSON:
python scripts/acceptance_test.py --json out/acceptance_results.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from textwrap import shorten
from typing import Any, Dict, Iterable, List, Optional

# Garante import de 'src' quando rodar fora do pytest
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.agent.graph import run_debug, GRAPH_VERSION  # noqa: E402


# Perguntas do desafio

QUESTIONS: List[str] = [
    # 1
    "Quantos chamados foram abertos no dia 28/11/2024?",
    # 2
    "Qual o subtipo de chamado mais comum relacionado a Iluminação Pública?",
    # 3
    "Quais os 3 bairros que mais tiveram chamados abertos sobre reparo de buraco em 2023?",
    # 4
    "Qual o nome da unidade organizacional que mais atendeu chamados de Fiscalização de estacionamento irregular?",
    # 5
    "Olá, tudo bem?",
    # 6
    "Me dê sugestões de brincadeiras para fazer com meu cachorro!",
]


# Utilidades


def _shape(df: Any) -> Optional[str]:
    """Retorna shape do DataFrame (se disponível) como 'linhas x colunas'."""
    try:
        shp = getattr(df, "shape", None)
        if not shp:
            return None
        return f"{shp[0]} x {shp[1]}"
    except Exception:
        return None


def _preview_df(df: Any, n: int = 5, width: int = 120) -> str:
    """Gera uma prévia amigável do DF, com fallback seguro."""
    try:
        import pandas as pd  # type: ignore

        if isinstance(df, pd.DataFrame):
            # Tenta usar tabulate se estiver disponível para tabela mais legível
            try:
                from tabulate import tabulate  # type: ignore

                head = df.head(n).copy()
                # Evita explosão de largura por colunas longas
                for c in head.columns:
                    head[c] = (
                        head[c]
                        .astype(str)
                        .map(lambda x: shorten(x, width=60, placeholder="…"))
                    )
                return tabulate(
                    head, headers="keys", tablefmt="github", showindex=False
                )
            except Exception:
                return shorten(str(df.head(n)), width=width, placeholder=" …")
        # Não é DF, tenta string curta
        return shorten(str(df), width=width, placeholder=" …")
    except Exception:
        return "n/a"


def _bool_from_env(name: str, default: bool) -> bool:
    """Interpreta variáveis '0/1/true/false' como booleanas."""
    val = os.getenv(name)
    if val is None:
        return default
    return str(val).strip().lower() in {"1", "true", "yes", "y", "on"}


def _select_questions(indices_csv: Optional[str], total: int) -> Iterable[int]:
    """Converte '1,3,6' em índices zero-based válidos."""
    if not indices_csv:
        return range(total)
    out: List[int] = []
    for tok in indices_csv.split(","):
        tok = tok.strip()
        if not tok:
            continue
        try:
            i = int(tok)
            if 1 <= i <= total:
                out.append(i - 1)
        except ValueError:
            pass
    return out or range(total)


# Execução


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="acceptance_test",
        description="Executa as perguntas do desafio e imprime o estado final de cada uma.",
    )
    p.add_argument(
        "--only",
        help="Lista de índices (1..6), ex.: '1,3,5'. Default: todas.",
        default=None,
    )
    p.add_argument(
        "--max-rows",
        type=int,
        default=5,
        help="Linhas no preview do DataFrame. Default: 5.",
    )
    p.add_argument(
        "--width",
        type=int,
        default=120,
        help="Largura para truncagem de textos. Default: 120.",
    )
    p.add_argument("--no-sql", action="store_true", help="Não exibe o SQL gerado.")
    p.add_argument(
        "--no-preview", action="store_true", help="Não exibe prévia do DataFrame."
    )
    p.add_argument(
        "--llm",
        choices=("auto", "on", "off"),
        default="auto",
        help="Força LLM on/off sobrescrevendo LLM_USE_FOR_SYNTH nesta execução.",
    )
    p.add_argument(
        "--json",
        dest="json_out",
        help="Caminho para salvar um resumo em JSON.",
        default=None,
    )
    p.add_argument(
        "--quiet", action="store_true", help="Saída compacta (índice, intent, answer)."
    )
    return p.parse_args(argv)


def main() -> int:
    args = parse_args()

    # LLM: modo 'on'/'off' sobrescreve LLM_USE_FOR_SYNTH apenas nesta execução
    prev_llm_env = os.getenv("LLM_USE_FOR_SYNTH")
    if args.llm == "on":
        os.environ["LLM_USE_FOR_SYNTH"] = "1"
    elif args.llm == "off":
        os.environ["LLM_USE_FOR_SYNTH"] = "0"

    # Cálculo do status efetivo do LLM
    llm_active = (os.getenv("LLM_USE_FOR_SYNTH") == "1") and bool(
        os.getenv("OPENAI_API_KEY")
    )

    print(f"\n== Acceptance Test | Graph v{GRAPH_VERSION} ==")
    print(f"PROJECT_ID={os.getenv('PROJECT_ID', 'n/a')}")
    print(
        f"LLM ativo: {llm_active} (LLM_USE_FOR_SYNTH={os.getenv('LLM_USE_FOR_SYNTH','n/a')}, "
        f"OPENAI_API_KEY set={bool(os.getenv('OPENAI_API_KEY'))})\n"
    )

    results: List[Dict[str, Any]] = []
    failures = 0

    indices = list(_select_questions(args.only, len(QUESTIONS)))
    try:
        for idx in indices:
            q = QUESTIONS[idx]
            header = f"[{idx+1}] Q: {q}"
            print("=" * 100)
            print(header)

            t0 = time.time()
            try:
                state: Dict[str, Any] = run_debug(q)
                elapsed_ms = (time.time() - t0) * 1000.0
            except Exception as e:
                failures += 1
                print(
                    f"- erro: falha inesperada ao executar a pergunta. detalhe={repr(e)}\n"
                )
                # Registra no JSON também
                results.append({"index": idx + 1, "question": q, "error": repr(e)})
                continue

            intent = state.get("intent")
            sql = state.get("sql")
            valid_ok = state.get("validation_ok")
            valid_err = state.get("validation_error")
            meta = state.get("meta", {}) or {}
            dry_bytes = meta.get("dry_run_bytes")
            graph_v = meta.get("graph_version")
            df = state.get("df")
            answer = state.get("answer", "")

            if args.quiet:
                print(
                    f"- intent: {intent} | answer: {shorten(answer, width=160, placeholder=' …')}\n"
                )
            else:
                # Impressão detalhada
                print(f"- intent: {intent}")
                print(f"- graph_version(meta): {graph_v}")
                print(f"- latency_ms: {elapsed_ms:.0f}")
                if dry_bytes is not None:
                    print(f"- dry_run_bytes: {dry_bytes:,}")

                if sql and not args.no_sql:
                    print(f"- sql: {sql}")

                if "validation_ok" in state:
                    print(f"- validation_ok: {valid_ok}")
                if valid_err:
                    print(f"- validation_error: {str(valid_err)[:240]}")

                if df is not None and not args.no_preview:
                    shp = _shape(df)
                    print(f"- df.shape: {shp or 'n/a'}")
                    print("- df.preview:")
                    print(_preview_df(df, n=args.max_rows, width=args.width))

                print(f"- answer: {answer}\n")

            # Guarda resumo para JSON
            results.append(
                {
                    "index": idx + 1,
                    "question": q,
                    "intent": intent,
                    "sql": sql,
                    "validation_ok": valid_ok,
                    "validation_error": valid_err,
                    "dry_run_bytes": dry_bytes,
                    "graph_version": graph_v,
                    "latency_ms": round(elapsed_ms, 0),
                    "df_shape": _shape(df),
                    "answer": answer,
                }
            )

    except KeyboardInterrupt:
        print("\nInterrompido pelo usuário (Ctrl+C).")
        # Restaura env do LLM e salva JSON parcial se solicitado
        if args.json_out and "results" in locals():
            out_path = Path(args.json_out)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            with out_path.open("w", encoding="utf-8") as f:
                json.dump(results, f, ensure_ascii=False, indent=2)
            print(f"Resumo parcial salvo em: {out_path}")
        # 130 é um código comum para SIGINT
        return 130

    # Exporta JSON (opcional)
    if args.json_out:
        out_path = Path(args.json_out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with out_path.open("w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        print(f"Resumo salvo em: {out_path}")

    # Restaura env do LLM (se alterado)
    if prev_llm_env is None:
        os.environ.pop("LLM_USE_FOR_SYNTH", None)
    else:
        os.environ["LLM_USE_FOR_SYNTH"] = prev_llm_env

    # Código de saída: 0 se tudo ok, 1 se houve falhas inesperadas
    if failures:
        print(f"\nConcluído com {failures} erro(s) inesperado(s).")
        return 1

    print("\nConcluído com sucesso")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
