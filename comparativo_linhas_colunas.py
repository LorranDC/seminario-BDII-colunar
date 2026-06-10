import argparse
import os
import sqlite3
import time

import duckdb


SQLITE_FILE = "comparativo_linhas.sqlite"
PARQUET_FILE = "comparativo_colunas.parquet"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Demonstra pontos fortes e fracos de armazenamento orientado a linhas e colunas."
    )
    parser.add_argument("--force-generate", action="store_true", help="recria os dados de amostra")
    parser.add_argument("--rows", type=int, default=10_000_000, help="quantidade de registros")
    parser.add_argument("--point-lookups", type=int, default=10_000, help="buscas por chave no teste OLTP")
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=60.0,
        help="tempo maximo para considerar que o workload ruim travou de forma controlada",
    )
    return parser.parse_args()


def timed(label, callback):
    start = time.perf_counter()
    result = callback()
    elapsed = time.perf_counter() - start
    print(f"{label}: {elapsed:.3f}s")
    return result, elapsed


def generate_data(rows):
    print(f"\n[GERACAO] Criando {rows:,} registros em SQLite e Parquet...")
    print("SQLite representa o caso orientado a linhas com chave primaria.")
    print("Parquet representa o caso colunar otimizado para analise.")

    if os.path.exists(SQLITE_FILE):
        os.remove(SQLITE_FILE)
    if os.path.exists(PARQUET_FILE):
        os.remove(PARQUET_FILE)

    conn = duckdb.connect()
    conn.execute("PRAGMA threads=4")

    # A ordem fisica fica embaralhada para reduzir a chance de o Parquet podar
    # row groups em buscas pontuais por id.
    print("[GERACAO] Criando dados base em ordem fisica embaralhada.")
    print("          Isso torna buscas pontuais no Parquet menos favoraveis.")
    conn.execute(f"""
        CREATE TABLE base AS
        SELECT
            i::BIGINT AS id,
            'Cliente_' || i AS nome,
            'cliente_' || i || '@empresa.com' AS email,
            'Categoria_' || (i % 20) AS categoria,
            (i % 1000)::INTEGER AS loja_id,
            ((i * 17) % 100000)::DECIMAL(12,2) / 100 AS valor,
            DATE '2026-01-01' + ((i % 365)::INTEGER) AS data_evento,
            'payload_' || repeat('x', 120) AS detalhe_1,
            'payload_' || repeat('y', 120) AS detalhe_2,
            'payload_' || repeat('z', 120) AS detalhe_3
        FROM range({rows}) AS t(i)
        ORDER BY hash(i)
    """)

    conn.execute(f"COPY base TO '{PARQUET_FILE}' (FORMAT PARQUET, ROW_GROUP_SIZE 122880)")
    print(f"[GERACAO] Parquet gerado: {PARQUET_FILE}")

    sqlite_conn = sqlite3.connect(SQLITE_FILE)
    sqlite_conn.execute("PRAGMA journal_mode=OFF")
    sqlite_conn.execute("PRAGMA synchronous=OFF")
    sqlite_conn.execute("PRAGMA temp_store=MEMORY")
    sqlite_conn.execute("""
        CREATE TABLE eventos (
            id INTEGER PRIMARY KEY,
            nome TEXT,
            email TEXT,
            categoria TEXT,
            loja_id INTEGER,
            valor REAL,
            data_evento TEXT,
            detalhe_1 TEXT,
            detalhe_2 TEXT,
            detalhe_3 TEXT
        )
    """)

    print("[GERACAO] Carregando SQLite em lotes...")
    batch_size = 50_000
    for offset in range(0, rows, batch_size):
        batch = conn.execute(f"""
            SELECT
                id,
                nome,
                email,
                categoria,
                loja_id,
                CAST(valor AS DOUBLE),
                CAST(data_evento AS VARCHAR),
                detalhe_1,
                detalhe_2,
                detalhe_3
            FROM base
            LIMIT {batch_size}
            OFFSET {offset}
        """).fetchall()
        sqlite_conn.executemany(
            "INSERT INTO eventos VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            batch,
        )
        sqlite_conn.commit()

    sqlite_conn.execute("CREATE INDEX idx_eventos_categoria ON eventos(categoria)")
    sqlite_conn.commit()
    sqlite_conn.close()
    conn.close()
    print(f"[GERACAO] SQLite gerado com PRIMARY KEY em id: {SQLITE_FILE}")


def ensure_data(args):
    exists = os.path.exists(SQLITE_FILE) and os.path.exists(PARQUET_FILE)
    if exists and not args.force_generate:
        print("[GERACAO] Amostra existente encontrada. Pulando geracao.")
        print("          Isto preserva a amostra ja criada e evita gastar disco novamente.")
        print(f"          Use --force-generate para recriar {SQLITE_FILE} e {PARQUET_FILE}.")
        return

    generate_data(args.rows)


def deterministic_ids(rows, count):
    return [((i * 104729) + 17) % rows for i in range(count)]


def run_oltp_row_good_column_bad(args):
    print("\n" + "=" * 72)
    print("TESTE A: OLTP / busca pontual por chave")
    print("Esperado: SQLite orientado a linhas e com indice vence; Parquet sofre.")
    print("=" * 72)
    print("Workload: varias consultas do tipo SELECT * WHERE id = ?")
    print("Por que favorece linhas:")
    print("- SQLite encontra a linha pela PRIMARY KEY e retorna o registro completo.")
    print("- Parquet nao tem indice transacional por id; cada busca pode varrer metadados/blocos.")
    print(f"Quantidade de buscas pontuais: {args.point_lookups:,}")
    print(f"Timeout do lado ruim: {args.timeout_seconds:.1f}s")

    ids = deterministic_ids(args.rows, args.point_lookups)

    sqlite_conn = sqlite3.connect(SQLITE_FILE)

    def sqlite_lookup():
        cursor = sqlite_conn.cursor()
        for row_id in ids:
            cursor.execute("SELECT * FROM eventos WHERE id = ?", (row_id,))
            cursor.fetchone()

    _, sqlite_time = timed("SQLite linha/index - busca por chave", sqlite_lookup)
    sqlite_conn.close()

    parquet_conn = duckdb.connect()
    parquet_conn.execute("PRAGMA memory_limit='1GB'")

    print("Parquet colunar - repetindo buscas pontuais sem indice...")
    start = time.perf_counter()
    completed = 0
    try:
        for row_id in ids:
            parquet_conn.execute(
                f"SELECT * FROM '{PARQUET_FILE}' WHERE id = ?",
                [row_id],
            ).fetchone()
            completed += 1
            elapsed = time.perf_counter() - start
            if elapsed > args.timeout_seconds:
                raise TimeoutError(
                    f"interrompido apos {elapsed:.3f}s e {completed}/{len(ids)} buscas"
                )

        parquet_time = time.perf_counter() - start
        print(f"Parquet colunar - busca pontual: {parquet_time:.3f}s")
        print(f"Resultado: SQLite foi {parquet_time / sqlite_time:.1f}x mais rapido.")
        print("Conclusao: consulta pontual por registro completo e um ponto forte de armazenamento em linhas.")
    except Exception as exc:
        print(f"Parquet colunar - TRAVOU CONTROLADO: {exc}")
        print("Resultado: workload de busca pontual favorece linha + indice.")
        print("Conclusao: colunar nao e a melhor escolha para muitas buscas OLTP isoladas.")
    finally:
        parquet_conn.close()


def run_olap_column_good_row_bad(args):
    print("\n" + "=" * 72)
    print("TESTE B: OLAP / agregacao lendo poucas colunas")
    print("Esperado: Parquet colunar vence; SQLite linha-a-linha e interrompido.")
    print("=" * 72)
    print("Workload: agregacao por categoria lendo somente categoria, valor e data_evento.")
    print("Por que favorece colunas:")
    print("- Parquet pode ler apenas as colunas necessarias.")
    print("- SQLite percorre registros linha-a-linha mesmo usando poucas colunas.")
    print(f"Timeout do lado ruim: {args.timeout_seconds:.1f}s")

    query_parquet = f"""
        SELECT categoria, SUM(valor) AS total, AVG(valor) AS media
        FROM '{PARQUET_FILE}'
        WHERE data_evento >= DATE '2026-04-01'
        GROUP BY categoria
        ORDER BY total DESC
    """

    parquet_conn = duckdb.connect()
    print("Consulta executada no Parquet:")
    print("  SELECT categoria, SUM(valor), AVG(valor)")
    print("  WHERE data_evento >= '2026-04-01'")
    print("  GROUP BY categoria")
    _, parquet_time = timed(
        "Parquet colunar - agregacao em poucas colunas",
        lambda: parquet_conn.execute(query_parquet).fetchall(),
    )
    parquet_conn.close()

    sqlite_conn = sqlite3.connect(SQLITE_FILE)
    start = time.perf_counter()

    def interrupt_if_too_slow():
        if time.perf_counter() - start > args.timeout_seconds:
            return 1
        return 0

    sqlite_conn.set_progress_handler(interrupt_if_too_slow, 50_000)

    try:
        print("Executando a mesma agregacao no SQLite linha-a-linha...")
        sqlite_conn.execute("""
            SELECT categoria, SUM(valor) AS total, AVG(valor) AS media
            FROM eventos
            WHERE data_evento >= '2026-04-01'
            GROUP BY categoria
            ORDER BY total DESC
        """).fetchall()
        sqlite_time = time.perf_counter() - start
        print(f"SQLite linha - agregacao analitica: {sqlite_time:.3f}s")
        print(f"Resultado: Parquet foi {sqlite_time / parquet_time:.1f}x mais rapido.")
        print("Conclusao: mesmo quando SQLite termina, o workload analitico favorece colunar.")
    except sqlite3.OperationalError as exc:
        elapsed = time.perf_counter() - start
        print(f"SQLite linha - TRAVOU CONTROLADO apos {elapsed:.3f}s: {exc}")
        print("Resultado: workload analitico lendo poucas colunas favorece colunar.")
        print("Conclusao: agregacoes de poucas colunas sao o ponto forte do armazenamento colunar.")
    finally:
        sqlite_conn.close()


def main():
    args = parse_args()
    print("=" * 72)
    print("COMPARATIVO PRATICO: LINHAS VS COLUNAS")
    print("=" * 72)
    print(f"Registros esperados na amostra: {args.rows:,}")
    print(f"Arquivos usados: {SQLITE_FILE} e {PARQUET_FILE}")
    print("Observacao: sem --force-generate, arquivos existentes sao reutilizados.")
    ensure_data(args)
    run_oltp_row_good_column_bad(args)
    run_olap_column_good_row_bad(args)


if __name__ == "__main__":
    main()
