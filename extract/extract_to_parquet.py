# extract_to_parquet.py
import oracledb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Настройки подключения
dsn = oracledb.makedsn("localhost", 1521, service_name="XEPDB1")
conn = oracledb.connect(user="appuser", password="AppPass123_", dsn=dsn)

def oracle_to_parquet(table_name):
    df = pd.read_sql(f"SELECT * FROM {table_name}", conn)

    # Преобразование типов
    for col in df.columns:
        if df[col].dtype == 'object':
            # Обработка CLOB/NULL
            df[col] = df[col].astype(str).replace('None', None)
        elif 'date' in str(df[col].dtype).lower() or 'datetime' in str(df[col].dtype):
            df[col] = pd.to_datetime(df[col], errors='coerce')

    # Сохранение
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, f"{table_name}.parquet")
    print(f"✅ {table_name} → {table_name}.parquet")

if __name__ == "__main__":
    try:
        # Исправленный список таблиц — только те, что есть в схеме
        tables = ["customers", "accounts", "transactions", "audit_log"]
        for tbl in tables:
            oracle_to_parquet(tbl)
    finally:
        conn.close()