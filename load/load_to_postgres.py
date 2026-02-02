# load_to_postgres.py
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('postgresql://user:pass@localhost:5432/mydb')


def parquet_to_postgres(table_name):
    df = pd.read_parquet(f"{table_name}.parquet")

    # Преобразование типов под PostgreSQL
    if 'is_active' in df.columns:
        df['is_active'] = df['is_active'].map({'Y': True, 'N': False})

    df.to_sql(table_name, engine, if_exists='replace', index=False)
    print(f"✅ {table_name}.parquet → PostgreSQL")


if __name__ == "__main__":
    parquet_to_postgres("customers")
    parquet_to_postgres("orders")