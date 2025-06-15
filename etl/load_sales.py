import pandas as pd
import psycopg2
import yaml

def load_config(path="config.yaml"):
    print("[DEBUG] Loading config from:", path)
    with open(path) as file:
        config = yaml.safe_load(file)
    print("[DEBUG] Config loaded:", config)
    return config

def map_dtype(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return "INTEGER"
    elif pd.api.types.is_float_dtype(dtype):
        return "FLOAT"
    elif pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "TIMESTAMP"
    else:
        return "TEXT"

def create_table_from_df(df, table_name, cur):
    print(f"[DEBUG] Creating table '{table_name}' if not exists...")
    columns = []
    for col in df.columns:
        sql_type = map_dtype(df[col].dtype)
        print(f"[DEBUG] Mapping column '{col}' to type '{sql_type}'")
        columns.append(f"{col} {sql_type}")
    column_str = ", ".join(columns)
    sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({column_str});"
    print("[DEBUG] Executing SQL:", sql)
    cur.execute(sql)
    print("[DEBUG] Table creation finished.")

def insert_data(df, table_name, cur):
    print(f"[DEBUG] Inserting data into '{table_name}'...")
    cols = list(df.columns)
    col_names = ", ".join(cols)
    placeholders = ", ".join(["%s"] * len(cols))

    for idx, (_, row) in enumerate(df.iterrows(), start=1):
        values = tuple(row[col] for col in cols)
        print(f"[DEBUG] Inserting row {idx}: {values}")
        cur.execute(
            f"INSERT INTO {table_name} ({col_names}) VALUES ({placeholders})",
            values
        )
    print(f"[DEBUG] Inserted {len(df)} rows successfully.")

def main():
    print("[DEBUG] Starting ETL process...")
    config = load_config()
    
    print("[DEBUG] Reading CSV file: data/tbl_sales.csv")
    df = pd.read_csv("data/tbl_sales.csv")
    print(f"[DEBUG] Read {len(df)} rows with columns: {list(df.columns)}")

    print("[DEBUG] Connecting to PostgreSQL...")
    conn = psycopg2.connect(
        host=config["host"],
        database=config["database"],
        user=config["user"],
        password=config["password"],
        port=config["port"]
    )
    print("[DEBUG] Connection established.")

    cur = conn.cursor()
    table_name = "sales"

    create_table_from_df(df, table_name, cur)
    insert_data(df, table_name, cur)

    conn.commit()
    print("[DEBUG] Transaction committed.")
    cur.close()
    conn.close()
    print("[DEBUG] PostgreSQL connection closed.")
    print("✅ ETL success!")

if __name__ == "__main__":
    main()
