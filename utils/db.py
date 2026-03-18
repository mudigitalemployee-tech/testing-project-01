"""Database connection and SQL execution utilities."""
import os
import yaml
import psycopg2
import psycopg2.extras


def load_config(config_path: str = None) -> dict:
    if config_path is None:
        config_path = os.path.join(os.path.dirname(__file__), "..", "config", "config.yaml")
    with open(config_path) as f:
        return yaml.safe_load(f)


def get_connection(config: dict = None):
    if config is None:
        config = load_config()
    db = config.get("database", {})
    return psycopg2.connect(
        host=os.getenv("DB_HOST", db.get("host", "localhost")),
        port=int(os.getenv("DB_PORT", db.get("port", 5432))),
        dbname=os.getenv("DB_NAME", db.get("name", "sales_dw")),
        user=os.getenv("DB_USER", db.get("user", "airflow")),
        password=os.getenv("DB_PASSWORD", db.get("password", "airflow")),
    )


def execute_sql(conn, sql: str, params=None, fetch: bool = False):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params)
        if fetch:
            return cur.fetchall()
    conn.commit()
    return None


def execute_sql_file(conn, filepath: str):
    with open(filepath) as f:
        sql = f.read()
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
