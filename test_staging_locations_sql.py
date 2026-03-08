import os
from pathlib import Path

from dotenv import load_dotenv
from jinja2 import Template
from sqlalchemy import text

from etl.db.postgresql_client import PostgreSqlClient


def main() -> None:
    load_dotenv()

    sql_template = Path("etl/sql/staging/locations.sql").read_text(encoding="utf-8")
    rendered_sql = Template(sql_template).render(
        bronze_schema="public",
        locations_table="locations",
    )

    client = PostgreSqlClient(
        server_name=os.environ.get("SERVER_NAME"),
        database_name=os.environ.get("DATABASE_NAME", "bronze_layer"),
        username=os.environ.get("DB_USERNAME"),
        password=os.environ.get("DB_PASSWORD"),
        port=int(os.environ.get("PORT", 5432)),
    )

    with client.engine.connect() as conn:
        row_count = conn.execute(text("SELECT COUNT(*) FROM public.locations")).scalar()
        sample_rows = conn.execute(text(rendered_sql)).fetchmany(3)

    print("Rendered SQL:\n")
    print(rendered_sql)
    print(f"\nRow count in public.locations: {row_count}")

    if sample_rows:
        first = dict(sample_rows[0]._mapping)
        print("\nSample row keys:")
        print(list(first.keys()))
        print("\nSample row:")
        print(first)
    else:
        print("\nNo rows returned from public.locations.")


if __name__ == "__main__":
    main()
