import os

from dotenv import load_dotenv
from jinja2 import Environment, FileSystemLoader
from sqlalchemy import text

from etl.db.postgresql_client import PostgreSqlClient


VALID_LOAD_METHODS = {"insert", "overwrite", "upsert"}
ANALYSIS_LOAD_METHODS = {"query", "insert", "overwrite"}


def _required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def _build_source_client() -> PostgreSqlClient:
    return PostgreSqlClient(
        server_name=_required_env("SERVER_NAME"),
        database_name=_required_env("DATABASE_NAME"),
        username=_required_env("DB_USERNAME"),
        password=_required_env("DB_PASSWORD"),
        port=int(os.environ.get("PORT", "5432")),
    )


def _build_target_client() -> PostgreSqlClient:
    return PostgreSqlClient(
        server_name=_required_env("TARGET_SERVER_NAME"),
        database_name=_required_env("TARGET_DATABASE_NAME"),
        username=_required_env("TARGET_DB_USERNAME"),
        password=_required_env("TARGET_DB_PASSWORD"),
        port=int(os.environ.get("TARGET_PORT", "5432")),
    )


def _load_with_method(
    target_client: PostgreSqlClient,
    method: str,
    data: list[dict],
    target_table,
    target_metadata,
) -> None:
    if method == "insert":
        target_client.insert(data=data, table=target_table, metadata=target_metadata)
        return

    if method == "overwrite":
        target_client.overwrite(data=data, table=target_table, metadata=target_metadata)
        return

    # default validated method is upsert
    target_client.upsert(data=data, table=target_table, metadata=target_metadata)


def _run_staging_templates(
    source_client: PostgreSqlClient,
    target_client: PostgreSqlClient,
) -> None:
    environment = Environment(loader=FileSystemLoader("etl/sql/staging"))

    for sql_path in environment.list_templates():
        sql_template = environment.get_template(sql_path)
        template_module = sql_template.make_module()
        template_config = getattr(template_module, "config", {})
        source_table_name = template_config.get("source_table_name")
        load_method = str(template_config.get("load_method", "upsert")).lower()

        if not source_table_name:
            raise ValueError(f"Template {sql_path} is missing config['source_table_name']")

        if load_method not in VALID_LOAD_METHODS:
            raise ValueError(
                f"Template {sql_path} has invalid load_method '{load_method}'. "
                f"Use one of {sorted(VALID_LOAD_METHODS)}"
            )

        source_table, _ = source_client.reflect_table(source_table_name)
        target_table, target_metadata = target_client.create_table_like(source_table)

        rendered_sql = sql_template.render()
        data = source_client.execute_sql(rendered_sql)

        if not data:
            print(f"No rows returned for {source_table_name}; skipping load.")
            continue

        _load_with_method(
            target_client=target_client,
            method=load_method,
            data=data,
            target_table=target_table,
            target_metadata=target_metadata,
        )

        print(
            f"Loaded {len(data)} rows into target table {target_table.name} "
            f"from {sql_path} using {load_method}"
        )


def _run_analysis_templates(
    source_client: PostgreSqlClient,
    target_client: PostgreSqlClient,
) -> None:
    environment = Environment(loader=FileSystemLoader("etl/sql/analysis"))

    for sql_path in environment.list_templates():
        sql_template = environment.get_template(sql_path)
        template_module = sql_template.make_module()
        template_config = getattr(template_module, "config", {})
        load_method = str(template_config.get("load_method", "query")).lower()
        target_schema = str(template_config.get("target_schema", "public"))
        target_table_name = template_config.get("target_table_name")

        if load_method not in ANALYSIS_LOAD_METHODS:
            raise ValueError(
                f"Analysis template {sql_path} has invalid load_method '{load_method}'. "
                f"Use one of {sorted(ANALYSIS_LOAD_METHODS)}"
            )

        rendered_sql = sql_template.render()

        if load_method == "query":
            rows = source_client.execute_sql(rendered_sql)
            print(f"Analysis template {sql_path} returned {len(rows)} rows")
            continue

        if not target_table_name:
            raise ValueError(
                f"Analysis template {sql_path} requires config['target_table_name'] "
                f"when load_method is '{load_method}'"
            )

        qualified_target = f"{target_schema}.{target_table_name}"

        if load_method == "overwrite":
            statement = (
                f"DROP TABLE IF EXISTS {qualified_target}; "
                f"CREATE TABLE {qualified_target} AS {rendered_sql}"
            )
        else:
            statement = (
                f"INSERT INTO {qualified_target} "
                f"{rendered_sql}"
            )

        with target_client.engine.begin() as connection:
            connection.execute(text(statement))

        print(
            f"Analysis template {sql_path} loaded into {qualified_target} "
            f"using {load_method}"
        )


if __name__ == "__main__":
    load_dotenv()

    source_client = _build_source_client()
    target_client = _build_target_client()

    # Analysis templates run against gold-layer data, so source and target context are both target_client.
    analysis_source_client = target_client
    analysis_target_client = target_client

    _run_staging_templates(source_client=source_client, target_client=target_client)
    _run_analysis_templates(
        source_client=analysis_source_client,
        target_client=analysis_target_client,
    )
