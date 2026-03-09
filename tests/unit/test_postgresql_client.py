from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from etl.db.postgresql_client import PostgreSqlClient


@patch("etl.db.postgresql_client.create_engine")
@patch("etl.db.postgresql_client.URL.create")
def test_init_builds_pg8000_engine(mock_url_create, mock_create_engine):
    mock_url_create.return_value = "mock-url"

    client = PostgreSqlClient(
        server_name="db-host",
        database_name="bronze_layer",
        username="postgres",
        password="secret",
        port=5433,
    )

    mock_url_create.assert_called_once_with(
        drivername="postgresql+pg8000",
        username="postgres",
        password="secret",
        host="db-host",
        port=5433,
        database="bronze_layer",
    )
    mock_create_engine.assert_called_once_with("mock-url")
    assert client.engine == mock_create_engine.return_value


@patch("etl.db.postgresql_client.text", side_effect=lambda sql: f"TEXT::{sql}")
def test_execute_sql_returns_list_of_dicts(mock_text):
    client = PostgreSqlClient.__new__(PostgreSqlClient)

    connection = MagicMock()
    connection.execute.return_value.fetchall.return_value = [
        SimpleNamespace(_mapping={"location_id": 1}),
        SimpleNamespace(_mapping={"location_id": 2}),
    ]

    context_manager = MagicMock()
    context_manager.__enter__.return_value = connection

    client.engine = MagicMock()
    client.engine.connect.return_value = context_manager

    rows = client.execute_sql("SELECT location_id FROM locations")

    mock_text.assert_called_once_with("SELECT location_id FROM locations")
    connection.execute.assert_called_once_with("TEXT::SELECT location_id FROM locations")
    assert rows == [{"location_id": 1}, {"location_id": 2}]


def test_drop_table_executes_expected_sql():
    client = PostgreSqlClient.__new__(PostgreSqlClient)
    client.engine = MagicMock()

    client.drop_table("locations")

    client.engine.execute.assert_called_once_with("drop table if exists locations;")


def test_overwrite_drops_then_inserts():
    client = PostgreSqlClient.__new__(PostgreSqlClient)
    client.drop_table = MagicMock()
    client.insert = MagicMock()
    table = SimpleNamespace(name="locations")
    metadata = object()
    data = [{"location_id": 1}]

    client.overwrite(data=data, table=table, metadata=metadata)

    client.drop_table.assert_called_once_with("locations")
    client.insert.assert_called_once_with(data=data, table=table, metadata=metadata)


@patch("etl.db.postgresql_client.postgresql.insert")
def test_upsert_processes_data_in_chunks(mock_insert):
    class FakeInsertStatement:
        def __init__(self):
            self.excluded = [SimpleNamespace(key="location_id"), SimpleNamespace(key="name")]
            self.chunk = None

        def values(self, chunk):
            self.chunk = chunk
            return self

        def on_conflict_do_update(self, index_elements, set_):
            return {
                "chunk_size": len(self.chunk),
                "index_elements": index_elements,
                "set_": set_,
            }

    mock_insert.side_effect = lambda _table: FakeInsertStatement()

    client = PostgreSqlClient.__new__(PostgreSqlClient)
    client.engine = MagicMock()

    metadata = MagicMock()
    table = MagicMock()
    table.primary_key.columns.values.return_value = [SimpleNamespace(name="location_id")]

    data = [{"location_id": i, "name": f"loc-{i}"} for i in range(5)]

    client.upsert(data=data, table=table, metadata=metadata, chunksize=2)

    metadata.create_all.assert_called_once_with(client.engine)
    assert client.engine.execute.call_count == 3

    first_upsert = client.engine.execute.call_args_list[0].args[0]
    assert first_upsert["chunk_size"] == 2
    assert first_upsert["index_elements"] == ["location_id"]
    assert list(first_upsert["set_"].keys()) == ["name"]