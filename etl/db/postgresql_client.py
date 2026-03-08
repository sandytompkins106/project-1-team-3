from sqlalchemy import create_engine, Table, MetaData, Column, text
from sqlalchemy.engine import URL
from sqlalchemy.dialects import postgresql
from typing import Optional

class PostgreSqlClient:
    """
    A client for querying postgresql database.
    """

    def __init__(
        self,
        server_name: str,
        database_name: str,
        username: str,
        password: str,
        port: int = 5432,
    ):
        self.host_name = server_name
        self.database_name = database_name
        self.username = username
        self.password = password
        self.port = port

        connection_url = URL.create(
            drivername="postgresql+pg8000",
            username=username,
            password=password,
            host=server_name,
            port=port,
            database=database_name,
        )

        self.engine = create_engine(connection_url)

    def select_all(self, table: Table) -> list[dict]:
        return [dict(row) for row in self.engine.execute(table.select()).all()]

    def create_table(self, metadata: MetaData) -> None:
        """
        Creates table provided in the metadata object
        """
        metadata.create_all(self.engine)

    def drop_table(self, table_name: str) -> None:
        self.engine.execute(f"drop table if exists {table_name};")

    def get_table(self, table_name: str, schema: str = "public") -> list[dict]:
        """Read an entire table and return as list of dictionaries."""
        
        # reflect the table from the database
        metadata = MetaData(schema=schema)
        table = Table(table_name, metadata, autoload_with=self.engine)
        
        # query and return as list of dicts
        result = self.engine.execute(table.select()).fetchall()
        return [dict(row) for row in result]

    def execute_sql(self, sql: str) -> list[dict]:
        """Execute a raw SQL query and return rows as list of dictionaries."""
        with self.engine.connect() as connection:
            result = connection.execute(text(sql)).fetchall()
        return [dict(row._mapping) for row in result]

    def reflect_table(self, table_name: str, schema: str = "public") -> tuple[Table, MetaData]:
        """Reflect a table definition from the connected database."""
        metadata = MetaData(schema=schema)
        table = Table(table_name, metadata, autoload_with=self.engine)
        return table, metadata

    def create_table_like(
        self,
        source_table: Table,
        target_table_name: Optional[str] = None,
    ) -> tuple[Table, MetaData]:
        """Create a table in the current database with the same columns and primary keys as source_table."""
        table_name = target_table_name or source_table.name
        metadata = MetaData()
        columns = [
            Column(column.name, column.type, primary_key=column.primary_key)
            for column in source_table.columns
        ]
        table = Table(table_name, metadata, *columns)
        metadata.create_all(self.engine)
        return table, metadata

    def insert(self, data: list[dict], table: Table, metadata: MetaData) -> None:
        metadata.create_all(self.engine)
        insert_statement = postgresql.insert(table).values(data)
        self.engine.execute(insert_statement)

    def overwrite(self, data: list[dict], table: Table, metadata: MetaData) -> None:
        self.drop_table(table.name)
        self.insert(data=data, table=table, metadata=metadata)

    def upsert(
        self,
        data: list[dict],
        table: Table,
        metadata: MetaData,
        chunksize: int = 1000,
    ) -> None:
        """Upsert data in chunks to avoid pg8000 parameter limits.

        Args:
            data: list of row dictionaries to insert/upsert
            table: target SQLAlchemy Table object
            metadata: metadata containing the table schema
            chunksize: number of rows to process per statement
        """
        # ensure table exists
        metadata.create_all(self.engine)
        target_table = table

        # identify primary key columns
        key_columns = [
            pk_column.name for pk_column in target_table.primary_key.columns.values()
        ]

        max_length = len(data)
        # iterate through data in chunks
        for i in range(0, max_length, chunksize):
            upper = i + chunksize if i + chunksize < max_length else max_length
            chunk = data[i:upper]
            insert_statement = postgresql.insert(target_table).values(chunk)
            upsert_statement = insert_statement.on_conflict_do_update(
                index_elements=key_columns,
                set_={
                    c.key: c for c in insert_statement.excluded if c.key not in key_columns
                },
            )
            self.engine.execute(upsert_statement)
