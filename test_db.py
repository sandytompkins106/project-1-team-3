import os
from dotenv import load_dotenv
from db.postgresql_client import PostgreSqlClient

# Load environment variables
load_dotenv()

# Test database connection
try:
    print("Testing database connection...")
    print(f"SERVER_NAME: {os.environ.get('SERVER_NAME')}")
    print(f"DATABASE_NAME: {os.environ.get('DATABASE_NAME')}")
    print(f"DB_USERNAME: {os.environ.get('DB_USERNAME')}")
    print(f"DB_PASSWORD length: {len(os.environ.get('DB_PASSWORD', ''))}")

    client = PostgreSqlClient(
        server_name=os.environ.get("SERVER_NAME"),
        database_name=os.environ.get("DATABASE_NAME"),
        username=os.environ.get("DB_USERNAME"),
        password=os.environ.get("DB_PASSWORD"),
        port=int(os.environ.get("PORT", 5432))
    )

    print("✅ Database connection successful!")

except Exception as e:
    print(f"❌ Database connection failed: {e}")