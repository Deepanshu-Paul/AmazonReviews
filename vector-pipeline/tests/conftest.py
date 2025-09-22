# tests/conftest.py
import os
import pytest
from testcontainers.postgres import PostgresContainer

@pytest.fixture(scope="session")
def pg_container():
    container = PostgresContainer("postgres:16-alpine")
    container.start()
    try:
        # Propagate connection info via env so your loaders/handlers pick it up
        os.environ["POSTGRES_HOST"] = container.get_container_host_ip()
        os.environ["POSTGRES_PORT"] = container.get_exposed_port(5432)
        os.environ["POSTGRES_USER"] = container.username
        os.environ["POSTGRES_PASSWORD"] = container.password
        os.environ["POSTGRES_DB"] = container.dbname
        yield container
    finally:
        container.stop()
