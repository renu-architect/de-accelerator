"""JDBC connectors (SQL Server, Azure SQL, PostgreSQL). Importing this package registers them."""

from de_accelerator.connectors.jdbc.connector import JdbcConnector

__all__ = ["JdbcConnector"]
