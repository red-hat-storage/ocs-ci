# -*- coding: utf8 -*-
"""
Module for interactions postgresql database.
"""
import logging
import functools

from psycopg2 import connect, OperationalError


logger = logging.getLogger(name=__file__)


class PgsqlManager:
    def __init__(
        self,
        username,
        password,
        keep_connection=False,
        database="postgres",
        host="localhost",
        port=5432,
    ):
        """
        Create of PgSQL Manager object to manage PostgreSQL database.

        Args:
            username (string): database user name
            password (string): password for database user
            keep_connection (bool): True if the connection should stay opened
            database (string): database name to work with
            host (string): IP or hostname of PostgreSQL server
            port (int): port number on which PostgreSQL server listen to

        """
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.database = database
        self._conn = None
        self.keep_connection = keep_connection

    class Cursor:
        """
        Context manager which make sure the cursor is opened and closed.
        """

        def __init__(self, conn):
            self.conn = conn
            self.cursor = None

        def __enter__(self):
            self.cursor = self.conn.cursor()
            return self.cursor

        def __exit__(self, exc_type, exc_value, exc_traceback):
            if self.cursor:
                self.cursor.close()

    @property
    def conn(self):
        """
        Connection property.
        """
        if not self._conn:
            self._connect()
        return self._conn

    def close_conn(self):
        """
        Method to close the connection.
        """
        if self._conn:
            self._conn.close()
            self._conn = None

    def conn_closure(func):
        """
        Decorator which will make sure the connection is closed after call of method
        decorated with. This also depends on self.keep_connection which may not close the
        connection if set to True, but in case of some error or crash, it will make sure
        the connection is always properly closed.
        """

        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            try:
                result = func(self, *args, **kwargs)
                return result
            except OperationalError:
                logger.exception(
                    f"Failed to run operation on postgresql server {self.host}"
                )
                if self._conn:
                    self.close_conn()
                raise
            finally:
                if self._conn and not self.keep_connection:
                    self.close_conn()

        return wrapper

    def _connect(self):
        """
        Method to create connection for this PgSQL manager class.
        """
        try:
            self._conn = connect(
                dbname=self.database,
                user=self.username,
                password=self.password,
                host=self.host,
                port=self.port,
            )
            self._conn.autocommit = True

        except OperationalError:
            logger.exception(f"Failed to connect to postgresql server {self.host}")
            raise

    @conn_closure
    def create_database(self, db_name, extra_params):
        """
        Create database.

        Args:
            db_name (string): database name
            extra_params (string): extra postgresql command to add after CREATE DATABASE db_name

        """
        with self.Cursor(self.conn) as cursor:
            cursor.execute(f"CREATE DATABASE {db_name} {extra_params}")
        logger.info(f"Database '{db_name}' created successfully!")

    @conn_closure
    def delete_database(self, db_name):
        """
        Delete the database.

        Args:
            db_name (string): database name

        """
        with self.Cursor(self.conn) as cursor:
            cursor.execute(
                "SELECT datname FROM pg_catalog.pg_database WHERE datname = %s",
                (db_name,),
            )
            result = cursor.fetchone()
            if result is None:
                logger.info(f"Database '{db_name}' does not exist.")
                return
            # Drop the database
            cursor.execute(f"DROP DATABASE {db_name}")

            logger.info(f"Database '{db_name}' deleted successfully!")
