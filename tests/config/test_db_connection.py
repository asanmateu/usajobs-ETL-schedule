import pytest
from src.config.DB_connection import *


class TestDBConnection:

    def test_db_connect(self):
        assert db_connect() is not None
