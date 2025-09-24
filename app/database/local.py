from os import environ
from psycopg_pool import AsyncConnectionPool


pool = AsyncConnectionPool(
    environ['DB_CONN'],
    min_size=1,
    max_size=4,
    open=False,
)
