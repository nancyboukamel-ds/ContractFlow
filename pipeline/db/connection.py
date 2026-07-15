"""
pipeline/db/connection.py
Async Postgres connection pool using asyncpg.
"""
## This is the database connection manager a tiny file with one job: create and share a single Postgres connection pool across all activities.
import asyncpg
from config import DATABASE_URL

_pool: asyncpg.Pool | None = None

"""
Why asyncpg instead of psycopg2?
- entire pipeline runs on asyncio Temporal's Python SDK is async, your activities are async functions. 
- psycopg2 is synchronous, meaning every database call would block the entire event loop and prevent other activities from running concurrently. 
- asyncpg is built for asyncio database calls are proper await operations that yield control back to the event loop while waiting for Postgres.

Why a connection pool instead of one connection per activity?
- Opening a database connection takes ~50-100ms: TCP handshake, authentication, SSL negotiation. If every activity opened and closed its own connection, a pipeline ingesting 499 contracts with 7 database calls each would spend significant time just connecting.
- A pool creates connections once at startup and reuses them:
Worker starts → pool opens 2 connections (min_size=2)
Activity runs → borrows a connection from pool
Activity done → returns connection to pool
Next activity → reuses the same connection instantly
"""

## The pool is created once when the first activity runs, then reused for the entire lifetime of the worker. The pattern:
async def get_pool() -> asyncpg.Pool:
    """Return the shared connection pool, creating it if needed."""
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            DATABASE_URL,
            min_size=2,
            ## at most 10 simultaneous database connections. Since running up to 5 child workflows in parallel, 
            # each potentially doing 2-3 database calls, 10 connections gives headroom without overwhelming Postgres.
            max_size=10,
            command_timeout=60,
        )
    return _pool

## called when the worker shuts down it gracefully closes all connections, allowing any in-flight queries to finish before the process exits. Without this, you'd get connection leak warnings from Postgres.
async def close_pool() -> None:
    """Gracefully close the pool (call on worker shutdown)."""
    global _pool
    if _pool:
        await _pool.close()
        _pool = None