"""
Centralized SSH Tunnel and MySQL Connection Management.

Consolidates SSH tunnel, MySQL engine creation, connection pooling,
and query execution logic previously duplicated across wordpress_extractor.py
and limesurvey_extractor.py.

Supports:
- Direct SSH tunnels (WordPress/WPEngine pattern)
- gcloud compute ssh tunnels with optional IAP
- Cross-platform: Windows (CREATE_NO_WINDOW) and Linux/Mac
- Thread-safe connection pooling with health checks
- Query execution with transient error retry
"""

import logging
import os
import socket
import subprocess
import sys
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

import sqlalchemy as sa
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Port Management
# ---------------------------------------------------------------------------

def find_free_port() -> Tuple[int, socket.socket]:
    """
    Find a free port and return both the port number and the socket.

    Keeps socket open to prevent port reuse race condition.
    Caller must close the socket after the tunnel binds to the port.
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('', 0))
    s.listen(1)
    port = s.getsockname()[1]
    return port, s


# ---------------------------------------------------------------------------
# SSH Tunnel Lifecycle
# ---------------------------------------------------------------------------

def start_ssh_tunnel(
    ssh_host: str,
    ssh_user: str,
    ssh_keyfile: str,
    db_host: str,
    db_port: int,
    local_port: int,
    port_socket: Optional[socket.socket] = None,
    use_gcloud: bool = False,
    gcloud_vm_name: Optional[str] = None,
    gcloud_project: Optional[str] = None,
    gcloud_zone: Optional[str] = None,
    use_iap: bool = False,
) -> subprocess.Popen:
    """
    Start an SSH tunnel forwarding ``local_port`` to ``db_host:db_port``.

    Uses direct SSH by default.  When *use_gcloud* is ``True`` the tunnel is
    opened via ``gcloud compute ssh`` instead (with optional IAP).
    """
    tunnel_cmd = [
        'ssh',
        '-i', ssh_keyfile,
        '-L', f'{local_port}:{db_host}:{db_port}',
        '-N',
        '-o', 'StrictHostKeyChecking=no',
        '-o', 'UserKnownHostsFile=/dev/null',
        '-o', 'LogLevel=ERROR',
        '-o', 'ConnectTimeout=30',
        '-o', 'TCPKeepAlive=yes',
        '-o', 'ServerAliveInterval=60',
        '-o', 'ServerAliveCountMax=3',
        f'{ssh_user}@{ssh_host}',
    ]
    logger.info(f"Using direct SSH tunnel: {ssh_user}@{ssh_host}")

    # Platform-specific subprocess creation
    if sys.platform == 'win32':
        creation_flags = getattr(subprocess, 'CREATE_NO_WINDOW', 0)
        tunnel_process = subprocess.Popen(
            tunnel_cmd,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=creation_flags,
        )
    else:
        tunnel_process = subprocess.Popen(
            tunnel_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

    # Release port reservation so the tunnel can bind
    if port_socket:
        try:
            port_socket.close()
        except Exception:
            pass

    # gcloud tunnels need more time to establish
    sleep_time = 5 if use_gcloud else 2
    time.sleep(sleep_time)

    if tunnel_process.poll() is not None:
        stdout, stderr = tunnel_process.communicate(timeout=1)
        error_msg = stderr.decode('utf-8', errors='ignore') if stderr else "No error message"
        raise RuntimeError(f"SSH tunnel failed to start. Error: {error_msg}")

    # For gcloud tunnels, wait until the port is actually listening
    if use_gcloud:
        _wait_for_port(tunnel_process, local_port, max_wait=15)

    return tunnel_process


def _wait_for_port(
    tunnel_process: subprocess.Popen,
    local_port: int,
    max_wait: int = 15,
) -> None:
    """Block until *local_port* is accepting connections or *max_wait* seconds elapse."""
    logger.info(f"Waiting for port {local_port} to become available...")
    waited = 0
    while waited < max_wait:
        if tunnel_process.poll() is not None:
            stdout, stderr = tunnel_process.communicate(timeout=1)
            error_msg = stderr.decode('utf-8', errors='ignore') if stderr else "No error message"
            raise RuntimeError(f"SSH tunnel process exited unexpectedly: {error_msg}")

        try:
            test_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            test_socket.settimeout(1)
            result = test_socket.connect_ex(('127.0.0.1', local_port))
            test_socket.close()
            if result == 0:
                logger.info(f"Port {local_port} is now listening")
                time.sleep(2)  # brief stabilisation pause
                return
        except Exception:
            pass
        time.sleep(1)
        waited += 1

    logger.warning(f"Port {local_port} not ready after {max_wait}s - proceeding anyway")


def stop_ssh_tunnel(tunnel_process: Optional[subprocess.Popen]) -> None:
    """Gracefully terminate an SSH tunnel subprocess."""
    if tunnel_process is None:
        return
    try:
        tunnel_process.terminate()
        tunnel_process.wait(timeout=3)
    except subprocess.TimeoutExpired:
        tunnel_process.kill()
        tunnel_process.wait()


# ---------------------------------------------------------------------------
# MySQL Engine Creation
# ---------------------------------------------------------------------------

def create_mysql_engine(
    db_user: str,
    db_password: str,
    db_host: str,
    db_port: int,
    db_name: str,
    *,
    pool_size: int = 5,
    max_overflow: int = 5,
    pool_timeout: int = 60,
    connect_timeout: int = 60,
    read_timeout: int = 600,
    write_timeout: int = 600,
) -> sa.engine.Engine:
    """
    Create a SQLAlchemy MySQL engine with sensible pool defaults.

    All timeout values are in seconds.
    """
    connection_string = (
        f"mysql+pymysql://{db_user}:{db_password}"
        f"@{db_host}:{db_port}/{db_name}"
        f"?charset=utf8mb4&connect_timeout={connect_timeout}"
        f"&read_timeout={read_timeout}&write_timeout={write_timeout}"
    )

    engine = create_engine(
        connection_string,
        pool_pre_ping=True,
        pool_recycle=300,
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_timeout=pool_timeout,
        echo=False,
        connect_args={
            'read_timeout': read_timeout,
            'write_timeout': write_timeout,
        },
    )
    return engine


# ---------------------------------------------------------------------------
# Query Execution with Retry
# ---------------------------------------------------------------------------

def execute_query_with_retry(
    engine: sa.engine.Engine,
    query: str,
    params: Optional[Dict[str, Any]] = None,
    *,
    max_attempts: int = 3,
    base_delay: float = 2.0,
    session_timeout_ms: int = 60_000,
) -> List[Dict[str, Any]]:
    """
    Execute a SQL query with transient-error retry.

    Transient errors (timeout, connection lost, deadlock) are retried up to
    *max_attempts* times with exponential backoff.
    """
    from shared.error_handler import retry_on_transient_error, TransientError

    @retry_on_transient_error(max_attempts=max_attempts, base_delay=base_delay)
    def _do_query():
        with engine.connect() as conn:
            try:
                conn.execute(text(f"SET SESSION max_execution_time = {session_timeout_ms}"))
            except Exception as e:
                logger.debug(f"Could not set session timeout: {e}")

            try:
                if params:
                    result = conn.execute(text(query), params)
                else:
                    result = conn.execute(text(query))
                rows = result.fetchall()
                if not rows:
                    return []
                return [dict(zip(result.keys(), row)) for row in rows]
            except Exception as e:
                error_str = str(e).lower()
                transient_keywords = [
                    'timeout', 'connection', 'lock', 'deadlock', 'lost connection',
                ]
                if any(kw in error_str for kw in transient_keywords):
                    raise TransientError(f"Transient database error: {e}")
                raise

    return _do_query()


# ---------------------------------------------------------------------------
# Connection Health Check
# ---------------------------------------------------------------------------

def test_connection_health(engine: sa.engine.Engine) -> bool:
    """Return ``True`` if a simple ``SELECT 1`` succeeds against *engine*."""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            result.fetchone()
        return True
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Thread-Safe Connection Pool
# ---------------------------------------------------------------------------

_POOL: Dict[str, Dict[str, Any]] = {}
_POOL_LOCK = threading.Lock()


def get_pooled_connection(
    key: str,
    factory: Any,
    *factory_args: Any,
    **factory_kwargs: Any,
) -> Dict[str, Any]:
    """
    Return a pooled connection dict for *key*, creating via *factory* if needed.

    *factory* must return a dict containing at least ``'engine'`` and
    ``'tunnel'`` keys.

    Thread-safe with double-check locking; health check runs outside the lock
    to avoid blocking other threads.
    """
    # Fast path – already pooled
    with _POOL_LOCK:
        conn_info = _POOL.get(key)

    if conn_info is not None:
        if test_connection_health(conn_info['engine']):
            return conn_info
        # Unhealthy – remove and recreate
        logger.warning(f"Unhealthy connection for {key}, recreating...")
        with _POOL_LOCK:
            _POOL.pop(key, None)
        _cleanup_connection(conn_info, key)

    # Create new connection (slow – done outside lock)
    logger.debug(f"Creating new connection for {key}")
    conn_info = factory(*factory_args, **factory_kwargs)

    with _POOL_LOCK:
        if key in _POOL:
            # Another thread won the race
            existing = _POOL[key]
            _safe_dispose(conn_info, key)
            return existing
        _POOL[key] = conn_info
        return conn_info


def cleanup_pooled_connection(key: str) -> None:
    """Remove and clean up a single pooled connection."""
    with _POOL_LOCK:
        conn_info = _POOL.pop(key, None)
    if conn_info:
        _cleanup_connection(conn_info, key)


def cleanup_all_connections() -> None:
    """Remove and clean up all pooled connections."""
    with _POOL_LOCK:
        items = list(_POOL.items())
        _POOL.clear()
    for key, conn_info in items:
        _cleanup_connection(conn_info, key)


def _cleanup_connection(conn_info: Dict[str, Any], key: str) -> None:
    """Dispose engine and stop tunnel for a connection dict."""
    _safe_dispose(conn_info, key)


def _safe_dispose(conn_info: Dict[str, Any], key: str) -> None:
    """Best-effort disposal of engine and tunnel."""
    try:
        if 'engine' in conn_info:
            conn_info['engine'].dispose()
    except Exception as e:
        logger.warning(f"Failed to dispose engine for {key}: {e}")
    try:
        if 'tunnel' in conn_info:
            stop_ssh_tunnel(conn_info['tunnel'])
    except Exception as e:
        logger.warning(f"Failed to stop tunnel for {key}: {e}")


__all__ = [
    'find_free_port',
    'start_ssh_tunnel',
    'stop_ssh_tunnel',
    'create_mysql_engine',
    'execute_query_with_retry',
    'test_connection_health',
    'get_pooled_connection',
    'cleanup_pooled_connection',
    'cleanup_all_connections',
]
