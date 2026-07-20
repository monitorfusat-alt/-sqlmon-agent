#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
SQL Server Monitoring Agent - OpenTelemetry OTLP para Grafana Cloud FREE
VERSION CORREGIDA v3 - ObservableGauge + Exportación manual sincronizada
Compatible con Python 3.6+ (sin f-strings)
Soporta ODBC Driver 17 y 18
Arreglado para SQL Server 2019 y usuarios con permisos limitados

Envia metricas directamente a Grafana Cloud via OTLP/HTTP.

FIXES aplicados en v3 (2026-07-19):
- MetricsSnapshot.count() agregado (AttributeError que causaba crash al final del ciclo)
- MetricsSnapshot.clear() agregado (AttributeError en _export_metrics finally block)
- API key removida del valor por defecto hardcodeado (seguridad)
- Corregido log "max_wait=Nones" cuando max_wait_time_sec es None
- FIX CRITICO: _send_metric ahora usa clave unica por servidor+metrica+labels.
  Con key=solo "name", los servidores procesados en paralelo se sobreescribian
  en el snapshot y solo llegaba el ultimo a Grafana (causa: 10 servidores perdidos)

FIXES aplicados en v2:
- Exportación MANUAL sincronizada con ciclo de monitoreo
- ObservableGauge con snapshot de métricas (no acumulativas)
- Variables globales reemplazadas por diccionario thread-safe
- Manejo de errores robusto en conexion DB2/SQL Server
- Credenciales via variables de entorno (no hardcodeadas)
- Query RESOURCES corregida para SQL Server 2019 (columna inválida eliminada)
- Query JOBS corregida para usuarios sin permisos en sysjobschedules
- Logging mejorado para diagnostico
"""

from __future__ import print_function

import os
import sys
import time
import logging
import signal
import argparse
import configparser
import threading
import base64
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---------------------------------------------------------------------------
# OPENTELEMETRY
# ---------------------------------------------------------------------------
try:
    from opentelemetry.metrics import Observation, get_meter_provider, set_meter_provider
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import InMemoryMetricReader
    from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.metrics.export import MetricsData
except ImportError:
    print("ERROR: OpenTelemetry no instalado. Ejecute:")
    print("  pip install opentelemetry-api opentelemetry-sdk opentelemetry-exporter-otlp-proto-http")
    sys.exit(1)

# ---------------------------------------------------------------------------
# DEPENDENCIAS DB
# ---------------------------------------------------------------------------
try:
    import pyodbc
except ImportError:
    pyodbc = None

try:
    import ibm_db
except ImportError:
    ibm_db = None


# ---------------------------------------------------------------------------
# CONFIGURACION OTLP DESDE VARIABLES DE ENTORNO
# ---------------------------------------------------------------------------
OTLP_ENDPOINT = os.getenv(
    "GRAFANA_CLOUD_OTLP_ENDPOINT",
    "https://otlp-gateway-prod-us-east-2.grafana.net/otlp/v1/metrics"
)

GRAFANA_CLOUD_USERNAME = os.getenv("GRAFANA_CLOUD_USERNAME", "1283546")
GRAFANA_CLOUD_API_KEY = os.getenv("GRAFANA_CLOUD_API_KEY", "")

if not GRAFANA_CLOUD_USERNAME or not GRAFANA_CLOUD_API_KEY:
    print("ERROR: Debe configurar GRAFANA_CLOUD_USERNAME y GRAFANA_CLOUD_API_KEY")
    print("  export GRAFANA_CLOUD_USERNAME='tu_user_id'")
    print("  export GRAFANA_CLOUD_API_KEY='tu_api_key'")
    sys.exit(1)

# Autenticacion Basic Auth
auth_header_value = base64.b64encode(
    ("%s:%s" % (GRAFANA_CLOUD_USERNAME, GRAFANA_CLOUD_API_KEY)).encode("utf-8")
).decode("utf-8")

AUTH_HEADERS = {"Authorization": "Basic %s" % auth_header_value}

print("INFO: OTLP Endpoint: %s" % OTLP_ENDPOINT)
print("INFO: Grafana Cloud User: %s" % GRAFANA_CLOUD_USERNAME)


# ---------------------------------------------------------------------------
# CONFIGURACION OPENTELEMETRY - EXPORTACION MANUAL
# ---------------------------------------------------------------------------
resource = Resource.create({"service.name": "sqlmon-agent"})

# Usar InMemoryMetricReader para controlar cuándo exportamos
metric_reader = InMemoryMetricReader()

meter_provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
set_meter_provider(meter_provider)

meter = get_meter_provider().get_meter(__name__)

# Crear el exporter para enviar manualmente
otlp_exporter = OTLPMetricExporter(
    endpoint=OTLP_ENDPOINT,
    headers=AUTH_HEADERS,
    timeout=30,
)


# ---------------------------------------------------------------------------
# DETECTAR ODBC DRIVER DISPONIBLE
# ---------------------------------------------------------------------------
def detect_odbc_driver():
    """Detecta si ODBC Driver 17 o 18 esta instalado"""
    if not pyodbc:
        return None
    try:
        conn_str = "DRIVER={ODBC Driver 18 for SQL Server};SERVER=localhost;UID=test;PWD=test;"
        pyodbc.connect(conn_str, timeout=2)
        return "ODBC Driver 18 for SQL Server"
    except Exception:
        pass

    try:
        conn_str = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;UID=test;PWD=test;"
        pyodbc.connect(conn_str, timeout=2)
        return "ODBC Driver 17 for SQL Server"
    except Exception:
        pass

    try:
        drivers = pyodbc.drivers()
        for driver in drivers:
            if "ODBC Driver 18 for SQL Server" in driver:
                return driver
        for driver in drivers:
            if "ODBC Driver 17 for SQL Server" in driver:
                return driver
        for driver in drivers:
            if "SQL Server" in driver:
                return driver
    except Exception:
        pass

    return "ODBC Driver 17 for SQL Server"


ODBC_DRIVER = detect_odbc_driver()
if ODBC_DRIVER:
    print("INFO: Usando ODBC Driver: %s" % ODBC_DRIVER)
else:
    print("WARN: pyodbc no disponible, solo modo DB2")


# ---------------------------------------------------------------------------
# CONFIGURACION POR DEFECTO
# ---------------------------------------------------------------------------
DEFAULT_CONFIG = {
    "agent": {
        "interval_seconds": "60",
        "max_workers": "5",
        "query_timeout": "30",
        "connection_timeout": "5",
        "log_level": "INFO",
        "log_file": "/var/log/sqlmon-agent/sqlmon-agent.log",
        "tmp_dir": "/tmp/sqlmon-agent",
    },
    "server_file": {
        "path": "/opt/sqlmon-agent/servers.txt",
    },
}


# ---------------------------------------------------------------------------
# LOGGER
# ---------------------------------------------------------------------------
def setup_logging(log_file, log_level):
    """Configura el sistema de logging"""
    logger = logging.getLogger("sqlmon-agent")
    logger.setLevel(getattr(logging, log_level.upper()))

    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    logger.addHandler(console)

    if log_file:
        try:
            os.makedirs(os.path.dirname(log_file), exist_ok=True)
        except Exception:
            pass
        try:
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        except Exception as e:
            print("WARN: No se pudo crear archivo de log: %s" % str(e))

    return logger


# ---------------------------------------------------------------------------
# PARSER DE SERVIDORES
# ---------------------------------------------------------------------------
class ServerConfig(object):
    """Configuracion de un servidor SQL Server"""

    def __init__(self, name, ip, instance, port, environment, group, region,
                 priority, contact, username, password):
        self.name = name
        self.ip = ip
        self.instance = instance
        self.port = int(port)
        self.environment = environment
        self.group = group
        self.region = region
        self.priority = priority
        self.contact = contact
        self.username = username
        self.password = password

    @property
    def connection_string(self):
        """Construye el connection string ODBC"""
        if self.instance and self.instance.upper() != "MSSQLSERVER":
            server = "%s\\%s,%d" % (self.ip, self.instance, self.port)
        else:
            server = "%s,%d" % (self.ip, self.port)

        trust_cert = "yes"
        query_timeout = getattr(self, '_query_timeout', 30)

        return (
            "DRIVER={%s};"
            "SERVER=%s;"
            "DATABASE=master;"
            "UID=%s;"
            "PWD=%s;"
            "Connection Timeout=%d;"
            "TrustServerCertificate=%s;"
            "Query Timeout=%d;"
        ) % (ODBC_DRIVER, server, self.username, self.password, 5, trust_cert, query_timeout)

    @property
    def tags(self):
        """Tags comunes para metricas de este servidor"""
        return {
            "server": self.name,
            "ip": self.ip,
            "entorno": self.environment,
            "grupo": self.group,
            "region": self.region,
            "prioridad": self.priority,
        }


class ServerParser(object):
    """Parsea el archivo de configuracion de servidores"""

    @staticmethod
    def parse(file_path):
        """Lee servers.txt y retorna lista de ServerConfig"""
        servers = []

        if not os.path.exists(file_path):
            raise FileNotFoundError("Archivo no encontrado: %s" % file_path)

        with open(file_path, "r", encoding="utf-8") as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()

                if not line or line.startswith("#"):
                    continue

                parts = [p.strip() for p in line.split("|")]

                if len(parts) < 11:
                    print("WARN: Linea %d ignorada (campos insuficientes): %s" % (line_num, line))
                    continue

                try:
                    server = ServerConfig(
                        name=parts[0],
                        ip=parts[1],
                        instance=parts[2],
                        port=int(parts[3]),
                        environment=parts[4],
                        group=parts[5],
                        region=parts[6],
                        priority=parts[7],
                        contact=parts[8],
                        username=parts[9],
                        password=parts[10],
                    )
                    servers.append(server)
                except (ValueError, IndexError) as e:
                    print("WARN: Error parseando linea %d: %s" % (line_num, str(e)))

        return servers


# ---------------------------------------------------------------------------
# METRICAS THREAD-SAFE - SNAPSHOT PARA ObservableGauge
# ---------------------------------------------------------------------------
class MetricsSnapshot(object):
    """
    Snapshot thread-safe de metricas para ObservableGauge.
    Mantiene un snapshot que se actualiza durante el ciclo y se lee
    cuando OTLP solicita las metricas.
    """

    def __init__(self):
        self._current = {}
        self._previous = {}
        self._lock = threading.Lock()

    def update(self, key, value, labels=None):
        """Actualiza una metrica en el snapshot actual"""
        with self._lock:
            self._current[key] = {"value": value, "labels": labels or {}}

    def get_observations(self):
        """Retorna todas las observaciones del snapshot actual como LISTA"""
        with self._lock:
            # Copiar snapshot actual para lectura segura
            snapshot = dict(self._current)
            return [Observation(item["value"], item["labels"]) for item in snapshot.values()]

    def rotate(self):
        """
        Rota el snapshot: el actual se convierte en anterior y se limpia el actual.
        Esto permite que OTLP lea el snapshot anterior mientras se construye uno nuevo.
        """
        with self._lock:
            self._previous = dict(self._current)
            self._current = {}

    def get_previous_observations(self):
        """Retorna observaciones del snapshot anterior (para exportación)"""
        with self._lock:
            for key, item in self._previous.items():
                yield Observation(item["value"], item["labels"])

    def count(self):
        """Retorna el número de métricas en el snapshot actual"""
        with self._lock:
            return len(self._current)

    def clear(self):
        """Limpia el snapshot actual y el anterior"""
        with self._lock:
            self._current = {}
            self._previous = {}


# Estado global de metricas
METRICS_SNAPSHOT = MetricsSnapshot()


# ---------------------------------------------------------------------------
# CALLBACK PARA ObservableGauge
# ---------------------------------------------------------------------------
def metrics_callback(options):
    """Callback que OTLP llama para obtener las metricas actuales"""
    return METRICS_SNAPSHOT.get_observations()


# ---------------------------------------------------------------------------
# REGISTRO DE METRICAS OTLP - ObservableGauge unificado
# ---------------------------------------------------------------------------
# Crear UN ObservableGauge que maneja todas las metricas
# Cada Observation tiene atributos que identifican la metrica especifica
meter.create_observable_gauge(
    "sqlmon_all_metrics",
    callbacks=[metrics_callback],
    description="SQL Server monitoring metrics",
)


# ---------------------------------------------------------------------------
# QUERIES DMV - ARREGLADAS PARA SQL 2019 Y PERMISOS LIMITADOS
# ---------------------------------------------------------------------------
class DMVQueries(object):
    """Coleccion de queries DMV para monitoreo"""

    ENGINE_STATUS = """
    SET NOCOUNT ON;
    SELECT
        @@SERVERNAME AS server_name,
        GETDATE() AS check_time,
        SERVERPROPERTY('ProductVersion') AS version,
        SERVERPROPERTY('Edition') AS edition,
        DATEDIFF(SECOND, (SELECT sqlserver_start_time FROM sys.dm_os_sys_info), GETDATE()) AS uptime_seconds,
        1 AS is_up;
    """

    ERROR_LOG = """
    SET NOCOUNT ON;
    CREATE TABLE #ErrorLog (LogDate DATETIME, ProcessInfo NVARCHAR(50), Text NVARCHAR(MAX));
    INSERT INTO #ErrorLog EXEC xp_readerrorlog 0, 1, NULL, NULL, NULL, NULL, N'desc';
    SELECT
        @@SERVERNAME AS server_name,
        GETDATE() AS check_time,
        COUNT(*) AS total_errors_last_hour,
        SUM(CASE WHEN Text LIKE '%Severity: 17%' OR Text LIKE '%Severity: 18%' OR Text LIKE '%Severity: 19%' THEN 1 ELSE 0 END) AS severity_17_19,
        SUM(CASE WHEN Text LIKE '%Severity: 20%' OR Text LIKE '%Severity: 21%' OR Text LIKE '%Severity: 22%' OR Text LIKE '%Severity: 23%' OR Text LIKE '%Severity: 24%' OR Text LIKE '%Severity: 25%' THEN 1 ELSE 0 END) AS severity_20_25,
        SUM(CASE WHEN Text LIKE '%deadlock%' OR Text LIKE '%Deadlock%' THEN 1 ELSE 0 END) AS deadlock_count,
        SUM(CASE WHEN Text LIKE '%backup failed%' OR Text LIKE '%BACKUP FAILED%' THEN 1 ELSE 0 END) AS backup_failures,
        SUM(CASE WHEN Text LIKE '%I/O error%' OR Text LIKE '%I/O Error%' THEN 1 ELSE 0 END) AS io_errors,
        SUM(CASE WHEN Text LIKE '%login failed%' OR Text LIKE '%Login failed%' THEN 1 ELSE 0 END) AS login_failures,
        MAX(LogDate) AS last_error_time
    FROM #ErrorLog WHERE LogDate >= DATEADD(HOUR, -1, GETDATE());
    DROP TABLE #ErrorLog;
    """

    BLOCKING = """
    SET NOCOUNT ON;
    WITH BlockingTree AS (
        SELECT
            r.session_id AS blocked_session,
            r.blocking_session_id AS blocking_session,
            DB_NAME(r.database_id) AS database_name,
            r.wait_type,
            r.wait_time / 1000.0 AS wait_time_sec,
            r.wait_resource,
            r.command,
            r.status,
            s.login_name,
            s.program_name,
            0 AS level
        FROM sys.dm_exec_requests r
        INNER JOIN sys.dm_exec_sessions s ON r.session_id = s.session_id
        WHERE r.blocking_session_id IS NOT NULL AND r.blocking_session_id <> 0

        UNION ALL

        SELECT
            r.session_id,
            r.blocking_session_id,
            DB_NAME(r.database_id),
            r.wait_type,
            r.wait_time / 1000.0,
            r.wait_resource,
            r.command,
            r.status,
            s.login_name,
            s.program_name,
            bt.level + 1
        FROM sys.dm_exec_requests r
        INNER JOIN sys.dm_exec_sessions s ON r.session_id = s.session_id
        INNER JOIN BlockingTree bt ON r.session_id = bt.blocking_session
        WHERE r.blocking_session_id IS NOT NULL AND r.blocking_session_id <> 0
    )
    SELECT
        @@SERVERNAME AS server_name, GETDATE() AS check_time,
        COUNT(DISTINCT blocked_session) AS blocked_sessions_count,
        COUNT(DISTINCT blocking_session) AS blocking_sessions_count,
        MAX(CASE WHEN wait_time_sec > 30 THEN 1 ELSE 0 END) AS has_critical_blocking,
        MAX(wait_time_sec) AS max_wait_time_sec,
        AVG(wait_time_sec) AS avg_wait_time_sec,
        SUM(CASE WHEN wait_time_sec > 30 THEN 1 ELSE 0 END) AS critical_blocks_count,
        SUM(CASE WHEN wait_time_sec > 300 THEN 1 ELSE 0 END) AS severe_blocks_count
    FROM BlockingTree;
    """

    WAIT_STATS = """
    SET NOCOUNT ON;
    SELECT TOP 50
        @@SERVERNAME AS server_name, GETDATE() AS check_time,
        wait_type, waiting_tasks_count,
        wait_time_ms / 1000.0 AS wait_time_sec,
        max_wait_time_ms / 1000.0 AS max_wait_time_sec,
        signal_wait_time_ms / 1000.0 AS signal_wait_time_sec,
        CASE
            WHEN wait_type LIKE 'PAGEIOLATCH%' THEN 'I/O Disk'
            WHEN wait_type LIKE 'PAGELATCH%' THEN 'Memory Contention'
            WHEN wait_type LIKE 'LCK%' THEN 'Locking'
            WHEN wait_type LIKE 'LATCH%' THEN 'Latch'
            WHEN wait_type LIKE 'CXPACKET%' OR wait_type LIKE 'CXCONSUMER%' THEN 'Parallelism'
            WHEN wait_type LIKE 'SOS_SCHEDULER%' THEN 'CPU Pressure'
            WHEN wait_type LIKE 'THREADPOOL%' THEN 'Thread Starvation'
            WHEN wait_type LIKE 'WRITELOG%' THEN 'Transaction Log I/O'
            WHEN wait_type LIKE 'ASYNC_NETWORK_IO%' THEN 'Network'
            WHEN wait_type LIKE 'BACKUPIO%' THEN 'Backup I/O'
            WHEN wait_type LIKE 'HADR_SYNC_COMMIT%' OR wait_type LIKE 'HADR_%' THEN 'Availability Groups'
            WHEN wait_type LIKE 'BUFFER%' THEN 'Buffer Pool'
            ELSE 'Other'
        END AS wait_category,
        CASE
            WHEN wait_time_ms > 10000 THEN 'CRITICAL'
            WHEN wait_time_ms > 5000 THEN 'WARNING'
            WHEN wait_time_ms > 1000 THEN 'ELEVATED'
            ELSE 'NORMAL'
        END AS severity
    FROM sys.dm_os_wait_stats
    WHERE wait_type NOT IN (
        'CLR_SEMAPHORE', 'LAZYWRITER_SLEEP', 'RESOURCE_QUEUE', 'SLEEP_TASK',
        'SLEEP_SYSTEMTASK', 'SQLTRACE_BUFFER_FLUSH', 'WAITFOR', 'LOGMGR_QUEUE',
        'CHECKPOINT_QUEUE', 'REQUEST_FOR_DEADLOCK_SEARCH', 'XE_TIMER_EVENT',
        'BROKER_TO_FLUSH', 'BROKER_TASK_STOP', 'CLR_MANUAL_EVENT', 'CLR_AUTO_EVENT',
        'DISPATCHER_QUEUE_SEMAPHORE', 'FT_IFTS_SCHEDULER_IDLE_WAIT', 'XE_DISPATCHER_WAIT',
        'XE_DISPATCHER_JOIN', 'SQLTRACE_INCREMENTAL_FLUSH_SLEEP', 'ONDEMAND_TASK_QUEUE',
        'BROKER_EVENTHANDLER', 'SLEEP_BPOOL_FLUSH', 'DIRTY_PAGE_POLL', 'HADR_FILESTREAM_IOMGR_IOCOMPLETION',
        'SP_SERVER_DIAGNOSTICS_SLEEP', 'QDS_PERSIST_TASK_MAIN_LOOP_SLEEP', 'QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP'
    )
    AND waiting_tasks_count > 0
    ORDER BY wait_time_ms DESC;
    """

    SLOW_QUERIES = """
    SET NOCOUNT ON;
    SELECT TOP 30
        @@SERVERNAME AS server_name, GETDATE() AS check_time,
        r.session_id,
        r.status,
        DB_NAME(r.database_id) AS database_name,
        r.command,
        r.cpu_time         / 1000.0 AS cpu_time_sec,
        r.total_elapsed_time / 1000.0 AS elapsed_time_sec,
        r.reads,
        r.writes,
        r.logical_reads,
        r.granted_query_memory / 128.0 AS granted_memory_mb,
        r.wait_type,
        r.wait_time / 1000.0 AS wait_time_sec,
        r.blocking_session_id,
        s.login_name,
        s.program_name,
        s.host_name,
        -- Texto completo del batch (primeros 500 chars, sin saltos de linea)
        ISNULL(
            LEFT(
                REPLACE(REPLACE(REPLACE(qt.text, CHAR(13), ' '), CHAR(10), ' '), CHAR(9), ' '),
                500
            ),
            'N/A'
        ) AS query_text,
        -- Statement exacto en ejecucion (recortado por statement_start/end_offset)
        ISNULL(
            LEFT(
                REPLACE(REPLACE(REPLACE(
                    SUBSTRING(
                        qt.text,
                        (r.statement_start_offset / 2) + 1,
                        CASE r.statement_end_offset
                            WHEN -1 THEN DATALENGTH(qt.text)
                            ELSE r.statement_end_offset
                        END - r.statement_start_offset
                    ),
                CHAR(13), ' '), CHAR(10), ' '), CHAR(9), ' '),
                500
            ),
            'N/A'
        ) AS statement_text,
        CASE
            WHEN r.total_elapsed_time > 300000 THEN 'CRITICAL'
            WHEN r.total_elapsed_time >  60000 THEN 'WARNING'
            WHEN r.total_elapsed_time >  10000 THEN 'ELEVATED'
            ELSE 'NORMAL'
        END AS severity,
        CASE
            WHEN r.total_elapsed_time > 300000 THEN 3
            WHEN r.total_elapsed_time >  60000 THEN 2
            WHEN r.total_elapsed_time >  10000 THEN 1
            ELSE 0
        END AS severity_level
    FROM sys.dm_exec_requests r
    INNER JOIN sys.dm_exec_sessions s ON r.session_id = s.session_id
    -- CROSS APPLY obtiene el texto SQL del handle sin subquery costosa
    CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) AS qt
    WHERE
        -- Solo procesos de usuario (excluye sesiones internas del motor)
        s.is_user_process = 1

        -- Solo queries activos o bloqueados, no idle connections ni sleeping
        AND r.status IN ('running', 'suspended', 'runnable')

        -- Umbral de tiempo: mas de 10 segundos transcurridos
        AND r.total_elapsed_time > 10000

        -- Excluir comandos de background/mantenimiento del motor que corren
        -- continuamente y aparecen con elapsed de dias aunque no sean "lentos"
        AND r.command NOT IN (
            'GHOST CLEANUP',
            'LOG WRITER',
            'CHECKPOINT',
            'LAZY WRITER',
            'RESOURCE MONITOR',
            'SIGNAL HANDLER',
            'LOCK MONITOR',
            'TASK MANAGER',
            'TRACE QUEUE TASK',
            'XE TIMER',
            'XE DISPATCHER',
            'BRKR TASK',
            'BRKR EVENT HANDLER',
            'FT FULL PASS',
            'FT CRAWL MON',
            'RECEIVE',
            'REPLICATION COMMANDS',
            'DBCC',
            'BACKUP DATABASE',
            'BACKUP LOG',
            'RESTORE DATABASE',
            'RESTORE LOG',
            'WAITFOR'
        )

        -- Excluir el agente de monitoreo mismo para no aparecer en su propio reporte
        AND s.program_name NOT LIKE 'sqlmon%'
        AND s.program_name NOT LIKE 'go-mssqldb%'

        -- Garantizar que hay SQL activo (no sesiones conectadas sin query)
        AND r.sql_handle IS NOT NULL
        AND r.sql_handle != 0x0000000000000000000000000000000000000000

    ORDER BY r.total_elapsed_time DESC;
    """

    # CORREGIDO: Query de Resources sin sqlserver_cpu_utilization (no existe en SQL 2019)
    # Usa sys.dm_os_schedulers para calcular CPU usage
    RESOURCES = """
    SET NOCOUNT ON;
    SELECT
        @@SERVERNAME AS server_name, GETDATE() AS check_time,
        -- CPU: calcular usando dm_os_schedulers
        (SELECT CASE 
            WHEN (SELECT COUNT(*) FROM sys.dm_os_schedulers WHERE status = 'VISIBLE ONLINE') = 0 
            THEN 0 
            ELSE 100.0 - (SELECT COUNT(*) * 100.0 / (SELECT COUNT(*) FROM sys.dm_os_schedulers WHERE status = 'VISIBLE ONLINE') 
                          FROM sys.dm_os_schedulers WHERE is_idle = 1 AND status = 'VISIBLE ONLINE')
         END) AS avg_cpu_percent,
        (SELECT physical_memory_kb / 1024.0 / 1024.0 FROM sys.dm_os_sys_info) AS physical_memory_gb,
        (SELECT committed_kb / 1024.0 / 1024.0 FROM sys.dm_os_sys_info) AS committed_memory_gb,
        (SELECT COUNT(*) * 8.0 / 1024 / 1024 FROM sys.dm_os_buffer_descriptors WHERE database_id <> 32767) AS buffer_pool_used_gb,
        (SELECT COUNT(*) * 8.0 / 1024 / 1024 FROM sys.dm_os_buffer_descriptors) AS buffer_pool_total_gb,
        (SELECT COUNT(*) FROM sys.dm_exec_sessions WHERE status = 'sleeping' AND last_request_start_time < DATEADD(MINUTE, -5, GETDATE())) AS idle_connections,
        (SELECT COUNT(*) FROM sys.dm_exec_sessions WHERE status = 'running') AS active_connections,
        (SELECT COUNT(*) FROM sys.dm_exec_sessions WHERE is_user_process = 1) AS user_connections,
        (SELECT COUNT(*) FROM sys.dm_exec_sessions) AS total_connections,
        (SELECT SUM(user_object_reserved_page_count) * 8.0 / 1024 / 1024 FROM sys.dm_db_file_space_usage) AS tempdb_user_objects_gb,
        (SELECT SUM(internal_object_reserved_page_count) * 8.0 / 1024 / 1024 FROM sys.dm_db_file_space_usage) AS tempdb_internal_objects_gb,
        (SELECT SUM(version_store_reserved_page_count) * 8.0 / 1024 / 1024 FROM sys.dm_db_file_space_usage) AS tempdb_version_store_gb;
    """

    DISK_SPACE = """
    SET NOCOUNT ON;
    SELECT TOP 50
        @@SERVERNAME AS server_name, GETDATE() AS check_time,
        DB_NAME(database_id) AS database_name, name AS logical_name, physical_name,
        type_desc AS file_type, size * 8.0 / 1024 / 1024 AS size_gb,
        max_size * 8.0 / 1024 / 1024 AS max_size_gb,
        CASE WHEN max_size = -1 THEN -1 ELSE (max_size - size) * 8.0 / 1024 / 1024 END AS free_space_gb,
        CASE WHEN max_size = -1 THEN 100 ELSE CAST((size * 100.0 / max_size) AS DECIMAL(5,2)) END AS used_percent,
        CASE
            WHEN max_size <> -1 AND (size * 100.0 / max_size) > 95 THEN 'CRITICAL'
            WHEN max_size <> -1 AND (size * 100.0 / max_size) > 85 THEN 'WARNING'
            WHEN max_size <> -1 AND (size * 100.0 / max_size) > 75 THEN 'ELEVATED'
            ELSE 'NORMAL'
        END AS severity
    FROM sys.master_files WHERE database_id > 4 ORDER BY used_percent DESC;
    """

    BACKUPS = """
    SET NOCOUNT ON;
    SELECT
        @@SERVERNAME AS server_name, GETDATE() AS check_time,
        d.name AS database_name, d.recovery_model_desc, d.state_desc,
        MAX(CASE WHEN bs.type = 'D' THEN bs.backup_finish_date END) AS last_full_backup,
        MAX(CASE WHEN bs.type = 'I' THEN bs.backup_finish_date END) AS last_diff_backup,
        MAX(CASE WHEN bs.type = 'L' THEN bs.backup_finish_date END) AS last_log_backup,
        DATEDIFF(HOUR, MAX(CASE WHEN bs.type = 'D' THEN bs.backup_finish_date END), GETDATE()) AS hours_since_full_backup,
        DATEDIFF(HOUR, MAX(CASE WHEN bs.type = 'L' THEN bs.backup_finish_date END), GETDATE()) AS hours_since_log_backup,
        CASE
            WHEN d.recovery_model_desc = 'FULL' AND DATEDIFF(HOUR, MAX(CASE WHEN bs.type = 'L' THEN bs.backup_finish_date END), GETDATE()) > 2 THEN 'CRITICAL'
            WHEN d.recovery_model_desc = 'FULL' AND DATEDIFF(HOUR, MAX(CASE WHEN bs.type = 'L' THEN bs.backup_finish_date END), GETDATE()) > 1 THEN 'WARNING'
            WHEN DATEDIFF(HOUR, MAX(CASE WHEN bs.type = 'D' THEN bs.backup_finish_date END), GETDATE()) > 48 THEN 'CRITICAL'
            WHEN DATEDIFF(HOUR, MAX(CASE WHEN bs.type = 'D' THEN bs.backup_finish_date END), GETDATE()) > 24 THEN 'WARNING'
            ELSE 'NORMAL'
        END AS backup_status
    FROM sys.databases d
    LEFT JOIN msdb.dbo.backupset bs ON d.name = bs.database_name
    WHERE d.state = 0
    GROUP BY d.name, d.recovery_model_desc, d.state_desc
    ORDER BY hours_since_full_backup DESC;
    """

    # CORREGIDO: Query de Jobs sin JOIN a sysjobschedules (requiere permisos especiales)
    JOBS = """
    SET NOCOUNT ON;
    SELECT TOP 50
        @@SERVERNAME AS server_name, GETDATE() AS check_time,
        j.name AS job_name, j.enabled AS is_enabled,
        CASE
            WHEN jh.run_status = 0 THEN 'Failed'
            WHEN jh.run_status = 1 THEN 'Succeeded'
            WHEN jh.run_status = 2 THEN 'Retry'
            WHEN jh.run_status = 3 THEN 'Canceled'
            WHEN jh.run_status = 4 THEN 'In Progress'
            ELSE 'Unknown'
        END AS last_run_status,
        jh.run_date AS last_run_date, jh.run_time AS last_run_time,
        jh.run_duration AS last_run_duration, jh.message AS last_run_message,
        CASE
            WHEN jh.run_status = 0 THEN 'CRITICAL'
            WHEN jh.run_status = 3 THEN 'WARNING'
            WHEN j.enabled = 0 THEN 'WARNING'
            WHEN jh.run_status = 4 AND jh.run_duration > 10000 THEN 'WARNING'
            ELSE 'NORMAL'
        END AS severity
    FROM msdb.dbo.sysjobs j
    LEFT JOIN (
        SELECT job_id, run_status, run_date, run_time, run_duration, message,
               ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY run_date DESC, run_time DESC) AS rn
        FROM msdb.dbo.sysjobhistory WHERE step_id = 0
    ) jh ON j.job_id = jh.job_id AND jh.rn = 1
    ORDER BY j.name;
    """

    AVAILABILITY_GROUPS = """
    SET NOCOUNT ON;
    IF EXISTS (SELECT 1 FROM sys.availability_groups)
    BEGIN
        SELECT
            @@SERVERNAME AS server_name, GETDATE() AS check_time,
            ag.name AS ag_name, ar.replica_server_name,
            ars.role_desc AS current_role, ars.synchronization_health_desc AS sync_health,
            ars.connected_state_desc AS connection_state, ars.operational_state_desc AS operational_state,
            ars.recovery_health_desc AS recovery_health, adc.database_name,
            drs.synchronization_state_desc AS sync_state, drs.database_state_desc AS database_state,
            drs.is_suspended, drs.suspend_reason_desc,
            drs.log_send_queue_size / 1024.0 / 1024.0 AS log_send_queue_gb,
            drs.log_send_rate / 1024.0 / 1024.0 AS log_send_rate_mb_sec,
            drs.redo_queue_size / 1024.0 / 1024.0 AS redo_queue_gb,
            drs.redo_rate / 1024.0 / 1024.0 AS redo_rate_mb_sec,
            drs.secondary_lag_seconds,
            CASE
                WHEN drs.synchronization_state_desc <> 'SYNCHRONIZED' AND ars.role_desc = 'PRIMARY' THEN 'WARNING'
                WHEN drs.is_suspended = 1 THEN 'CRITICAL'
                WHEN drs.secondary_lag_seconds > 300 THEN 'WARNING'
                WHEN ars.synchronization_health_desc <> 'HEALTHY' THEN 'CRITICAL'
                ELSE 'NORMAL'
            END AS severity
        FROM sys.availability_groups ag
        INNER JOIN sys.availability_replicas ar ON ag.group_id = ar.group_id
        INNER JOIN sys.dm_hadr_availability_replica_states ars ON ar.replica_id = ars.replica_id
        INNER JOIN sys.availability_databases_cluster adc ON ag.group_id = adc.group_id
        INNER JOIN sys.dm_hadr_database_replica_states drs ON adc.group_database_id = drs.group_database_id
            AND ar.replica_id = drs.replica_id;
    END
    ELSE
    BEGIN
        SELECT @@SERVERNAME AS server_name, GETDATE() AS check_time, 'NO_AG' AS ag_name,
               '' AS replica_server_name, '' AS current_role, 'HEALTHY' AS sync_health,
               0 AS secondary_lag_seconds, 'NORMAL' AS severity;
    END
    """

    DEADLOCKS = """
    SET NOCOUNT ON;
    SELECT
        @@SERVERNAME AS server_name, GETDATE() AS check_time,
        COUNT(*) AS deadlock_count_last_hour
    FROM (
        SELECT
            XEventData.XEvent.value('(@timestamp)[1]', 'datetime2') AS deadlock_time,
            XEventData.XEvent.query('.') AS deadlock_xml
        FROM (
            SELECT CAST(target_data AS XML) AS TargetData
            FROM sys.dm_xe_session_targets st
            INNER JOIN sys.dm_xe_sessions s ON s.address = st.event_session_address
            WHERE s.name = 'DeadlockMonitor'
        ) AS Data
        CROSS APPLY TargetData.nodes('RingBufferTarget/event') AS XEventData(XEvent)
    ) deadlocks
    WHERE deadlock_time >= DATEADD(HOUR, -1, GETDATE());
    """

    LATCH_CONTENTION = """
    SET NOCOUNT ON;
    SELECT TOP 5
        @@SERVERNAME AS server_name, GETDATE() AS check_time,
        latch_class, waiting_requests_count,
        wait_time_ms / 1000.0 AS wait_time_sec,
        max_wait_time_ms / 1000.0 AS max_wait_time_sec,
        CASE
            WHEN wait_time_ms > 10000 THEN 'CRITICAL'
            WHEN wait_time_ms > 5000 THEN 'WARNING'
            ELSE 'NORMAL'
        END AS severity
    FROM sys.dm_os_latch_stats
    WHERE waiting_requests_count > 0
    ORDER BY wait_time_ms DESC;
    """


# ---------------------------------------------------------------------------
# SQL SERVER MONITOR
# ---------------------------------------------------------------------------
class SqlServerMonitor(object):
    """Monitorea un servidor SQL Server individual"""

    def __init__(self, server, query_timeout=30, logger=None):
        self.server = server
        self.query_timeout = query_timeout
        self.logger = logger or logging.getLogger(__name__)
        self._connection = None

    def _get_connection(self):
        """Obtiene o crea conexion ODBC"""
        if self._connection is None:
            self._connection = pyodbc.connect(
                self.server.connection_string,
                timeout=self.query_timeout,
            )
        return self._connection

    def _close_connection(self):
        """Cierra la conexion ODBC"""
        if self._connection:
            try:
                self._connection.close()
            except Exception:
                pass
            self._connection = None

    def _execute_query(self, query, query_name="query"):
        """
        Ejecuta una query con timeout estricto via threading.

        pyodbc.Cursor no tiene atributo timeout. El Query Timeout del
        connection string no es confiable en ODBC Driver 17/18 para queries
        que se cuelgan a nivel de red (el driver espera una respuesta que
        nunca llega). Se usa un thread interno con join(timeout) para
        garantizar que la query no bloquee mas de self.query_timeout segundos.
        """
        import threading

        result_container = {"rows": None, "error": None}

        def _run():
            try:
                conn = self._get_connection()
                cursor = conn.cursor()
                try:
                    cursor.execute(query)
                    columns = [desc[0] for desc in cursor.description]
                    rows = []
                    while True:
                        batch = cursor.fetchmany(100)
                        if not batch:
                            break
                        for row in batch:
                            rows.append({columns[i]: row[i] for i in range(len(columns))})
                    result_container["rows"] = rows
                except pyodbc.Error as e:
                    result_container["error"] = e
                finally:
                    try:
                        cursor.close()
                    except Exception:
                        pass
            except Exception as e:
                result_container["error"] = e

        t = threading.Thread(target=_run, daemon=True)
        t.start()
        t.join(timeout=self.query_timeout)

        if t.is_alive():
            # El thread sigue corriendo - la query está colgada.
            # No podemos matarlo pero lo dejamos como daemon para que
            # no bloquee el proceso. Cerramos la conexion para forzar
            # que el driver libere el socket en el proximo GC.
            self.logger.warning("  %s - %s: TIMEOUT (%ds) - query colgada, abortando" % (
                self.server.name, query_name, self.query_timeout))
            self._close_connection()
            return []

        error = result_container["error"]
        if error is not None:
            error_msg = str(error)
            if "Invalid column name" in error_msg:
                self.logger.warning("  %s - %s: Columna no encontrada: %s" % (
                    self.server.name, query_name, error_msg[:100]))
            else:
                self.logger.error("  %s - %s: Error en query: %s" % (
                    self.server.name, query_name, error_msg[:150]))
                self._close_connection()
            return []

        return result_container["rows"] or []

    def _send_metric(self, name, value, extra_labels=None):
        """Registra metrica en el snapshot para ObservableGauge"""
        labels = self.server.tags.copy()
        if extra_labels:
            labels.update(extra_labels)
        # Agregar el nombre de la metrica como atributo para diferenciar series
        labels["metric_name"] = name

        # FIX: la clave debe ser unica por servidor + metrica + labels extra.
        # Si se usa solo "name" como key, servidores procesados en paralelo
        # se sobreescriben mutuamente en el snapshot y solo llega el ultimo.
        key_parts = [name, self.server.name]
        if extra_labels:
            key_parts += ["%s=%s" % (k, v) for k, v in sorted(extra_labels.items())]
        key = "|".join(key_parts)

        METRICS_SNAPSHOT.update(key, value, labels)

    def check_engine_status(self):
        """CHECK 01: Verifica estado del motor"""
        try:
            results = self._execute_query(DMVQueries.ENGINE_STATUS, "EngineStatus")
            if not results:
                self._send_metric("sql_server_up", 0)
                return False

            row = results[0]
            self._send_metric("sql_server_up", 1)
            self._send_metric("sql_uptime_seconds", float(row.get("uptime_seconds", 0) or 0))

            self.logger.info("  %s - Engine: UP, Uptime=%ss" % (self.server.name, row.get("uptime_seconds", 0)))
            return True

        except Exception as e:
            self.logger.error("  %s - Engine Status ERROR: %s" % (self.server.name, str(e)))
            self._send_metric("sql_server_up", 0)
            self._close_connection()
            return False

    def check_error_log(self):
        """CHECK 02: Errores en error log"""
        try:
            results = self._execute_query(DMVQueries.ERROR_LOG, "ErrorLog")
            if not results:
                return

            row = results[0]
            self._send_metric("sql_errors_total_last_hour", float(row.get("total_errors_last_hour", 0) or 0))
            self._send_metric("sql_errors_severity_20_25", float(row.get("severity_20_25", 0) or 0))
            self._send_metric("sql_deadlocks_last_hour", float(row.get("deadlock_count", 0) or 0))
            self._send_metric("sql_backup_failures_last_hour", float(row.get("backup_failures", 0) or 0))
            self._send_metric("sql_io_errors_last_hour", float(row.get("io_errors", 0) or 0))
            self._send_metric("sql_login_failures_last_hour", float(row.get("login_failures", 0) or 0))

            self.logger.info("  %s - Errors: total=%s, sev20-25=%s, deadlocks=%s" % (
                self.server.name,
                row.get("total_errors_last_hour", 0),
                row.get("severity_20_25", 0),
                row.get("deadlock_count", 0)
            ))
        except Exception as e:
            self.logger.warning("  %s - Error Log: %s" % (self.server.name, str(e)))

    def check_blocking(self):
        """CHECK 03: Sesiones bloqueadas"""
        try:
            results = self._execute_query(DMVQueries.BLOCKING, "Blocking")
            if not results:
                return

            row = results[0]
            self._send_metric("sql_blocked_sessions", float(row.get("blocked_sessions_count", 0) or 0))
            self._send_metric("sql_blocking_sessions", float(row.get("blocking_sessions_count", 0) or 0))
            self._send_metric("sql_max_block_wait_sec", float(row.get("max_wait_time_sec", 0) or 0))
            self._send_metric("sql_critical_blocks", float(row.get("critical_blocks_count", 0) or 0))
            self._send_metric("sql_severe_blocks", float(row.get("severe_blocks_count", 0) or 0))

            self.logger.info("  %s - Blocking: blocked=%s, max_wait=%ss" % (
                self.server.name,
                row.get("blocked_sessions_count", 0),
                row.get("max_wait_time_sec", 0) or 0
            ))
        except Exception as e:
            self.logger.warning("  %s - Blocking: %s" % (self.server.name, str(e)))

    def check_wait_stats(self):
        """CHECK 04: Estadisticas de espera"""
        try:
            results = self._execute_query(DMVQueries.WAIT_STATS, "WaitStats")
            self.logger.info("  %s - Wait Stats: %d tipos encontrados, procesando..." % (self.server.name, len(results)))

            count = 0
            for row in results:
                count += 1
                extra_labels = {
                    "wait_type": str(row.get("wait_type", "unknown")),
                    "category": str(row.get("wait_category", "Other")),
                }
                self._send_metric("sql_wait_time_sec", float(row.get("wait_time_sec", 0) or 0), extra_labels)
                # sql_wait_tasks eliminado: redundante con wait_time_sec para alertas (-700 series)

                if count % 50 == 0:
                    self.logger.info("  %s - Wait Stats: %d/%d metricas agregadas..." % (
                        self.server.name, count, len(results)))

            self.logger.info("  %s - Wait Stats: %d tipos procesados" % (self.server.name, len(results)))
        except Exception as e:
            self.logger.warning("  %s - Wait Stats: %s" % (self.server.name, str(e)))

    def check_slow_queries(self):
        """CHECK 05: Queries lentos"""
        try:
            results = self._execute_query(DMVQueries.SLOW_QUERIES, "SlowQueries")
            count = 0
            for row in results:
                count += 1

                # Truncar texto a 200 chars para no exceder limites de labels de Prometheus/Grafana
                raw_stmt = str(row.get("statement_text") or row.get("query_text") or "N/A")
                stmt_short = raw_stmt[:200].strip() if raw_stmt else "N/A"

                extra_labels = {
                    "session_id":   str(row.get("session_id", "0")),
                    "database":     str(row.get("database_name", "unknown")),
                    "severity":     str(row.get("severity", "NORMAL")),
                    "login":        str(row.get("login_name", "unknown")),
                    "host":         str(row.get("host_name", "unknown")),
                    "program":      str(row.get("program_name", "unknown"))[:60],
                    "wait_type":    str(row.get("wait_type") or "NONE"),
                    "statement":    stmt_short,
                }
                self._send_metric("sql_slow_query_elapsed_sec",
                                  float(row.get("elapsed_time_sec", 0) or 0), extra_labels)
                self._send_metric("sql_slow_query_cpu_sec",
                                  float(row.get("cpu_time_sec", 0) or 0), extra_labels)
                self._send_metric("sql_slow_query_logical_reads",
                                  float(row.get("logical_reads", 0) or 0), extra_labels)

                # Log inmediato por query encontrada para visibilidad en consola/archivo
                self.logger.info(
                    "  %s - SlowQuery sid=%s db=%s elapsed=%.1fs cpu=%.1fs wait=%s login=%s stmt=%.120s" % (
                        self.server.name,
                        row.get("session_id", "?"),
                        row.get("database_name", "?"),
                        float(row.get("elapsed_time_sec", 0) or 0),
                        float(row.get("cpu_time_sec", 0) or 0),
                        row.get("wait_type") or "NONE",
                        row.get("login_name", "?"),
                        stmt_short,
                    )
                )

            self.logger.info("  %s - Slow Queries: %d encontrados" % (self.server.name, count))
        except Exception as e:
            self.logger.warning("  %s - Slow Queries: %s" % (self.server.name, str(e)))

    def check_resources(self):
        """CHECK 07: Uso de recursos"""
        try:
            results = self._execute_query(DMVQueries.RESOURCES, "Resources")
            if not results:
                return

            row = results[0]
            self._send_metric("sql_cpu_percent", float(row.get("avg_cpu_percent", 0) or 0))
            self._send_metric("sql_physical_memory_gb", float(row.get("physical_memory_gb", 0) or 0))
            self._send_metric("sql_committed_memory_gb", float(row.get("committed_memory_gb", 0) or 0))
            self._send_metric("sql_buffer_pool_used_gb", float(row.get("buffer_pool_used_gb", 0) or 0))
            self._send_metric("sql_active_connections", float(row.get("active_connections", 0) or 0))
            self._send_metric("sql_user_connections", float(row.get("user_connections", 0) or 0))
            self._send_metric("sql_total_connections", float(row.get("total_connections", 0) or 0))
            self._send_metric("sql_tempdb_user_gb", float(row.get("tempdb_user_objects_gb", 0) or 0))
            self._send_metric("sql_tempdb_internal_gb", float(row.get("tempdb_internal_objects_gb", 0) or 0))
            self._send_metric("sql_tempdb_version_gb", float(row.get("tempdb_version_store_gb", 0) or 0))

            self.logger.info("  %s - Resources: CPU=%s%%, Mem=%sGB, Conn=%s" % (
                self.server.name,
                row.get("avg_cpu_percent", 0),
                row.get("committed_memory_gb", 0),
                row.get("active_connections", 0)
            ))
        except Exception as e:
            self.logger.warning("  %s - Resources: %s" % (self.server.name, str(e)))

    def check_disk_space(self):
        """CHECK 08: Espacio en disco"""
        try:
            results = self._execute_query(DMVQueries.DISK_SPACE, "DiskSpace")
            for row in results:
                extra_labels = {
                    "database": str(row.get("database_name", "unknown")),
                    "file": str(row.get("logical_name", "unknown")),
                    "file_type": str(row.get("file_type", "unknown")),
                }
                self._send_metric("sql_disk_used_percent", float(row.get("used_percent", 0) or 0), extra_labels)
                # sql_disk_free_gb y sql_disk_size_gb eliminados: used_percent es suficiente para alertar (-896 series)

            self.logger.info("  %s - Disk Space: %d archivos procesados" % (self.server.name, len(results)))
        except Exception as e:
            self.logger.warning("  %s - Disk Space: %s" % (self.server.name, str(e)))

    def check_backups(self):
        """CHECK 09: Estado de backups"""
        try:
            results = self._execute_query(DMVQueries.BACKUPS, "Backups")
            for row in results:
                extra_labels = {"database": str(row.get("database_name", "unknown"))}
                self._send_metric("sql_hours_since_full_backup", float(row.get("hours_since_full_backup", 0) or 0), extra_labels)
                self._send_metric("sql_hours_since_log_backup", float(row.get("hours_since_log_backup", 0) or 0), extra_labels)

            self.logger.info("  %s - Backups: %d bases procesadas" % (self.server.name, len(results)))
        except Exception as e:
            self.logger.warning("  %s - Backups: %s" % (self.server.name, str(e)))

    def check_jobs(self):
        """CHECK 10: Estado de jobs"""
        try:
            results = self._execute_query(DMVQueries.JOBS, "Jobs")
            for row in results:
                extra_labels = {"job": str(row.get("job_name", "unknown"))}
                status_value = 0
                if row.get("last_run_status") == "Succeeded":
                    status_value = 1
                elif row.get("last_run_status") == "In Progress":
                    status_value = 2

                self._send_metric("sql_job_last_run_status", status_value, extra_labels)
                # sql_job_run_duration eliminado: sin umbral de alerta operacional útil (-510 series)

            self.logger.info("  %s - Jobs: %d jobs procesados" % (self.server.name, len(results)))
        except Exception as e:
            self.logger.warning("  %s - Jobs: %s" % (self.server.name, str(e)))

    def check_availability_groups(self):
        """CHECK 11: Availability Groups"""
        try:
            results = self._execute_query(DMVQueries.AVAILABILITY_GROUPS, "AG")
            for row in results:
                if row.get("ag_name") == "NO_AG":
                    continue
                extra_labels = {
                    "ag": str(row.get("ag_name", "unknown")),
                    "replica": str(row.get("replica_server_name", "unknown")),
                    "database": str(row.get("database_name", "unknown")),
                }
                self._send_metric("sql_ag_lag_seconds",  float(row.get("secondary_lag_seconds", 0) or 0), extra_labels)
                self._send_metric("sql_ag_log_queue_gb", float(row.get("log_send_queue_gb",    0) or 0), extra_labels)
                # sql_ag_redo_queue_gb eliminado: log_queue_gb ya mide el atraso de replicación (-480 series)

            self.logger.info("  %s - AG: %d replicas procesadas" % (self.server.name, len(results)))
        except Exception as e:
            self.logger.warning("  %s - AG: %s" % (self.server.name, str(e)))

    def check_deadlocks(self):
        """CHECK 14: Deadlocks"""
        try:
            results = self._execute_query(DMVQueries.DEADLOCKS, "Deadlocks")
            if results:
                self._send_metric("sql_deadlocks_last_hour", float(results[0].get("deadlock_count_last_hour", 0)))
                self.logger.info("  %s - Deadlocks: %s" % (self.server.name, results[0].get("deadlock_count_last_hour", 0)))
        except Exception as e:
            self.logger.warning("  %s - Deadlocks: %s" % (self.server.name, str(e)))

    def check_latch_contention(self):
        """CHECK 15: Contencion de latches"""
        try:
            results = self._execute_query(DMVQueries.LATCH_CONTENTION, "Latch")
            for row in results:
                extra_labels = {"latch_class": str(row.get("latch_class", "unknown"))}
                self._send_metric("sql_latch_wait_sec", float(row.get("wait_time_sec", 0) or 0), extra_labels)

            self.logger.info("  %s - Latch: %d clases procesadas" % (self.server.name, len(results)))
        except Exception as e:
            self.logger.warning("  %s - Latch: %s" % (self.server.name, str(e)))

    def monitor(self):
        """Ejecuta todas las verificaciones en secuencia"""
        self.logger.info("Monitoreando: %s (%s:%d)" % (self.server.name, self.server.ip, self.server.port))

        if not self.check_engine_status():
            return

        self.check_error_log()
        self.check_blocking()
        self.check_wait_stats()
        self.check_slow_queries()
        self.check_resources()
        self.check_disk_space()
        self.check_backups()
        self.check_jobs()
        self.check_availability_groups()
        self.check_deadlocks()
        self.check_latch_contention()

        self._close_connection()


# ---------------------------------------------------------------------------
# AGENTE PRINCIPAL
# ---------------------------------------------------------------------------
class SqlMonAgent(object):
    """Agente principal de monitoreo"""

    def __init__(self, config_path=None):
        self.config = self._load_config(config_path)
        self.logger = setup_logging(
            self.config["agent"]["log_file"],
            self.config["agent"]["log_level"],
        )
        self.servers = []
        self._shutdown = threading.Event()

    def _load_config(self, config_path):
        """Carga configuracion desde archivo o usa defaults"""
        config = {}
        for section, values in DEFAULT_CONFIG.items():
            config[section] = dict(values)

        if config_path and os.path.exists(config_path):
            parser = configparser.ConfigParser()
            parser.read(config_path)
            for section in parser.sections():
                if section in config:
                    config[section].update(dict(parser[section]))
                else:
                    config[section] = dict(parser[section])

        return config

    def load_servers(self):
        """Carga la lista de servidores desde archivo"""
        server_file = self.config.get("server_file", {}).get("path", "/opt/sqlmon-agent/servers.txt")
        self.logger.info("Cargando servidores desde: %s" % server_file)
        self.servers = ServerParser.parse(server_file)
        self.logger.info("%d servidores cargados" % len(self.servers))

    def _monitor_server(self, server):
        """Monitorea un servidor individual"""
        try:
            monitor = SqlServerMonitor(
                server=server,
                query_timeout=int(self.config["agent"]["query_timeout"]),
                logger=self.logger,
            )
            monitor.monitor()
        except Exception as e:
            self.logger.error("Error monitoreando %s: %s" % (server.name, str(e)))

    def _export_metrics(self):
        """
        Exporta metricas via OTLP y limpia el snapshot.
        Orden: count -> get_metrics_data (lee snapshot) -> export -> clear
        El clear() en finally garantiza que el proximo ciclo empiece limpio
        sin importar si el export fallo o no.
        """
        try:
            series_count = METRICS_SNAPSHOT.count()
            self.logger.info("[ENVIO] Exportando %d series de metricas..." % series_count)

            if series_count == 0:
                self.logger.warning("[ENVIO] Snapshot vacio, no hay metricas para exportar")
                return

            metrics = metric_reader.get_metrics_data()
            if metrics is None:
                self.logger.warning("[ENVIO] metric_reader devolvio None")
                return

            from opentelemetry.sdk.metrics.export import MetricExportResult
            result = otlp_exporter.export(metrics)

            if result == MetricExportResult.SUCCESS:
                self.logger.info("[ENVIO] OK - %d series enviadas a Grafana Cloud" % series_count)
            else:
                self.logger.error("[ENVIO] FALLO al exportar (resultado=%s)" % result)

        except Exception as e:
            self.logger.error("[ENVIO] Excepcion exportando metricas: %s" % str(e))
        finally:
            METRICS_SNAPSHOT.clear()
            self.logger.info("[ENVIO] Snapshot limpiado para proximo ciclo")

    def run_cycle(self):
        """Ejecuta un ciclo completo de monitoreo"""
        cycle_start = time.time()
        self.logger.info("=" * 60)
        self.logger.info("Iniciando ciclo de monitoreo (%d servidores)" % len(self.servers))
        self.logger.info("=" * 60)

        max_workers = int(self.config["agent"]["max_workers"])
        query_timeout = int(self.config["agent"]["query_timeout"])
        server_timeout = query_timeout * 15 + 30

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(self._monitor_server, server): server
                for server in self.servers
            }

            for future in as_completed(futures, timeout=server_timeout):
                server = futures[future]
                try:
                    future.result(timeout=5)
                except Exception as e:
                    self.logger.error("Excepcion en %s: %s" % (server.name, str(e)[:150]))

        self.logger.info("Monitoreo completado. Series en snapshot: %d" % METRICS_SNAPSHOT.count())
        self._export_metrics()

        cycle_duration = time.time() - cycle_start
        self.logger.info("Ciclo completado en %.1fs" % cycle_duration)

    def run(self):
        """Bucle principal del agente"""
        self.load_servers()
        interval = int(self.config["agent"]["interval_seconds"])

        self.logger.info("=" * 60)
        self.logger.info("SQL Server Monitoring Agent iniciado")
        self.logger.info("ODBC Driver: %s" % ODBC_DRIVER)
        self.logger.info("Intervalo: %ds" % interval)
        self.logger.info("OTLP Endpoint: %s" % OTLP_ENDPOINT)
        self.logger.info("Servidores: %d" % len(self.servers))
        self.logger.info("=" * 60)

        # Primer ciclo inmediato
        self.run_cycle()

        while not self._shutdown.is_set():
            self._shutdown.wait(timeout=interval)
            if not self._shutdown.is_set():
                try:
                    self.run_cycle()
                except Exception as e:
                    self.logger.error("Error en ciclo: %s" % str(e))

        self.logger.info("Agente detenido")

    def shutdown(self):
        """Señaliza shutdown graceful"""
        self.logger.info("Recibida señal de shutdown...")
        self._shutdown.set()


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="SQL Server Monitoring Agent - OTLP Grafana Cloud")
    parser.add_argument("-c", "--config", help="Ruta al archivo de configuracion")
    parser.add_argument("--servers", help="Ruta al archivo de servidores")
    args = parser.parse_args()

    agent = SqlMonAgent(config_path=args.config)

    if args.servers:
        agent.config["server_file"] = {"path": args.servers}

    def signal_handler(signum, frame):
        agent.shutdown()

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    try:
        agent.run()
    except KeyboardInterrupt:
        agent.shutdown()

    return 0


if __name__ == "__main__":
    sys.exit(main())
