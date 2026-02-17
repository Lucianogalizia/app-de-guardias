import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
import psycopg2.extras

ISO_DT = "%Y-%m-%d %H:%M:%S"


def utcnow_str() -> str:
    return datetime.utcnow().strftime(ISO_DT)


def get_setting(key: str, secrets: Any = None, default: Optional[str] = None) -> Optional[str]:
    # Prioridad: ENV -> secrets -> default
    if os.getenv(key) is not None:
        return os.getenv(key)
    if secrets is not None and key in secrets:
        return str(secrets.get(key))
    return default


def get_db_backend(secrets=None) -> str:
    return (get_setting("DB_BACKEND", secrets, "postgres") or "postgres").strip().lower()


def get_sqlite_path(secrets=None) -> str:
    return get_setting("DB_PATH", secrets, "data/app.db") or "data/app.db"


def pg_conn_params(secrets=None) -> Dict[str, Any]:
    # Cloud Run + Cloud SQL: DB_HOST suele ser /cloudsql/INSTANCE_CONNECTION_NAME
    host = get_setting("DB_HOST", secrets, None)
    port = int(get_setting("DB_PORT", secrets, "5432") or "5432")
    name = get_setting("DB_NAME", secrets, None)
    user = get_setting("DB_USER", secrets, None)
    password = get_setting("DB_PASSWORD", secrets, None)

    if not name or not user:
        raise RuntimeError(
            "Faltan variables DB_NAME/DB_USER. Configuralas en Cloud Run (o secrets locales)."
        )

    params = {"dbname": name, "user": user, "password": password, "port": port}
    if host:
        params["host"] = host
    return params


@contextmanager
def get_conn(secrets=None):
    backend = get_db_backend(secrets)
    if backend == "sqlite":
        path = get_sqlite_path(secrets)
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        conn = sqlite3.connect(path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        try:
            yield ("sqlite", conn)
            conn.commit()
        finally:
            conn.close()
    else:
        params = pg_conn_params(secrets)
        conn = psycopg2.connect(**params)
        try:
            yield ("postgres", conn)
            conn.commit()
        finally:
            conn.close()


def migrate(secrets=None) -> None:
    backend = get_db_backend(secrets)

    if backend == "sqlite":
        with get_conn(secrets) as (_, conn):
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS personal (
                    legajo TEXT PRIMARY KEY,
                    cuil TEXT NOT NULL,
                    nombre TEXT NOT NULL,
                    leader_legajo TEXT NOT NULL,
                    funcion TEXT,
                    origen TEXT,
                    lugar_trabajo TEXT,
                    extra_json TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS partes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    legajo TEXT NOT NULL,
                    periodo_yyyymm TEXT NOT NULL,
                    estado TEXT NOT NULL CHECK(estado IN ('BORRADOR','ENVIADO','APROBADO','RECHAZADO')),
                    submitted_at TEXT,
                    approved_at TEXT,
                    approved_by_legajo TEXT,
                    rejection_comment TEXT,
                    UNIQUE(legajo, periodo_yyyymm)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    legajo TEXT NOT NULL,
                    fecha TEXT NOT NULL,
                    tipo TEXT NOT NULL CHECK(tipo IN ('G','F','D','HO','HV','HE')),
                    valor_text TEXT,
                    valor_num REAL,
                    comentario TEXT
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_items_legajo_fecha ON items(legajo, fecha)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_partes_estado ON partes(estado)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_partes_legajo_periodo ON partes(legajo, periodo_yyyymm)")
        return

    # Postgres
    with get_conn(secrets) as (_, conn):
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS personal (
                legajo TEXT PRIMARY KEY,
                cuil TEXT NOT NULL,
                nombre TEXT NOT NULL,
                leader_legajo TEXT NOT NULL,
                funcion TEXT,
                origen TEXT,
                lugar_trabajo TEXT,
                extra_json TEXT
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS partes (
                id BIGSERIAL PRIMARY KEY,
                legajo TEXT NOT NULL,
                periodo_yyyymm TEXT NOT NULL,
                estado TEXT NOT NULL CHECK (estado IN ('BORRADOR','ENVIADO','APROBADO','RECHAZADO')),
                submitted_at TEXT,
                approved_at TEXT,
                approved_by_legajo TEXT,
                rejection_comment TEXT,
                UNIQUE (legajo, periodo_yyyymm)
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS items (
                id BIGSERIAL PRIMARY KEY,
                legajo TEXT NOT NULL,
                fecha TEXT NOT NULL,
                tipo TEXT NOT NULL CHECK (tipo IN ('G','F','D','HO','HV','HE')),
                valor_text TEXT,
                valor_num DOUBLE PRECISION,
                comentario TEXT
            );
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_items_legajo_fecha ON items(legajo, fecha);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_partes_estado ON partes(estado);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_partes_legajo_periodo ON partes(legajo, periodo_yyyymm);")
        cur.close()


# ---------- Personal ----------

def upsert_personal_rows(secrets, rows: List[Dict[str, Any]]) -> Tuple[int, int]:
    inserted = 0
    updated = 0

    backend = get_db_backend(secrets)
    with get_conn(secrets) as (_, conn):
        if backend == "sqlite":
            for r in rows:
                legajo = str(r["legajo"]).strip()
                cur = conn.execute("SELECT legajo FROM personal WHERE legajo = ?", (legajo,))
                exists = cur.fetchone() is not None

                conn.execute(
                    """
                    INSERT INTO personal (legajo, cuil, nombre, leader_legajo, funcion, origen, lugar_trabajo, extra_json)
                    VALUES (?,?,?,?,?,?,?,?)
                    ON CONFLICT(legajo) DO UPDATE SET
                        cuil=excluded.cuil,
                        nombre=excluded.nombre,
                        leader_legajo=excluded.leader_legajo,
                        funcion=excluded.funcion,
                        origen=excluded.origen,
                        lugar_trabajo=excluded.lugar_trabajo,
                        extra_json=excluded.extra_json
                    """,
                    (
                        legajo,
                        str(r["cuil"]).strip(),
                        str(r["nombre"]).strip(),
                        str(r["leader_legajo"]).strip(),
                        r.get("funcion"),
                        r.get("origen"),
                        r.get("lugar_trabajo"),
                        r.get("extra_json"),
                    ),
                )
                updated += 1 if exists else 0
                inserted += 0 if exists else 1
            return inserted, updated

        # Postgres
        cur = conn.cursor()
        for r in rows:
            legajo = str(r["legajo"]).strip()
            cur.execute("SELECT legajo FROM personal WHERE legajo=%s", (legajo,))
            exists = cur.fetchone() is not None

            cur.execute(
                """
                INSERT INTO personal (legajo, cuil, nombre, leader_legajo, funcion, origen, lugar_trabajo, extra_json)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (legajo) DO UPDATE SET
                    cuil=EXCLUDED.cuil,
                    nombre=EXCLUDED.nombre,
                    leader_legajo=EXCLUDED.leader_legajo,
                    funcion=EXCLUDED.funcion,
                    origen=EXCLUDED.origen,
                    lugar_trabajo=EXCLUDED.lugar_trabajo,
                    extra_json=EXCLUDED.extra_json
                """,
                (
                    legajo,
                    str(r["cuil"]).strip(),
                    str(r["nombre"]).strip(),
                    str(r["leader_legajo"]).strip(),
                    r.get("funcion"),
                    r.get("origen"),
                    r.get("lugar_trabajo"),
                    r.get("extra_json"),
                ),
            )
            updated += 1 if exists else 0
            inserted += 0 if exists else 1
        cur.close()
        return inserted, updated


def list_personal(secrets) -> List[Dict[str, Any]]:
    backend = get_db_backend(secrets)
    with get_conn(secrets) as (_, conn):
        if backend == "sqlite":
            cur = conn.execute(
                """
                SELECT legajo, cuil, nombre, leader_legajo, funcion, origen, lugar_trabajo
                FROM personal
                ORDER BY nombre
                """
            )
            return [dict(r) for r in cur.fetchall()]

        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """
            SELECT legajo, cuil, nombre, leader_legajo, funcion, origen, lugar_trabajo
            FROM personal
            ORDER BY nombre
            """
        )
        rows = cur.fetchall()
        cur.close()
        return rows


def get_person_by_legajo(secrets, legajo: str) -> Optional[Dict[str, Any]]:
    legajo = str(legajo).strip()
    backend = get_db_backend(secrets)
    with get_conn(secrets) as (_, conn):
        if backend == "sqlite":
            cur = conn.execute(
                """
                SELECT legajo, cuil, nombre, leader_legajo, funcion, origen, lugar_trabajo
                FROM personal WHERE legajo = ?
                """,
                (legajo,),
            )
            r = cur.fetchone()
            return dict(r) if r else None

        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """
            SELECT legajo, cuil, nombre, leader_legajo, funcion, origen, lugar_trabajo
            FROM personal WHERE legajo = %s
            """,
            (legajo,),
        )
        r = cur.fetchone()
        cur.close()
        return r


def leader_set_in_db(secrets) -> List[str]:
    backend = get_db_backend(secrets)
    with get_conn(secrets) as (_, conn):
        if backend == "sqlite":
            cur = conn.execute(
                "SELECT DISTINCT leader_legajo AS l FROM personal WHERE leader_legajo IS NOT NULL AND leader_legajo <> ''"
            )
            return sorted([str(r["l"]).strip() for r in cur.fetchall()])

        cur = conn.cursor()
        cur.execute(
            "SELECT DISTINCT leader_legajo FROM personal WHERE leader_legajo IS NOT NULL AND leader_legajo <> ''"
        )
        rows = cur.fetchall()
        cur.close()
        return sorted([str(r[0]).strip() for r in rows])


# ---------- Partes ----------

def get_or_create_parte(secrets, legajo: str, periodo_yyyymm: str) -> Dict[str, Any]:
    legajo = str(legajo).strip()
    periodo_yyyymm = str(periodo_yyyymm).strip()
    backend = get_db_backend(secrets)

    with get_conn(secrets) as (_, conn):
        if backend == "sqlite":
            conn.execute(
                """
                INSERT INTO partes (legajo, periodo_yyyymm, estado)
                VALUES (?, ?, 'BORRADOR')
                ON CONFLICT(legajo, periodo_yyyymm) DO NOTHING
                """,
                (legajo, periodo_yyyymm),
            )
            cur = conn.execute(
                "SELECT * FROM partes WHERE legajo = ? AND periodo_yyyymm = ?",
                (legajo, periodo_yyyymm),
            )
            return dict(cur.fetchone())

        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """
            INSERT INTO partes (legajo, periodo_yyyymm, estado)
            VALUES (%s, %s, 'BORRADOR')
            ON CONFLICT (legajo, periodo_yyyymm) DO NOTHING
            """,
            (legajo, periodo_yyyymm),
        )
        cur.execute(
            "SELECT * FROM partes WHERE legajo=%s AND periodo_yyyymm=%s",
            (legajo, periodo_yyyymm),
        )
        r = cur.fetchone()
        cur.close()
        return r


def update_parte_estado(
    secrets,
    legajo: str,
    periodo_yyyymm: str,
    nuevo_estado: str,
    submitted_at: Optional[str] = None,
    approved_at: Optional[str] = None,
    approved_by_legajo: Optional[str] = None,
    rejection_comment: Optional[str] = None,
) -> None:
    legajo = str(legajo).strip()
    periodo_yyyymm = str(periodo_yyyymm).strip()
    backend = get_db_backend(secrets)

    with get_conn(secrets) as (_, conn):
        if backend == "sqlite":
            conn.execute(
                """
                UPDATE partes
                SET estado = ?,
                    submitted_at = COALESCE(?, submitted_at),
                    approved_at = COALESCE(?, approved_at),
                    approved_by_legajo = COALESCE(?, approved_by_legajo),
                    rejection_comment = ?
                WHERE legajo = ? AND periodo_yyyymm = ?
                """,
                (nuevo_estado, submitted_at, approved_at, approved_by_legajo, rejection_comment, legajo, periodo_yyyymm),
            )
            return

        cur = conn.cursor()
        cur.execute(
            """
            UPDATE partes SET
                estado = %s,
                submitted_at = COALESCE(%s, submitted_at),
                approved_at = COALESCE(%s, approved_at),
                approved_by_legajo = COALESCE(%s, approved_by_legajo),
                rejection_comment = %s
            WHERE legajo = %s AND periodo_yyyymm = %s
            """,
            (nuevo_estado, submitted_at, approved_at, approved_by_legajo, rejection_comment, legajo, periodo_yyyymm),
        )
        cur.close()


def list_pendientes_para_lider(secrets, leader_legajo: str) -> List[Dict[str, Any]]:
    leader_legajo = str(leader_legajo).strip()
    backend = get_db_backend(secrets)

    with get_conn(secrets) as (_, conn):
        if backend == "sqlite":
            cur = conn.execute(
                """
                SELECT p.id, p.legajo, per.nombre, p.periodo_yyyymm, p.estado, p.submitted_at
                FROM partes p
                JOIN personal per ON per.legajo = p.legajo
                WHERE p.estado = 'ENVIADO' AND per.leader_legajo = ?
                ORDER BY p.submitted_at DESC, per.nombre
                """,
                (leader_legajo,),
            )
            return [dict(r) for r in cur.fetchall()]

        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """
            SELECT p.id, p.legajo, per.nombre, p.periodo_yyyymm, p.estado, p.submitted_at
            FROM partes p
            JOIN personal per ON per.legajo = p.legajo
            WHERE p.estado = 'ENVIADO' AND per.leader_legajo = %s
            ORDER BY p.submitted_at DESC NULLS LAST, per.nombre
            """,
            (leader_legajo,),
        )
        rows = cur.fetchall()
        cur.close()
        return rows


def get_parte(secrets, legajo: str, periodo_yyyymm: str) -> Optional[Dict[str, Any]]:
    legajo = str(legajo).strip()
    periodo_yyyymm = str(periodo_yyyymm).strip()
    backend = get_db_backend(secrets)

    with get_conn(secrets) as (_, conn):
        if backend == "sqlite":
            cur = conn.execute(
                "SELECT * FROM partes WHERE legajo=? AND periodo_yyyymm=?",
                (legajo, periodo_yyyymm),
            )
            r = cur.fetchone()
            return dict(r) if r else None

        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            "SELECT * FROM partes WHERE legajo=%s AND periodo_yyyymm=%s",
            (legajo, periodo_yyyymm),
        )
        r = cur.fetchone()
        cur.close()
        return r


# ---------- Items ----------

def delete_items_for_dates(secrets, legajo: str, fechas_iso: List[str]) -> None:
    if not fechas_iso:
        return
    legajo = str(legajo).strip()
    backend = get_db_backend(secrets)

    with get_conn(secrets) as (_, conn):
        if backend == "sqlite":
            q_marks = ",".join(["?"] * len(fechas_iso))
            conn.execute(
                f"DELETE FROM items WHERE legajo = ? AND fecha IN ({q_marks})",
                (legajo, *fechas_iso),
            )
            return

        cur = conn.cursor()
        cur.execute(
            "DELETE FROM items WHERE legajo=%s AND fecha = ANY(%s)",
            (legajo, fechas_iso),
        )
        cur.close()


def insert_items(secrets, items: List[Dict[str, Any]]) -> None:
    if not items:
        return
    backend = get_db_backend(secrets)

    with get_conn(secrets) as (_, conn):
        if backend == "sqlite":
            conn.executemany(
                """
                INSERT INTO items (legajo, fecha, tipo, valor_text, valor_num, comentario)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        str(it["legajo"]).strip(),
                        str(it["fecha"]).strip(),
                        str(it["tipo"]).strip(),
                        it.get("valor_text"),
                        it.get("valor_num"),
                        it.get("comentario"),
                    )
                    for it in items
                ],
            )
            return

        cur = conn.cursor()
        psycopg2.extras.execute_batch(
            cur,
            """
            INSERT INTO items (legajo, fecha, tipo, valor_text, valor_num, comentario)
            VALUES (%s,%s,%s,%s,%s,%s)
            """,
            [
                (
                    str(it["legajo"]).strip(),
                    str(it["fecha"]).strip(),
                    str(it["tipo"]).strip(),
                    it.get("valor_text"),
                    it.get("valor_num"),
                    it.get("comentario"),
                )
                for it in items
            ],
            page_size=500,
        )
        cur.close()


def list_items_for_period(secrets, legajo: str, fecha_desde_iso: str, fecha_hasta_iso: str) -> List[Dict[str, Any]]:
    legajo = str(legajo).strip()
    backend = get_db_backend(secrets)

    with get_conn(secrets) as (_, conn):
        if backend == "sqlite":
            cur = conn.execute(
                """
                SELECT * FROM items
                WHERE legajo = ? AND fecha >= ? AND fecha <= ?
                ORDER BY fecha, tipo
                """,
                (legajo, fecha_desde_iso, fecha_hasta_iso),
            )
            return [dict(r) for r in cur.fetchall()]

        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """
            SELECT * FROM items
            WHERE legajo=%s AND fecha >= %s AND fecha <= %s
            ORDER BY fecha, tipo
            """,
            (legajo, fecha_desde_iso, fecha_hasta_iso),
        )
        rows = cur.fetchall()
        cur.close()
        return rows
