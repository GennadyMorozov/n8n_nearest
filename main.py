import logging
import os
import threading
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import psycopg2
from fastapi import FastAPI, HTTPException
from psycopg2 import pool
from pydantic import BaseModel

logger = logging.getLogger("uvicorn.error")

# 👈 Змінено на нові колонки з _int
ALLOWED_COLUMNS = {
    "rss_simhash_int",
    "content_simhash_int"
}


class Request(BaseModel):
    news_id: int
    simhash: str
    pub_date: str
    simhash_column: str


DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Пул соединений: без него каждый запрос = новый TCP-handshake + auth
_connection_pool = None
_pool_lock = threading.Lock()


def get_pool():
    """Ленивая инициализация пула соединений (1–20)."""
    global _connection_pool

    if _connection_pool is None:
        with _pool_lock:
            if _connection_pool is None:
                _connection_pool = pool.ThreadedConnectionPool(
                    1,
                    20,
                    host=DB_HOST,
                    port=DB_PORT,
                    dbname=DB_NAME,
                    user=DB_USER,
                    password=DB_PASSWORD
                )

    return _connection_pool


def get_connection():
    return get_pool().getconn()


def release_connection(conn, broken=False):
    """
    Вернуть соединение в пул.

    SELECT оставляет транзакцию открытой (idle in transaction): это держит
    snapshot и мешает VACUUM, поэтому перед возвратом делаем rollback.
    broken=True — соединение уже негодное, закрываем вместо возврата.
    """
    if conn is None:
        return

    try:
        if broken:
            get_pool().putconn(conn, close=True)
            return

        conn.rollback()
        get_pool().putconn(conn)

    except Exception as exc:
        logger.warning("release_connection failed: %s", exc)
        try:
            conn.close()
        except Exception:
            pass


def warm_up():
    """
    Прогрев кэша Postgres / ОС под реальные запросы /nearest.

    Без прогрева первый запрос после старта контейнера читает индекс
    с диска: 200–3300 ms вместо 6–30 ms. Здесь мы один раз проходим
    по тому же диапазону индекса (7 дней), что и в /nearest, но без
    выгрузки строк в Python (count(*) выполняется Index Only Scan).

    Ошибки не фатальны: сервис должен подняться даже если БД ещё не готова.
    """
    conn = None
    broken = False

    try:
        conn = get_connection()
        cur = conn.cursor()

        for column in sorted(ALLOWED_COLUMNS):
            cur.execute(
                f"""
                    SELECT count(*)
                    FROM news
                    WHERE pub_date >= now() - INTERVAL '7 days'
                      AND pub_date < now()
                      AND {column} IS NOT NULL
                """
            )
            cur.fetchone()

        cur.close()

    except Exception as exc:
        broken = True
        logger.warning("nearest warm-up failed: %s", exc)

    finally:
        release_connection(conn, broken=broken)


@asynccontextmanager
async def lifespan(app: FastAPI):
    warm_up()
    yield
    get_pool().closeall()


app = FastAPI(
    title="News Nearest Neighbor",
    lifespan=lifespan
)


@app.get("/")
def root():
    return {"status": "ok"}


@app.post("/nearest")
def nearest(req: Request):

    if req.simhash_column not in ALLOWED_COLUMNS:
        raise HTTPException(
            status_code=400,
            detail="Invalid simhash column"
        )

    # 👈 Тепер приймаємо десяткове число (рядок), без base=16
    try:
        target_hash = int(req.simhash)
    except Exception:
        raise HTTPException(
            status_code=400,
            detail="Invalid simhash value"
        )

    # None = кандидата не знайдено; inf = будь-яка відстань краща за стартову
    best_id = None
    best_distance = float("inf")
    best_pub_date = None

    # 👈 Використовуємо динамічну назву колонки (rss_simhash_int або content_simhash_int)
    sql = f"""
        SELECT
            id,
            {req.simhash_column},
            pub_date
        FROM news
        WHERE pub_date >= %s::timestamptz - INTERVAL '7 days'
          AND pub_date < %s::timestamptz
          AND id <> %s
          AND {req.simhash_column} IS NOT NULL
        ORDER BY pub_date ASC, id ASC
    """

    conn = None
    broken = False

    try:
        conn = get_connection()
        cur = conn.cursor()

        try:
            cur.execute(
                sql,
                (
                    req.pub_date,
                    req.pub_date,
                    req.news_id
                )
            )
            rows = cur.fetchall()

        finally:
            cur.close()

    except Exception as exc:
        broken = True
        raise HTTPException(
            status_code=500,
            detail=f"Database error: {exc}"
        )

    finally:
        release_connection(conn, broken=broken)

    for news_id, simhash, pub_date in rows:

        if not simhash:
            continue

        # 👈 Тепер конвертуємо з десяткового рядка в int, без base=16
        try:
            candidate_hash = int(simhash)
        except Exception:
            continue

        distance = (target_hash ^ candidate_hash).bit_count()

        if distance < best_distance:
            best_distance = distance
            best_id = news_id
            best_pub_date = pub_date

    time_diff_minutes = None

    if best_pub_date:

        current_date = datetime.fromisoformat(
            req.pub_date.replace("Z", "+00:00")
        )

        # Приводимо обидві дати до UTC
        if current_date.tzinfo is None:
            current_date = current_date.replace(
                tzinfo=timezone.utc
            )
        else:
            current_date = current_date.astimezone(
                timezone.utc
            )

        if best_pub_date.tzinfo is None:
            best_pub_date = best_pub_date.replace(
                tzinfo=timezone.utc
            )
        else:
            best_pub_date = best_pub_date.astimezone(
                timezone.utc
            )

        time_diff_minutes = int(
            abs(
                (
                    current_date - best_pub_date
                ).total_seconds()
            ) / 60
        )

    return {
        "nearest_id": best_id,
        "nearest_distance": (
            best_distance
            if best_id is not None
            else None
        ),
        "nearest_pub_date": (
            best_pub_date.isoformat()
            if best_pub_date
            else None
        ),
        "time_diff_minutes": time_diff_minutes,
        "checked": len(rows)
    }


@app.get("/dbtest")
def dbtest():

    conn = None
    broken = False

    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.close()

    except Exception as exc:
        broken = True
        raise HTTPException(
            status_code=503,
            detail=f"Database error: {exc}"
        )

    finally:
        release_connection(conn, broken=broken)

    return {"status": "db ok"}