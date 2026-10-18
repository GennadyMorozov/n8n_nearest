from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import psycopg2
import os
from datetime import datetime, timezone

app = FastAPI(title="News Nearest Neighbor")

ALLOWED_COLUMNS = {
    "rss_simhash",
    "content_simhash"
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


def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
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

    try:
        target_hash = int(req.simhash, 16)
    except Exception:
        raise HTTPException(
            status_code=400,
            detail="Invalid simhash value"
        )

    best_id = 0
    best_distance = 65
    best_pub_date = None

    conn = get_connection()
    cur = conn.cursor()

    sql = f"""
	SELECT
  	  id,
 	   {req.simhash_column},
 	   pub_date
	FROM news
	WHERE pub_date >= %s::timestamptz - INTERVAL '30 days'
	  AND pub_date < %s::timestamptz
 	  AND id < %s
 	  AND {req.simhash_column} IS NOT NULL
	ORDER BY pub_date ASC, id ASC	
    """

    cur.execute(
        sql,
        (
            req.pub_date,
            req.pub_date,
            req.news_id
        )
    )

    rows = cur.fetchall()

    for news_id, simhash, pub_date in rows:

        if not simhash:
            continue

        try:
            candidate_hash = int(simhash, 16)
        except Exception:
            continue

        distance = (target_hash ^ candidate_hash).bit_count()

        if distance < best_distance:
            best_distance = distance
            best_id = news_id
            best_pub_date = pub_date

    cur.close()
    conn.close()

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
            if best_id != 0
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

    conn = get_connection()
    conn.close()

    return {"status": "db ok"}