"""PostgreSQL 연결·파티션 생성·적재."""

import time
from datetime import date, timedelta

import psycopg2
from psycopg2.extras import execute_values


def connect_with_retry(dsn: str, timeout_sec: int = 60):
    """compose 기동 직후 DB가 준비될 때까지 재시도."""
    deadline = time.monotonic() + timeout_sec
    while True:
        try:
            conn = psycopg2.connect(dsn)
            conn.autocommit = False
            return conn
        #DB 부팅 또는 네트워크 준비 부족으로 에러가 발생할 경우
        except psycopg2.OperationalError:
            if time.monotonic() > deadline:
                raise
            time.sleep(1)


def events_already_loaded(conn) -> bool:
    """재실행(compose restart) 시 중복 적재 방지 가드."""
    with conn.cursor() as cur:
        # events 테이블 데이터가 단 하나라도 있는지 조회
        cur.execute("SELECT EXISTS (SELECT 1 FROM events)")
        return cur.fetchone()[0]


def create_daily_partitions(conn, first_day: date, last_day: date) -> list[str]:
    """[first_day, last_day] 구간의 일별 파티션 생성.

    파티션 경계는 DB 타임존(Asia/Seoul, 01_schema.sql에서 고정) 기준
    자정으로 해석된다 — KST 하루 = 파티션 하나.
    """
    names = []
    with conn.cursor() as cur:
        d = first_day
        while d <= last_day:
            name = f"events_{d:%Y%m%d}"
            cur.execute(
                f"CREATE TABLE IF NOT EXISTS {name} PARTITION OF events "
                f"FOR VALUES FROM (%s) TO (%s)",
                (d.isoformat(), (d + timedelta(days=1)).isoformat()),
            )
            names.append(name)
            d += timedelta(days=1)
    return names


def insert_courses(conn, courses: list[dict]) -> None:
    rows = [
        (c["course_id"], c["title"], c["category"],
         c["price"], c["discount_price"], c["is_free"])
        for c in courses
    ]
    with conn.cursor() as cur:
        # 충돌 시 무시하고 넘어가라 (course_id는 PK라 중복될 수 없는데 만약 발생하면 무시)
        execute_values(
            cur,
            "INSERT INTO courses (course_id, title, category, price, discount_price, is_free) "
            "VALUES %s ON CONFLICT (course_id) DO NOTHING",
            rows,
        )


def insert_events(conn, events: list[tuple]) -> None:
    with conn.cursor() as cur:
        execute_values(
            cur,
            "INSERT INTO events (event_id, event_type, event_time, user_id, "
            "session_id, course_id, amount, error_code) VALUES %s",
            events,
            page_size=2000,
        )
        cur.execute("ANALYZE events")  # 적재 직후 통계 갱신 → 플래너가 바로 정확히 동작
