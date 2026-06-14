"""파이프라인 진입점: 생성 → 적재 → 아카이브.

`docker compose up` 시 DB가 준비되면 자동 실행된다:
  1. [now - GEN_DAYS일, now] 윈도우의 이벤트 백필 시뮬레이션
  2. 일별 파티션 생성 후 적재 (재실행 시 중복 적재 방지 가드)
  3. 전체 파티션을 Parquet+zstd로 내보내고 크기 비교 리포트 출력
"""

from datetime import datetime

import config
import db
import export_parquet
from simulate import KST, Simulator

# DB에 적재한 데이터 정보 요약 출력
def print_summary(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT event_type, count(*),
                   round(100.0 * count(*) / sum(count(*)) OVER (), 1)
            FROM events GROUP BY 1 ORDER BY 2 DESC
        """)
        rows = cur.fetchall()
        cur.execute("SELECT count(*), min(event_time), max(event_time) FROM events")
        total, t_min, t_max = cur.fetchone()

    print(f"\n적재 완료: {total:,}건  ({t_min:%Y-%m-%d %H:%M} ~ {t_max:%Y-%m-%d %H:%M} KST)")
    for event_type, cnt, pct in rows:
        print(f"  {event_type:<10} {cnt:>8,}건  ({pct}%)")
    print(flush=True)


def main() -> None:
    # DB 연결, split('@)[-1]은 비번 노출을 방지
    print(f"DB 연결 대기: {config.PG_DSN.split('@')[-1]}", flush=True)
    conn = db.connect_with_retry(config.PG_DSN)

    # 데이터 적재
    if db.events_already_loaded(conn):
        print("events 테이블에 데이터가 이미 있음 — 생성·적재 스킵 (재실행 가드)", flush=True)
    else:
        now = datetime.now(KST)
        print(f"백필 시뮬레이션: [{now:%Y-%m-%d %H:%M} 기준 과거 {config.DAYS}일] "
              f"유저 {config.NUM_USERS:,} / 클래스 {config.NUM_COURSES} / "
              f"세션 {config.DAYS * config.SESSIONS_PER_DAY:,}", flush=True)
        # 이벤트 생성
        sim = Simulator(
            now,
            days=config.DAYS,
            num_users=config.NUM_USERS,
            num_courses=config.NUM_COURSES,
            sessions_per_day=config.SESSIONS_PER_DAY,
            seed=config.SEED,
        )
        events = sim.run()

        first_day = events[0][2].astimezone(KST).date()
        last_day = events[-1][2].astimezone(KST).date()
        partitions = db.create_daily_partitions(conn, first_day, last_day)
        print(f"일별 파티션 {len(partitions)}개 생성: {partitions[0]} ~ {partitions[-1]}", flush=True)

        db.insert_courses(conn, sim.courses)
        db.insert_events(conn, events)
        conn.commit()
        print_summary(conn)
    
    export_parquet.run(conn, config.PARQUET_DIR)
    conn.close()
    print("\n파이프라인 완료. Grafana: http://localhost:3000", flush=True)


if __name__ == "__main__":
    main()
