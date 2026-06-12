"""파티션 단위 Parquet + zstd 아카이브 내보내기.

장기 보관 레이어: 오래된 일별 파티션을 컬럼 저장 포맷(Parquet)으로 내보낸다.
우리 데이터는 구조적으로 희소(amount ~95% NULL, error_code ~98% NULL)하고
저카디널리티(event_type 6종)라 컬럼 저장에서 압축 효율이 극대화된다
— NULL 런은 RLE, 반복 값은 dictionary encoding, 그 위에 zstd.

내보내기 후 PG 파티션 크기와 Parquet 파일 크기를 실측 비교해 리포트를 남긴다.
(데모를 위해 파티션은 DROP하지 않는다 — 운영이라면 내보내기 검증 후
 DETACH PARTITION → DROP으로 핫 저장소를 가볍게 유지한다.)
"""

import os

import pyarrow as pa
import pyarrow.parquet as pq

EXPORT_COLUMNS = [
    "event_id", "event_type", "event_time",
    "user_id", "session_id", "course_id", "amount", "error_code",
]


def list_partitions(conn) -> list[tuple[str, int]]:
    """(파티션명, PG 크기 bytes) 목록 — 인덱스·TOAST 포함 전체 크기."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT inhrelid::regclass::text AS partition_name,
                   pg_total_relation_size(inhrelid) AS total_bytes
            FROM pg_inherits
            WHERE inhparent = 'events'::regclass
            ORDER BY 1
        """)
        return cur.fetchall()


def export_partition(conn, partition_name: str, out_dir: str) -> str:
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT {', '.join(EXPORT_COLUMNS)} FROM {partition_name} "
            f"ORDER BY event_time"
        )
        rows = cur.fetchall()

    columns = list(zip(*rows)) if rows else [[] for _ in EXPORT_COLUMNS]
    table = pa.table(
        {name: list(col) for name, col in zip(EXPORT_COLUMNS, columns)},
        schema=pa.schema([
            ("event_id", pa.string()),
            ("event_type", pa.string()),
            ("event_time", pa.timestamp("us", tz="Asia/Seoul")),
            ("user_id", pa.string()),
            ("session_id", pa.string()),
            ("course_id", pa.string()),
            ("amount", pa.int32()),
            ("error_code", pa.string()),
        ]),
    )
    path = os.path.join(out_dir, f"{partition_name}.parquet")
    # use_dictionary 기본 True — 저카디널리티 컬럼(event_type 등)은 사전 인코딩
    pq.write_table(table, path, compression="zstd")
    return path


def human(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:,.1f} {unit}" if unit != "B" else f"{n:,} {unit}"
        n /= 1024
    return f"{n:,.1f} TB"


def run(conn, out_dir: str) -> None:
    os.makedirs(out_dir, exist_ok=True)
    partitions = list_partitions(conn)

    lines = [
        "PG 파티션 크기 vs Parquet+zstd 파일 크기 실측 비교",
        "(PG 크기는 인덱스 포함 전체, Parquet은 단일 파일)",
        "",
        f"{'파티션':<20} {'PG 크기':>12} {'Parquet':>12} {'압축비':>8}",
        "-" * 56,
    ]
    total_pg, total_pq = 0, 0
    for name, pg_bytes in partitions:
        path = export_partition(conn, name, out_dir)
        pq_bytes = os.path.getsize(path)
        total_pg += pg_bytes
        total_pq += pq_bytes
        ratio = pg_bytes / pq_bytes if pq_bytes else 0
        lines.append(f"{name:<20} {human(pg_bytes):>12} {human(pq_bytes):>12} {ratio:>7.1f}x")

    if total_pq:
        lines.append("-" * 56)
        lines.append(
            f"{'합계':<20} {human(total_pg):>12} {human(total_pq):>12} "
            f"{total_pg / total_pq:>7.1f}x"
        )

    report = "\n".join(lines)
    print(report, flush=True)
    with open(os.path.join(out_dir, "size_report.txt"), "w") as f:
        f.write(report + "\n")
