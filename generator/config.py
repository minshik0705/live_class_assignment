"""환경변수 기반 설정 — docker-compose.yml에서 조절한다."""

import os


def _int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


PG_DSN = os.getenv("PG_DSN", "postgresql://app:app@localhost:5432/eventdb")

DAYS = _int("GEN_DAYS", 7)                          # 백필할 과거 일수
NUM_USERS = _int("GEN_USERS", 2000)                 # 유저 풀 크기
NUM_COURSES = _int("GEN_COURSES", 60)               # 클래스 수
SESSIONS_PER_DAY = _int("GEN_SESSIONS_PER_DAY", 3000)  # 하루 세션 수 (이벤트 수의 ~4배)
SEED = _int("GEN_SEED", 42)                         # 재현 가능한 생성을 위한 시드

PARQUET_DIR = os.getenv("PARQUET_DIR", "./parquet_output")
