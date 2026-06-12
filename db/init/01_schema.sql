-- =============================================================
-- 이벤트 로그 파이프라인 스키마
--
-- 설계 요약 (상세 근거는 README "스키마 설명" 참조):
--   - events: 단일 와이드 팩트 테이블. 이벤트 타입별 테이블 분리 대신
--     한 테이블 + nullable 타입 전용 컬럼 (쿼리가 타입을 가로지르기 때문)
--   - 일 단위 RANGE 파티셔닝: 쿼리가 시간 범위 기반 → 파티션 프루닝,
--     오래된 파티션 단위 Parquet 아카이브 후 정리 용이
--   - 일별 파티션은 생성기(generator)가 데이터 날짜 범위에 맞춰 동적 생성
-- =============================================================

-- 파티션 경계가 KST 자정에 정렬되도록 DB 기본 타임존을 고정
ALTER DATABASE eventdb SET timezone TO 'Asia/Seoul';

-- 차원 테이블: 클래스(강의) 속성
CREATE TABLE courses (
    course_id      text    PRIMARY KEY,
    title          text    NOT NULL,
    category       text    NOT NULL,
    price          integer NOT NULL,          -- 원화, 0이면 무료 클래스
    discount_price integer,                   -- 할인가 (없으면 NULL)
    is_free        boolean NOT NULL
);

-- 팩트 테이블: 이벤트 로그 (append-only)
CREATE TABLE events (
    event_id   uuid        NOT NULL,
    event_type text        NOT NULL,
    event_time timestamptz NOT NULL,
    user_id    text        NOT NULL,
    session_id text        NOT NULL,
    course_id  text,                          -- page_view(클래스 상세)/enroll/purchase/refund 만 값
    amount     integer,                       -- purchase/refund 만 값 (원화)
    error_code text,                          -- error 만 값

    CONSTRAINT valid_event_type CHECK (
        event_type IN ('page_view', 'signup', 'enroll', 'purchase', 'refund', 'error')
    )
) PARTITION BY RANGE (event_time);

-- PK를 일부러 걸지 않았다: append-only 로그에 point lookup이 없고,
-- 파티션 테이블의 PK는 (event_id, event_time) 복합이 강제되는데
-- 그 거대한 B-tree가 어떤 쿼리에도 쓰이지 않기 때문 (README 참조).
-- course_id FK도 같은 이유(대량 적재 시 행마다 검증 비용)로 생략.

-- 인덱스 전략: 쿼리 축 2개에 인덱스 2개
-- ① 시간축 — BRIN: 시간순 append-only 적재라 물리 순서↔값 상관관계 성립.
--    블록 묶음별 min/max 요약만 저장해 B-tree 대비 수백 배 작다.
CREATE INDEX idx_events_time_brin ON events USING brin (event_time);

-- ② 유저축 — B-tree 복합: 한 유저의 이벤트는 시간축 전체에 흩어져
--    BRIN 전제가 무너짐. (user_id, event_time) 순서 덕에 유저를 찾으면
--    그 유저의 이벤트가 이미 시간순 → 퍼널/세션 분석에 맞는 형태.
CREATE INDEX idx_events_user_time ON events (user_id, event_time);

-- event_type 인덱스는 걸지 않는다: 카디널리티 6이라 선택도가 없어
-- 인덱스보다 파티션 프루닝 + 순차 스캔이 낫다.
