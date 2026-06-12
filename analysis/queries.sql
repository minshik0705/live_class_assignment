-- =============================================================
-- Step 3. 데이터 집계 분석 쿼리
--
-- 실행 방법 (스택 기동 후):
--   docker compose exec postgres psql -U app -d eventdb -f - < analysis/queries.sql
-- 또는 psql 접속 후 개별 실행:
--   docker compose exec postgres psql -U app -d eventdb
--
-- 1~4번은 과제 예시 쿼리, 5~7번은 실제 크리에이터 대시보드 지표를
-- 재현하는 도메인 쿼리다. Grafana 대시보드의 패널들과 대응한다.
-- =============================================================

-- 1. 이벤트 타입별 발생 횟수
SELECT event_type,
       count(*)                                              AS cnt,
       round(100.0 * count(*) / sum(count(*)) OVER (), 1)    AS pct
FROM events
GROUP BY event_type
ORDER BY cnt DESC;

-- 2. 유저별 총 이벤트 수 상위 10 (헤비유저 skew 확인 — Zipf 분포 검증)
SELECT user_id, count(*) AS event_cnt
FROM events
GROUP BY user_id
ORDER BY event_cnt DESC
LIMIT 10;

-- 3. 시간대(0~23시)별 이벤트 추이 — 저녁 피크 확인
SELECT extract(hour FROM event_time) AS hour_of_day,
       count(*)                      AS cnt
FROM events
GROUP BY 1
ORDER BY 1;

-- 4. 에러 이벤트 비율 (일별)
SELECT date_trunc('day', event_time)::date                              AS day,
       count(*) FILTER (WHERE event_type = 'error')                     AS errors,
       count(*)                                                         AS total,
       round(100.0 * count(*) FILTER (WHERE event_type = 'error')
             / count(*), 2)                                             AS error_rate_pct
FROM events
GROUP BY 1
ORDER BY 1;

-- 5. 일별 거래액 — 결제 vs 환불 (크리에이터 대시보드의 거래액 지표 재현)
SELECT date_trunc('day', event_time)::date                              AS day,
       COALESCE(sum(amount) FILTER (WHERE event_type = 'purchase'), 0)  AS purchase_amount,
       COALESCE(sum(amount) FILTER (WHERE event_type = 'refund'), 0)    AS refund_amount,
       COALESCE(sum(CASE WHEN event_type = 'purchase' THEN amount
                         WHEN event_type = 'refund'   THEN -amount END), 0) AS net_amount
FROM events
GROUP BY 1
ORDER BY 1;

-- 6. 일별 신규 방문자 수 — 유저별 첫 page_view 시각으로 판별
SELECT date_trunc('day', first_seen)::date AS day,
       count(*)                            AS new_visitors
FROM (
    SELECT user_id, min(event_time) AS first_seen
    FROM events
    WHERE event_type = 'page_view'
    GROUP BY user_id
) t
GROUP BY 1
ORDER BY 1;

-- 7. 매출 상위 클래스 (팩트 ↔ 차원 테이블 조인, 스타 클래스 skew 확인)
SELECT c.course_id,
       c.title,
       c.category,
       count(*)        AS purchase_cnt,
       sum(e.amount)   AS revenue
FROM events e
JOIN courses c USING (course_id)
WHERE e.event_type = 'purchase'
GROUP BY c.course_id, c.title, c.category
ORDER BY revenue DESC
LIMIT 10;

-- (보너스) 파티션 프루닝 확인: 최근 2일 조건이면 해당 일자 파티션만 스캔한다
-- EXPLAIN (COSTS OFF)
-- SELECT count(*) FROM events
-- WHERE event_time >= now() - interval '2 days';
