"""이벤트 시뮬레이터 — '현재 시각 기록기'가 아니라 과거 N일치 역사의 백필 시뮬레이터.

현재 시각만 찍으면 모든 데이터가 당일 파티션 하나에 몰려 일 단위 파티셔닝,
7일 추이 차트, 파티션 단위 Parquet 아카이브가 전부 무의미해진다. 그래서
실행 시점 기준 [now - DAYS일, now] 상대 윈도우 안에 event_time을 배정한다
— 평가자가 언제 실행해도 Grafana 기본 시간창(최근 7일)에 데이터가 보인다.

현실성 장치 5가지:
  1. 퍼널 선후관계: page_view → signup → enroll → purchase → (일부) refund.
     세션 단위 상태로 순서를 보장한다. 독립 랜덤 추출은 "구매했는데 방문한
     적 없는 유저" 같은 모순된 로그를 만든다.
  2. skew 2축: 유저·클래스 모두 Zipf 분포 — 소수 헤비유저와 스타 클래스에
     트래픽·매출이 집중되는 멱법칙.
  3. 시간대 피크: 라이브 강의 도메인 특성상 저녁(19~22시) 집중.
  4. 에러율: 트래픽에 비례해 전체의 ~1.5%.
  5. 무료/유료 분기: 무료 클래스는 enroll만, 유료는 enroll+purchase.
     refund는 결제의 일부가 1~72시간 뒤에 발생 (전액 환불만 — 부분 환불은
     스코프에서 명시적으로 제외).
"""

import random
import uuid
from datetime import datetime, time as dtime, timedelta, timezone

KST = timezone(timedelta(hours=9))

# 0~23시 가중치 — 새벽 저점, 점심 완만, 저녁 19~22시 피크
HOUR_WEIGHTS = [
    1.0, 0.5, 0.3, 0.2, 0.2, 0.3,   # 00~05
    0.6, 1.0, 1.5, 2.0, 2.5, 2.8,   # 06~11
    3.0, 2.6, 2.5, 2.5, 2.6, 3.0,   # 12~17
    4.0, 6.0, 7.0, 7.0, 5.0, 2.5,   # 18~23 (저녁 피크)
]

CATEGORIES = ["개발", "디자인", "마케팅", "커리어", "재테크", "라이프"]
ERROR_CODES = [
    "500_INTERNAL", "502_BAD_GATEWAY", "404_NOT_FOUND",
    "PAYMENT_FAILED", "VIDEO_LOAD_FAILED",
]

# 퍼널 전환 확률
P_COURSE_PAGE = 0.6   # page_view 중 클래스 상세 비율 (나머지는 홈/검색 → course_id NULL)
P_ENROLL = 0.08       # 클래스 상세 조회 → 수강신청 전환율
P_REFUND = 0.07       # 결제 건 중 환불 비율
P_ERROR = 0.015       # 이벤트당 에러 동반 확률
P_NEW_USER = 0.3      # 윈도우 안에서 가입하는 신규 유저 비율

# events 테이블 컬럼 순서와 동일 (db.insert_events에서 사용)
EVENT_COLUMNS = (
    "event_id", "event_type", "event_time",
    "user_id", "session_id", "course_id", "amount", "error_code",
)


def zipf_weights(n: int, s: float = 1.1) -> list[float]:
    """k등 항목의 가중치 ∝ 1/k^s — 1등이 압도적이고 꼬리가 긴 멱법칙 분포."""
    return [1.0 / (k ** s) for k in range(1, n + 1)]


def make_courses(num_courses: int, rng: random.Random) -> list[dict]:
    """클래스 차원 데이터. 30%는 무료, 유료의 절반은 할인가 보유."""
    courses = []
    for i in range(1, num_courses + 1):
        is_free = rng.random() < 0.3
        price = 0 if is_free else rng.randrange(29000, 200000, 1000)
        discount_price = None
        if not is_free and rng.random() < 0.5:
            discount_price = int(price * rng.uniform(0.5, 0.9)) // 1000 * 1000
        courses.append({
            "course_id": f"c{i:04d}",
            "title": f"라이브 클래스 {i:04d}",
            "category": rng.choice(CATEGORIES),
            "price": price,
            "discount_price": discount_price,
            "is_free": is_free,
        })
    return courses


class Simulator:
    def __init__(self, now: datetime, *, days: int, num_users: int,
                 num_courses: int, sessions_per_day: int, seed: int):
        self.now = now.astimezone(KST)
        self.days = days
        self.sessions_per_day = sessions_per_day
        self.rng = random.Random(seed)

        self.courses = make_courses(num_courses, self.rng)
        self.course_weights = zipf_weights(num_courses)
        self.users = [f"u{i:05d}" for i in range(1, num_users + 1)]
        self.user_weights = zipf_weights(num_users)

        # 신규 유저는 윈도우 안 첫 세션에서 signup 이벤트를 남긴다.
        # 기존 유저(70%)는 윈도우 이전에 가입했다고 가정 → signup 없음.
        self.is_new_user = {u: self.rng.random() < P_NEW_USER for u in self.users}
        self.signed_up: set[str] = set()
        self.enrolled: set[tuple[str, str]] = set()  # (user, course) 중복 수강신청 방지

        self.events: list[tuple] = []

    # ---------- 이벤트 적재 ----------

    def emit(self, event_type: str, t: datetime, user: str, session: str,
             course_id: str | None = None, amount: int | None = None,
             error_code: str | None = None) -> None:
        event_id = str(uuid.UUID(int=self.rng.getrandbits(128)))
        self.events.append(
            (event_id, event_type, t, user, session, course_id, amount, error_code)
        )

    def maybe_error(self, t: datetime, user: str, session: str,
                    course_id: str | None) -> None:
        """트래픽 비례 에러 — 이벤트당 P_ERROR 확률로 같은 맥락에서 발생."""
        if self.rng.random() < P_ERROR:
            self.emit("error", t + timedelta(seconds=1), user, session,
                      course_id=course_id, error_code=self.rng.choice(ERROR_CODES))

    # ---------- 시간 배정 ----------

    def random_session_start(self) -> datetime | None:
        """윈도우 안의 세션 시작 시각. 일자는 균등, 시각은 HOUR_WEIGHTS 가중."""
        day_offset = self.rng.randrange(self.days)
        base_date = (self.now - timedelta(days=day_offset)).date()
        hour = self.rng.choices(range(24), weights=HOUR_WEIGHTS)[0]
        t = datetime.combine(base_date, dtime(hour=hour), tzinfo=KST) \
            + timedelta(minutes=self.rng.randrange(60), seconds=self.rng.randrange(60))
        if t > self.now:
            return None  # 오늘의 미래 시각 → 세션 스킵 ("오늘은 아직 진행 중")
        return t

    # ---------- 세션 시뮬레이션 ----------

    def run_session(self, user: str, start: datetime) -> None:
        session = f"s{uuid.UUID(int=self.rng.getrandbits(128)).hex[:16]}"
        t = start

        # 진입: 홈/검색 페이지 (course_id NULL)
        self.emit("page_view", t, user, session)
        self.maybe_error(t, user, session, None)

        # 신규 유저의 첫 세션이면 가입
        if self.is_new_user[user] and user not in self.signed_up:
            t += timedelta(seconds=self.rng.randrange(20, 120))
            self.emit("signup", t, user, session)
            self.signed_up.add(user)

        # 추가 페이지 조회 0~5회
        n_views = self.rng.choices([0, 1, 2, 3, 4, 5],
                                   weights=[15, 30, 25, 15, 10, 5])[0]
        for _ in range(n_views):
            t += timedelta(seconds=self.rng.randrange(10, 180))
            if self.rng.random() >= P_COURSE_PAGE:
                self.emit("page_view", t, user, session)  # 홈/검색 등
                self.maybe_error(t, user, session, None)
                continue

            # 클래스 상세 조회 (Zipf — 스타 클래스에 집중)
            course = self.rng.choices(self.courses, weights=self.course_weights)[0]
            cid = course["course_id"]
            self.emit("page_view", t, user, session, course_id=cid)
            self.maybe_error(t, user, session, cid)

            # 수강신청 전환
            if self.rng.random() < P_ENROLL and (user, cid) not in self.enrolled:
                t += timedelta(seconds=self.rng.randrange(15, 120))
                self.emit("enroll", t, user, session, course_id=cid)
                self.enrolled.add((user, cid))

                # 유료 클래스만 결제 동반 (무료는 enroll로 끝)
                if not course["is_free"]:
                    t += timedelta(seconds=self.rng.randrange(30, 300))
                    amount = course["discount_price"] or course["price"]
                    self.emit("purchase", t, user, session, course_id=cid, amount=amount)
                    self.maybe_error(t, user, session, cid)

                    # 환불: 결제 1~72시간 뒤, 윈도우(now) 안일 때만
                    if self.rng.random() < P_REFUND:
                        refund_t = t + timedelta(hours=self.rng.uniform(1, 72))
                        if refund_t <= self.now:
                            self.emit("refund", refund_t, user, session,
                                      course_id=cid, amount=amount)

    def run(self) -> list[tuple]:
        total_sessions = self.days * self.sessions_per_day
        for _ in range(total_sessions):
            start = self.random_session_start()
            if start is None:
                continue
            user = self.rng.choices(self.users, weights=self.user_weights)[0]
            self.run_session(user, start)

        # 실서비스 로그는 시간순으로 도착한다. 세션 단위로 생성한 백필 데이터를
        # event_time 정렬 후 적재해 시간순 도착을 재현한다 — 물리 저장 순서와
        # event_time의 상관관계가 BRIN 인덱스의 전제이기 때문.
        self.events.sort(key=lambda e: e[2])
        return self.events
