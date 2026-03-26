"""
AppPulse 수집 대상 앱 목록 + 설정.
새 고객이 등록하면 여기에 앱을 추가합니다.
나중에 DB에서 읽어오도록 변경 가능.
"""

# 수집 대상 앱 (app_id → 메타 정보)
# 고객 앱 + 경쟁사를 같이 등록
APPS = {
    # Finance 카테고리
    "com.wealthfront": {
        "name": "Wealthfront",
        "category": "finance",
        "competitors": ["com.robinhood.android", "com.sofi.mobile", "piuk.blockchain.android"]
    },
    "com.robinhood.android": {
        "name": "Robinhood",
        "category": "finance",
        "competitors": []  # Wealthfront의 경쟁사로 이미 수집됨
    },
    "com.sofi.mobile": {
        "name": "SoFi",
        "category": "finance",
        "competitors": []
    },
    "piuk.blockchain.android": {
        "name": "Blockchain.com",
        "category": "finance",
        "competitors": []
    },

    # Travel 카테고리
    "com.hostelworld.app": {
        "name": "Hostelworld",
        "category": "travel",
        "competitors": ["com.kayak.android"]
    },
    "com.kayak.android": {
        "name": "KAYAK",
        "category": "travel",
        "competitors": []
    },

    # Productivity 카테고리
    "com.asana.app": {
        "name": "Asana",
        "category": "productivity",
        "competitors": []
    },
}

# 수집 설정
COLLECT_CONFIG = {
    "reviews_per_app": 200,       # 매일 수집할 리뷰 수 (최신순)
    "lang": "en",
    "country": "us",
    "sleep_between_apps": 2,      # 앱 간 대기 시간 (초)
    "max_retries": 3,
    "retry_delay": 5,
}

# 패널 생성 설정
PANEL_CONFIG = {
    "rolling_windows": [3, 7, 14, 30],   # 이동평균 윈도우
    "drop_threshold": 0.3,                # 급락 기준 (점)
    "min_reviews_per_day": 1,             # 일 최소 리뷰 수
}
