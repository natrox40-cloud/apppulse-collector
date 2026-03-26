"""
Supabase 연결 + 테이블 조작.

필요한 테이블 (Supabase SQL Editor에서 생성):

-- 1. 원본 리뷰
CREATE TABLE reviews (
    id BIGSERIAL PRIMARY KEY,
    app_id TEXT NOT NULL,
    review_id TEXT,
    score INTEGER NOT NULL,
    text TEXT,
    thumbs_up INTEGER DEFAULT 0,
    review_date TIMESTAMPTZ,
    app_version TEXT,
    reply_content TEXT,
    reply_date TIMESTAMPTZ,
    collected_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(app_id, review_id)
);
CREATE INDEX idx_reviews_app_date ON reviews(app_id, review_date);

-- 2. 일별 패널 (매일 업데이트)
CREATE TABLE daily_panel (
    id BIGSERIAL PRIMARY KEY,
    app_id TEXT NOT NULL,
    date DATE NOT NULL,
    avg_rating_7d FLOAT,
    avg_rating_3d FLOAT,
    avg_rating_14d FLOAT,
    avg_rating_30d FLOAT,
    review_count_7d FLOAT,
    review_count_3d FLOAT,
    review_count_14d FLOAT,
    review_count_30d FLOAT,
    negative_ratio_7d FLOAT,
    negative_ratio_3d FLOAT,
    negative_ratio_14d FLOAT,
    negative_ratio_30d FLOAT,
    positive_ratio_7d FLOAT,
    rating_volatility_7d FLOAT,
    thumbs_up_7d FLOAT,
    reply_ratio_7d FLOAT,
    negative_streak INTEGER DEFAULT 0,
    review_count_spike FLOAT,
    rating_momentum FLOAT,
    rating_accel_3v7 FLOAT,
    rating_accel_7v14 FLOAT,
    rating_accel_7v30 FLOAT,
    neg_accel_3v7 FLOAT,
    neg_accel_7v14 FLOAT,
    days_since_version_change INTEGER,
    days_since_drop INTEGER,
    recovery_from_min FLOAT,
    comp_avg_rating_7d FLOAT,
    comp_rating_diff FLOAT,
    comp_negative_ratio_7d FLOAT,
    health_score FLOAT,
    health_std FLOAT,
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(app_id, date)
);
CREATE INDEX idx_panel_app_date ON daily_panel(app_id, date);

-- 3. 주간 리포트
CREATE TABLE weekly_reports (
    id BIGSERIAL PRIMARY KEY,
    app_id TEXT NOT NULL,
    report_date DATE NOT NULL,
    period_start DATE,
    period_end DATE,
    drop_probability FLOAT,
    rise_probability FLOAT,
    risk_level TEXT,
    health_score FLOAT,
    briefing_text TEXT,
    report_json JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(app_id, report_date)
);

-- 4. 고객 피드백
CREATE TABLE feedback (
    id BIGSERIAL PRIMARY KEY,
    report_id BIGINT REFERENCES weekly_reports(id),
    app_id TEXT NOT NULL,
    feedback_type TEXT,  -- 'useful', 'inaccurate', 'action_taken'
    action_description TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 5. 패치 분석
CREATE TABLE patch_analysis (
    id BIGSERIAL PRIMARY KEY,
    app_id TEXT NOT NULL,
    patch_date DATE,
    old_version TEXT,
    new_version TEXT,
    update_type TEXT,
    user_reaction TEXT,
    risk_score INTEGER,
    neg_change FLOAT,
    score_change FLOAT,
    analyzed_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(app_id, new_version)
);
"""

import os
from supabase import create_client, Client
from datetime import datetime, date
import json


def get_client() -> Client:
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")
    if not url or not key:
        raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set")
    return create_client(url, key)


def upsert_reviews(client: Client, reviews: list[dict]) -> int:
    """리뷰를 reviews 테이블에 upsert. 중복은 무시."""
    if not reviews:
        return 0
    
    # Clean data for Supabase
    clean = []
    for r in reviews:
        clean.append({
            "app_id": r["app_id"],
            "review_id": r.get("review_id", ""),
            "score": int(r["score"]),
            "text": str(r.get("text", ""))[:5000],  # 5000자 제한
            "thumbs_up": int(r.get("thumbs_up", 0)),
            "review_date": r.get("review_date"),
            "app_version": str(r.get("app_version", ""))[:50],
            "reply_content": str(r.get("reply_content", ""))[:2000] if r.get("reply_content") else None,
            "reply_date": r.get("reply_date"),
        })
    
    # Batch upsert (Supabase handles conflicts via UNIQUE constraint)
    batch_size = 100
    inserted = 0
    for i in range(0, len(clean), batch_size):
        batch = clean[i:i+batch_size]
        try:
            result = client.table("reviews").upsert(
                batch, on_conflict="app_id,review_id"
            ).execute()
            inserted += len(result.data) if result.data else 0
        except Exception as e:
            print(f"  Upsert error (batch {i}): {e}")
    
    return inserted


def upsert_daily_panel(client: Client, panel_rows: list[dict]) -> int:
    """일별 패널 데이터를 upsert."""
    if not panel_rows:
        return 0
    
    # Convert date objects to strings
    clean = []
    for row in panel_rows:
        r = {}
        for k, v in row.items():
            if isinstance(v, (date, datetime)):
                r[k] = v.isoformat()
            elif isinstance(v, float) and (v != v):  # NaN check
                r[k] = None
            else:
                r[k] = v
        clean.append(r)
    
    batch_size = 100
    inserted = 0
    for i in range(0, len(clean), batch_size):
        batch = clean[i:i+batch_size]
        try:
            result = client.table("daily_panel").upsert(
                batch, on_conflict="app_id,date"
            ).execute()
            inserted += len(result.data) if result.data else 0
        except Exception as e:
            print(f"  Panel upsert error (batch {i}): {e}")
    
    return inserted


def get_recent_reviews(client: Client, app_id: str, days: int = 90):
    """특정 앱의 최근 N일 리뷰를 가져옴."""
    from datetime import timedelta
    cutoff = (datetime.now() - timedelta(days=days)).isoformat()
    result = client.table("reviews")\
        .select("*")\
        .eq("app_id", app_id)\
        .gte("review_date", cutoff)\
        .order("review_date", desc=True)\
        .execute()
    return result.data if result.data else []


def save_weekly_report(client: Client, report: dict) -> int:
    """주간 리포트를 저장."""
    clean = {
        "app_id": report["app_id"],
        "report_date": report["report_date"],
        "period_start": report.get("period_start"),
        "period_end": report.get("period_end"),
        "drop_probability": report.get("drop_probability"),
        "rise_probability": report.get("rise_probability"),
        "risk_level": report.get("risk_level"),
        "health_score": report.get("health_score"),
        "briefing_text": report.get("briefing_text"),
        "report_json": json.dumps(report.get("report_json", {}), ensure_ascii=False, default=str),
    }
    result = client.table("weekly_reports").upsert(
        clean, on_conflict="app_id,report_date"
    ).execute()
    return len(result.data) if result.data else 0
