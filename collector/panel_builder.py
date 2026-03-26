"""
일별 패널 생성기.
원본 리뷰에서 일별 통계 → 이동평균 → Flow 변수 → 경쟁사 교차 변수를 계산합니다.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from collector.config import PANEL_CONFIG, APPS


def build_daily_stats(reviews: list[dict]) -> pd.DataFrame:
    """원본 리뷰 → 일별 기본 통계."""
    if not reviews:
        return pd.DataFrame()
    
    df = pd.DataFrame(reviews)
    df["review_date"] = pd.to_datetime(df["review_date"])
    df["date"] = df["review_date"].dt.date
    
    daily = df.groupby(["app_id", "date"]).agg(
        avg_rating=("score", "mean"),
        review_count=("score", "count"),
        rating_std=("score", "std"),
        negative_count=("score", lambda x: (x <= 2).sum()),
        positive_count=("score", lambda x: (x >= 4).sum()),
        avg_thumbs_up=("thumbs_up", "mean"),
        has_reply=("reply_content", lambda x: (x.notna() & (x != "")).sum()),
        unique_versions=("app_version", "nunique"),
        latest_version=("app_version", "last"),
    ).reset_index()
    
    daily["negative_ratio"] = daily["negative_count"] / daily["review_count"]
    daily["positive_ratio"] = daily["positive_count"] / daily["review_count"]
    daily["reply_ratio"] = daily["has_reply"] / daily["review_count"]
    
    return daily


def add_rolling_features(daily: pd.DataFrame) -> pd.DataFrame:
    """이동평균 + 가속도 + 스트릭 등을 추가합니다."""
    if daily.empty:
        return daily
    
    windows = PANEL_CONFIG["rolling_windows"]
    daily = daily.sort_values(["app_id", "date"])
    
    frames = []
    for app_id in daily["app_id"].unique():
        ad = daily[daily["app_id"] == app_id].copy()
        ad["date"] = pd.to_datetime(ad["date"])
        
        # 날짜 연속으로 만들기 (빈 날짜 채우기)
        if len(ad) > 1:
            date_range = pd.date_range(ad["date"].min(), ad["date"].max())
            ad = ad.set_index("date").reindex(date_range).rename_axis("date").reset_index()
            ad["app_id"] = app_id
        
        # 이동평균
        for w in windows:
            suffix = f"_{w}d"
            ad[f"avg_rating{suffix}"] = ad["avg_rating"].rolling(w, min_periods=max(1, w//3)).mean()
            ad[f"review_count{suffix}"] = ad["review_count"].rolling(w, min_periods=1).sum()
            ad[f"negative_ratio{suffix}"] = ad["negative_ratio"].rolling(w, min_periods=max(1, w//3)).mean()
            ad[f"positive_ratio{suffix}"] = ad["positive_ratio"].rolling(w, min_periods=max(1, w//3)).mean()
        
        # 가속도
        if "avg_rating_3d" in ad.columns and "avg_rating_7d" in ad.columns:
            ad["rating_accel_3v7"] = ad["avg_rating_3d"] - ad["avg_rating_7d"]
        if "avg_rating_7d" in ad.columns and "avg_rating_14d" in ad.columns:
            ad["rating_accel_7v14"] = ad["avg_rating_7d"] - ad["avg_rating_14d"]
        if "avg_rating_7d" in ad.columns and "avg_rating_30d" in ad.columns:
            ad["rating_accel_7v30"] = ad["avg_rating_7d"] - ad["avg_rating_30d"]
        if "negative_ratio_3d" in ad.columns and "negative_ratio_7d" in ad.columns:
            ad["neg_accel_3v7"] = ad["negative_ratio_3d"] - ad["negative_ratio_7d"]
        if "negative_ratio_7d" in ad.columns and "negative_ratio_14d" in ad.columns:
            ad["neg_accel_7v14"] = ad["negative_ratio_7d"] - ad["negative_ratio_14d"]
        
        # 변동성
        ad["rating_volatility_7d"] = ad["avg_rating"].rolling(7, min_periods=3).std()
        
        # 모멘텀
        ad["rating_momentum"] = ad["avg_rating_7d"] - ad["avg_rating_14d"] if "avg_rating_14d" in ad.columns else 0
        
        # 리뷰 급증
        rc_mean = ad["review_count"].rolling(30, min_periods=7).mean()
        ad["review_count_spike"] = ad["review_count_7d"] / rc_mean.replace(0, np.nan) if "review_count_7d" in ad.columns else 1
        
        # Thumbs up 7d
        ad["thumbs_up_7d"] = ad["avg_thumbs_up"].rolling(7, min_periods=3).mean()
        ad["reply_ratio_7d"] = ad["reply_ratio"].rolling(7, min_periods=3).mean()
        
        # 부정 연속 상승일
        if "negative_ratio" in ad.columns:
            streak = 0
            streaks = []
            prev = None
            for val in ad["negative_ratio"].values:
                if pd.isna(val):
                    streaks.append(streak)
                    continue
                if prev is not None and val > prev:
                    streak += 1
                else:
                    streak = 0
                streaks.append(streak)
                prev = val
            ad["negative_streak"] = streaks
        
        # 버전 변경 경과일
        if "latest_version" in ad.columns:
            ad["version_changed"] = (ad["latest_version"] != ad["latest_version"].shift(1)).astype(int)
            ad["version_changed"].iloc[0] = 0
            days_since = []
            count = 999
            for vc in ad["version_changed"].values:
                if vc == 1:
                    count = 0
                else:
                    count += 1
                days_since.append(count)
            ad["days_since_version_change"] = days_since
        
        # 급락 경과일 + 최저점 회복
        threshold = PANEL_CONFIG["drop_threshold"]
        if "avg_rating_7d" in ad.columns:
            ad["drop_event"] = (ad["avg_rating_7d"].diff() < -threshold).astype(int)
            days_since_drop = []
            count = 999
            for de in ad["drop_event"].values:
                if de == 1:
                    count = 0
                else:
                    count += 1
                days_since_drop.append(count)
            ad["days_since_drop"] = days_since_drop
            
            rolling_min = ad["avg_rating_7d"].rolling(30, min_periods=7).min()
            ad["recovery_from_min"] = ad["avg_rating_7d"] - rolling_min
        
        frames.append(ad)
    
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def add_competitor_features(panel: pd.DataFrame, apps_config: dict) -> pd.DataFrame:
    """경쟁사 교차 변수를 추가합니다."""
    if panel.empty:
        return panel
    
    panel = panel.copy()
    panel["comp_avg_rating_7d"] = np.nan
    panel["comp_rating_diff"] = np.nan
    panel["comp_negative_ratio_7d"] = np.nan
    
    for app_id, meta in apps_config.items():
        competitors = meta.get("competitors", [])
        if not competitors:
            continue
        
        app_mask = panel["app_id"] == app_id
        
        for date_val in panel.loc[app_mask, "date"].unique():
            comp_ratings = []
            comp_neg = []
            
            for comp_id in competitors:
                comp_row = panel[(panel["app_id"] == comp_id) & (panel["date"] == date_val)]
                if not comp_row.empty:
                    r = comp_row.iloc[0].get("avg_rating_7d")
                    n = comp_row.iloc[0].get("negative_ratio_7d")
                    if pd.notna(r):
                        comp_ratings.append(r)
                    if pd.notna(n):
                        comp_neg.append(n)
            
            if comp_ratings:
                mask = app_mask & (panel["date"] == date_val)
                app_rating = panel.loc[mask, "avg_rating_7d"].values
                panel.loc[mask, "comp_avg_rating_7d"] = np.mean(comp_ratings)
                if len(app_rating) > 0 and pd.notna(app_rating[0]):
                    panel.loc[mask, "comp_rating_diff"] = app_rating[0] - np.mean(comp_ratings)
            if comp_neg:
                mask = app_mask & (panel["date"] == date_val)
                panel.loc[mask, "comp_negative_ratio_7d"] = np.mean(comp_neg)
    
    return panel


def build_panel(all_reviews: list[dict], apps_config: dict) -> pd.DataFrame:
    """원본 리뷰 → 완성된 패널."""
    print("  Building daily stats...")
    daily = build_daily_stats(all_reviews)
    if daily.empty:
        return daily
    
    print("  Adding rolling features...")
    panel = add_rolling_features(daily)
    
    print("  Adding competitor features...")
    panel = add_competitor_features(panel, apps_config)
    
    # 날짜를 date 타입으로 통일
    panel["date"] = pd.to_datetime(panel["date"]).dt.date
    
    # NaN 정리
    numeric_cols = panel.select_dtypes(include=[np.number]).columns
    panel[numeric_cols] = panel[numeric_cols].replace([np.inf, -np.inf], np.nan)
    
    print(f"  Panel: {panel.shape}, Apps: {panel['app_id'].nunique()}, Days: {panel['date'].nunique()}")
    return panel
