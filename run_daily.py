"""
매일 실행되는 메인 스크립트.
GitHub Actions가 이 파일을 실행합니다.

1. 등록된 모든 앱의 최신 리뷰 수집
2. Supabase reviews 테이블에 저장
3. 일별 패널 재계산
4. Supabase daily_panel 테이블에 저장
"""

import sys
import time
from datetime import datetime

from collector.config import APPS
from collector.scraper import collect_all_apps
from collector.panel_builder import build_panel
from collector.db import get_client, upsert_reviews, upsert_daily_panel, get_recent_reviews


def main():
    start = time.time()
    print("=" * 60)
    print(f"AppPulse Daily Collection — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)
    
    # 1. Supabase 연결
    print("\n[1/4] Connecting to Supabase...")
    try:
        client = get_client()
        print("  Connected.")
    except Exception as e:
        print(f"  FAILED: {e}")
        sys.exit(1)
    
    # 2. 리뷰 수집
    print(f"\n[2/4] Collecting reviews for {len(APPS)} apps...")
    results = collect_all_apps(APPS)
    
    total_reviews = 0
    for app_id, data in results.items():
        reviews = data["reviews"]
        if reviews:
            inserted = upsert_reviews(client, reviews)
            total_reviews += inserted
            print(f"    {APPS[app_id]['name']}: {inserted} new reviews saved")
    
    print(f"  Total: {total_reviews} new reviews")
    
    # 3. 패널 재계산 (최근 90일)
    print(f"\n[3/4] Rebuilding daily panel...")
    all_reviews = []
    for app_id in APPS.keys():
        recent = get_recent_reviews(client, app_id, days=90)
        all_reviews.extend(recent)
    
    print(f"  Total reviews for panel: {len(all_reviews)}")
    
    if all_reviews:
        panel = build_panel(all_reviews, APPS)
        
        if not panel.empty:
            # Panel을 dict list로 변환
            panel_cols = [c for c in panel.columns if c in [
                'app_id', 'date', 'avg_rating_7d', 'avg_rating_3d', 'avg_rating_14d', 'avg_rating_30d',
                'review_count_7d', 'review_count_3d', 'review_count_14d', 'review_count_30d',
                'negative_ratio_7d', 'negative_ratio_3d', 'negative_ratio_14d', 'negative_ratio_30d',
                'positive_ratio_7d', 'rating_volatility_7d', 'thumbs_up_7d', 'reply_ratio_7d',
                'negative_streak', 'review_count_spike', 'rating_momentum',
                'rating_accel_3v7', 'rating_accel_7v14', 'rating_accel_7v30',
                'neg_accel_3v7', 'neg_accel_7v14',
                'days_since_version_change', 'days_since_drop', 'recovery_from_min',
                'comp_avg_rating_7d', 'comp_rating_diff', 'comp_negative_ratio_7d',
            ]]
            panel_rows = panel[panel_cols].to_dict('records')
            
            # 4. Supabase에 저장
            print(f"\n[4/4] Saving panel to Supabase ({len(panel_rows)} rows)...")
            saved = upsert_daily_panel(client, panel_rows)
            print(f"  Saved: {saved} rows")
        else:
            print("  Panel is empty, skipping save.")
    else:
        print("  No reviews found, skipping panel build.")
    
    elapsed = time.time() - start
    print(f"\n{'=' * 60}")
    print(f"Done in {elapsed:.0f}s ({elapsed/60:.1f}min)")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
