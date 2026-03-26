"""
Google Play 리뷰 수집기.
google-play-scraper로 최신 리뷰를 가져옵니다.
"""

import time
from datetime import datetime
from google_play_scraper import reviews, Sort, app as app_info
from collector.config import COLLECT_CONFIG


def collect_reviews(app_id: str) -> list[dict]:
    """앱의 최신 리뷰를 수집합니다."""
    cfg = COLLECT_CONFIG
    all_reviews = []
    continuation_token = None
    
    for attempt in range(cfg["max_retries"]):
        try:
            result, continuation_token = reviews(
                app_id,
                lang=cfg["lang"],
                country=cfg["country"],
                sort=Sort.NEWEST,
                count=cfg["reviews_per_app"],
                continuation_token=continuation_token,
            )
            
            for r in result:
                all_reviews.append({
                    "app_id": app_id,
                    "review_id": r.get("reviewId", ""),
                    "score": r.get("score", 0),
                    "text": r.get("content", ""),
                    "thumbs_up": r.get("thumbsUpCount", 0),
                    "review_date": r.get("at").isoformat() if r.get("at") else None,
                    "app_version": r.get("reviewCreatedVersion", ""),
                    "reply_content": r.get("replyContent", ""),
                    "reply_date": r.get("repliedAt").isoformat() if r.get("repliedAt") else None,
                })
            
            break  # 성공하면 루프 탈출
            
        except Exception as e:
            print(f"    Retry {attempt+1}/{cfg['max_retries']}: {e}")
            time.sleep(cfg["retry_delay"])
    
    return all_reviews


def get_app_info(app_id: str) -> dict:
    """앱 기본 정보를 가져옵니다."""
    try:
        info = app_info(app_id, lang="en", country="us")
        return {
            "app_id": app_id,
            "name": info.get("title", ""),
            "score": info.get("score", 0),
            "ratings": info.get("ratings", 0),
            "installs": info.get("installs", ""),
            "version": info.get("version", ""),
            "updated": info.get("updated", ""),
        }
    except Exception as e:
        print(f"    App info error: {e}")
        return {"app_id": app_id, "name": app_id}


def collect_all_apps(apps: dict) -> dict:
    """모든 등록 앱의 리뷰를 수집합니다."""
    cfg = COLLECT_CONFIG
    results = {}
    
    total = len(apps)
    for i, (app_id, meta) in enumerate(apps.items()):
        print(f"  [{i+1}/{total}] {meta.get('name', app_id)}...", end=" ")
        
        reviews_list = collect_reviews(app_id)
        info = get_app_info(app_id)
        
        # 현재 버전 감지 (패치 추적용)
        current_version = info.get("version", "")
        
        results[app_id] = {
            "reviews": reviews_list,
            "info": info,
            "current_version": current_version,
            "collected_at": datetime.now().isoformat(),
        }
        
        print(f"{len(reviews_list)} reviews, v{current_version}")
        time.sleep(cfg["sleep_between_apps"])
    
    return results
