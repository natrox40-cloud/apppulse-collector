# ============================================================
# AppPulse Data Collector
# GitHub Actions로 매일 자동 실행
# google-play-scraper → Supabase 저장
# ============================================================

# 이 파일들을 GitHub 저장소에 올리면 매일 자동으로 리뷰를 수집합니다.
# 
# 저장소 구조:
# apppulse-collector/
# ├── .github/
# │   └── workflows/
# │       └── daily_collect.yml    ← GitHub Actions 설정
# ├── collector/
# │   ├── scraper.py               ← 리뷰 수집
# │   ├── panel_builder.py         ← 일별 패널 생성
# │   ├── config.py                ← 앱 목록 + 설정
# │   └── db.py                    ← Supabase 연결
# ├── requirements.txt
# └── run_daily.py                 ← 매일 실행되는 메인 스크립트
#
# 필요한 GitHub Secrets:
# - SUPABASE_URL
# - SUPABASE_KEY
# - ANTHROPIC_API_KEY (주간 리포트용, 나중에 추가)
