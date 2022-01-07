NAVER_NEWS_HOUR_STAT = 'naver-news-hour-stat'
NAVER_NEWS_DAY_STAT = 'naver-news-day-stat'

DAUM_NEWS_HOUR_STAT = 'daum-news-hour-stat'
DAUM_NEWS_DAY_STAT = 'daum-news-day-stat'

# 데이터 건수 고려 시 현행 유지
HOURLY_NEWS_SUMMARY = 'hourly-news-summary'
DAILY_NEWS_SUMMARY = 'daily-news-summary'

################################################
# 아래부터는 index 및 alias 관리 체계를 따른다:
# ref: https://pri-confluence.joongang.co.kr/pages/viewpage.action?pageId=27549616
################################################
# module 내부에서 인덱스명 처리 (먼저 alias를 수동으로 적용한 후에 아래와 같이 origin 없이 가능)
# (prod) index: origin-daily-produce-reporter-2021
# (prod) alias: daily-produce-reporter-2021
DAILY_PRODUCE_REPORTER = 'daily-produce-reporter'
DAILY_PRODUCE_DEPART = 'daily-produce-depart'
DAILY_PRODUCE_SECTION = 'daily-produce-section'

DAILY_PLATFORM_SUMMARY = 'daily-platform-summary'

BASIC_NEWS_INFO = 'basic-news-info'

# TimebasedRunner에서 실제 index 네임
# index: origin-hourly-summary-2021
# alias: hourly-summary-2021
HOURLY_SUMMARY = 'hourly-summary'
DAILY_SUMMARY = 'daily-summary'

SOURCE_CODE_JTBC = '11'
SOURCE_CODE_JAM = '17'

# jtbcnews 사이트에 접근한 유니크한 사용자 및 로그인한 멤버 관련 일/월 지표 index
USER_DAILY_SUMMARY = 'user-daily'
USER_MONTHLY_SUMMARY = 'user-monthly'

# election관련 일별 지표 (election 뉴스 조회수에 대해 parm_origin +ua_brawser_family로 구분
ELECTION_USER_DAILY_SUMMARY = 'election-user-daily'
ELECTION_DAILY_NEWS_SUMMARY = 'election-daily-news'

JTBC_PROGRAM_INFO = 'jtbc-program-info'
JTBC_MAPPING_TV_NEW_PROGRAM = 'mapping-tv-news-program'


ELASTIC_DEV_SHARD = 1
ELASTIC_PROD_SHARD = 2
