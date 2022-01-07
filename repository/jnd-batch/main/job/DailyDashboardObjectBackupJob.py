import os
from util.JtbcLogger import JtbcLogger
from _datetime import datetime, timedelta
import time
from util.helper import Helper, Parser
from main.snapshot.DailyDashboardObjectBackup import DailyDashboardObjectBackup
import traceback
from pathlib import Path


class DailyDashboardObjectBackupJob:

	def __init__(self):
		if os.path.isdir('/box/jnd-batch'):
			# prod
			path = '/box/jnd-batch'
		else:
			# dev
			path = Path(__file__).parent.parent.parent

		logger_factory = JtbcLogger(path=os.path.join(path, "logs"), file_name=type(self).__name__)
		self.logger = logger_factory.logger

	def run(self, target_date: datetime = None):
		flag = True

		my_helper = Helper()
		try:
			target_date = target_date.replace(minute=59, second=59, microsecond=999999)
			self.logger.info(f"started job({type(self).__name__}) > {target_date.strftime('%Y-%m-%d %H:%M:%S')}")
			start = time.time()

			dashboard_list = list()
			dashboard_list.append({"name": "[기획팀] JTBC 플랫폼 콘텐트 사용 통계", "id": "6a842460-37c6-11ec-bad4-83d15d02a802"})
			dashboard_list.append({"name": "[News]실시간 현황", "id": "73decde0-04c6-11ec-b88b-1dd008f44c55"})
			dashboard_list.append({"name": "[News]실시간 현황(모바일)", "id": "a6590dc0-1ff7-11ec-aec0-4532b2a3a9aa"})
			dashboard_list.append({"name": "[NEWS]플랫폼별 현황", "id": "a5b078e0-41fd-11ec-bad4-83d15d02a802"})
			dashboard_list.append({"name": "[News]기사조회수(Top200)", "id": "a16cf430-3e0a-11ec-bad4-83d15d02a802"})
			dashboard_list.append({"name": "[News] 부서별 생산수&조회수", "id": "4e332e60-3793-11ec-bad4-83d15d02a802"})
			dashboard_list.append({"name": "[News] 기자별 생산수&조회수", "id": "0eb3ef50-4138-11ec-bad4-83d15d02a802"})
			dashboard_list.append({"name": "대선 특집 페이지 콘텐트 유입 성과", "id": "911110d0-3167-11ec-b2d9-6fb9c12d3e6d"})
			dashboard_list.append({"name": "[News]기사 상세", "id": "8d3ec6c0-27dd-11ec-b2d9-6fb9c12d3e6d"})
			dashboard_list.append({"name": "[기획팀] JTBC 뉴스 플랫폼 콘텐트 사용 통계", "id": "89083410-5663-11ec-848c-b371817548f1"})

			''' # 키바나 대시보드 정보 가져오기
			GET .kibana/_search
			{
			  "query": {
				"match": {
				  "type": "dashboard"
				}
			  },
			  "_source": ["_id", "dashboard.title", "updated_at"]
			}			
			'''

			object_backup = DailyDashboardObjectBackup(helper=my_helper, dashboard_list=dashboard_list)
			object_backup.get_dashboard_object()

			end = time.time()

			self.logger.info("taken time > {}".format(end - start))
			self.logger.info(f"finished job({type(self).__name__})")

		except (ValueError, Exception):
			self.logger.error(f"[{my_helper.build_level.upper()}] ERROR: {traceback.format_exc()}")
			self.logger.error(f"[{my_helper.build_level.upper()}] job({type(self).__name__}) was not working. \
					(target_date = {target_date.strftime('%Y-%m-%d %H:%M:%S')})\n\n\n")

			my_helper.get_slack().post_message(f"[{my_helper.build_level.upper()}] job(PlatformSummary) ERROR \
					(target_date = {target_date.strftime('%Y-%m-%d %H:%M:%S')})\n{traceback.format_exc()}")
			flag = False

		return flag


if __name__ == "__main__":
	runner = DailyDashboardObjectBackupJob()
	# 매일 23시에 대시보드 관련 오브젝트 백업 수행
	the_date = datetime.today()
	runner.run(target_date=the_date)
