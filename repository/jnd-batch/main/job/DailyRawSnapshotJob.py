import os
from util.JtbcLogger import JtbcLogger
from _datetime import datetime, timedelta
import time
from util.helper import Helper, Parser
from main.snapshot.DailyRawDataSnapshot import DailyRawDataSnapshot
import traceback
from pathlib import Path


class DailyRawSnapshotJob:

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

			obj_snapshot = DailyRawDataSnapshot(helper=my_helper, target_date=target_date)
			obj_snapshot.createRepository()
			obj_snapshot.createReindex()
			obj_snapshot.createSnapshot()
			# ###tar 압축제외
			# ###obj_snapshot.StepFinalTask()

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
	runner = DailyRawSnapshotJob()
	# 새벽에 전일자 스냅샷 수행
	the_date = datetime.today() - timedelta(days=1)
	runner.run(target_date=the_date)
