import os
from util.JtbcLogger import JtbcLogger
from datetime import datetime, timedelta
import time
from util.helper import Helper, Parser
from main.module.PlatformSummary import PlatformSummary
import traceback
from pathlib import Path
from main.job.Job import Job

@Job(job_name="PlatformSummary")
def runner(target_date:datetime, logger:JtbcLogger, my_helper:Helper):
	"""
	주의: 아직 운영에 적용 안됨
	:param target_date:
	:param logger:
	:param my_helper:
	:return:
	"""
	flag = True

	target_date = target_date - timedelta(hours=1)
	target_date = target_date.replace(minute=59, second=59, microsecond=999999)
	logger.info(f"started job(PlatformSummary) > {target_date.strftime('%Y-%m-%d %H:%M:%S')}")
	try:
		start = time.time()

		obj_platform = PlatformSummary(my_helper)
		ret1, res1 = obj_platform.aggrDailyPlatformSummary(the_date=target_date)

		end = time.time()

		logger.info('플랫폼별 현황 지표 > {}건 처리'.format(len(res1)))
		logger.info("taken time > {}".format(end - start))
		logger.info("finished job(PlatformSummary)")

	except (ValueError, Exception):
		raise

	return flag

# class PlatformSummaryJobRunner:
#
# 	def __init__(self):
# 		if os.path.isdir('/box/jnd-batch'):
# 			# prod
# 			path = '/box/jnd-batch'
# 		else:
# 			# dev
# 			path = Path(__file__).parent.parent.parent # data-batch
#
# 		logger_factory = JtbcLogger(path=os.path.join(path, "logs"), file_name=type(self).__name__)
# 		self.logger = logger_factory.logger
#
# 	def run(self, target_date: datetime = None):
# 		flag = True
#
# 		my_helper = Helper()
# 		try:
# 			target_date = target_date - timedelta(hours=1)
# 			target_date = target_date.replace(minute=59, second=59, microsecond=999999)
# 			self.logger.info(f"started job({type(self).__name__}) > {target_date.strftime('%Y-%m-%d %H:%M:%S')}")
# 			start = time.time()
#
# 			obj_platform = PlatformSummary(my_helper)
# 			ret1, res1 = obj_platform.aggrDailyPlatformSummary(the_date=target_date)
#
# 			end = time.time()
#
# 			self.logger.info('플랫폼별 현황 지표 > {}건 처리'.format(len(res1)))
# 			self.logger.info("taken time > {}".format(end - start))
# 			self.logger.info("finished job(PlatformSummary)")
#
# 		except (ValueError, Exception):
# 			self.logger.error(f"[{my_helper.build_level.upper()}] ERROR: {traceback.format_exc()}")
# 			self.logger.error(f"[{my_helper.build_level.upper()}] job(PlatformSummary) was not working. \
# 					(target_date = {target_date.strftime('%Y-%m-%d %H:%M:%S')})\n\n\n")
#
# 			my_helper.get_slack().post_message(f"[{my_helper.build_level.upper()}] job(PlatformSummary) ERROR \
# 					(target_date = {target_date.strftime('%Y-%m-%d %H:%M:%S')})\n{traceback.format_exc()}")
# 			flag = False
#
# 		return flag


if __name__ == "__main__":
	# runner = PlatformSummaryJobRunner()
	# the_date = datetime.now() - timedelta(hours=1)
	# runner.run(target_date=the_date)
	flag = runner(target_date=datetime.now())
	print("=" * 100)
	print(flag)
