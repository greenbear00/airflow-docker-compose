import os
from datetime import datetime, timedelta
import time

from main.job.Job import Job
from util.helper import Helper
from main.module.UpdateNewsInfo import UpdateNewsInfo
from main.module.ProduceNewsSumary import ProduceNewsSummary
from pathlib import Path
from util.JtbcLogger import JtbcLogger
import traceback

@Job(job_name="ProduceNewsSummary")
def runner(target_date:datetime, logger:JtbcLogger, my_helper:Helper):
	"""
	집계 시간: 0 1 * * * (매일 01시에 전날 데이터를 집계)
	:param target_date:
	:param logger:
	:param my_helper:
	:return:
	"""
	flag = True
	target_date = target_date - timedelta(days=1)
	target_date = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
	logger.info("started job(ProduceNewsSummary) > {}".format(target_date.strftime('%Y-%m-%d %H:%M:%S')))
	try:

		start = time.time()

		obj_update_news = UpdateNewsInfo(my_helper)
		news_ret, news_set = obj_update_news.getNewsData(last_time=target_date)
		upsert_ret, upsert_list = obj_update_news.UpsertNewsBasicInfo(news_set=news_set, the_date=target_date)

		obj_produce_news = ProduceNewsSummary(my_helper)
		# 기자별 생산량 지표
		ret1, res1 = obj_produce_news.aggrProduceByReporter(the_date=target_date)
		# 부서별 생산량 지표
		ret2, res2 = obj_produce_news.aggrProduceByDepartment(the_date=target_date)
		# 섹션별 생산량 지표
		ret3, res3 = obj_produce_news.aggrProduceBySection(the_date=target_date)

		end = time.time()

		logger.info('Basic 기사정보 > {}건 처리'.format(len(upsert_list)))
		logger.info('기자별 생산량 지표 > {}건 처리'.format(len(res1)))
		logger.info('부서별 생산량 지표 > {}건 처리'.format(len(res2)))
		logger.info('섹션별 생산량 지표 > {}건 처리'.format(len(res3)))
		logger.info("taken time > {}".format(end - start))
		logger.info("finished job(ProduceNewsSummary)\n\n")
	except Exception:
		raise
	return flag



if __name__ == '__main__':
	flag = runner(target_date=datetime.now())
	print("=" * 100)
	print(flag)
	# if os.path.isdir('/box/jnd-batch'):
	# 	path = '/box/jnd-batch'
	# else:
	# 	# path = os.path.abspath(os.path.join(os.getcwd(), os.pardir))
	# 	path = Path(__file__).parent.parent.parent
	# filename = os.path.basename(__file__)
	# (file, ext) = os.path.splitext(filename)
	#
	# os.makedirs(os.path.join(path, 'logs'), exist_ok=True)
	# # log_path = os.path.join(path, 'logs', '{}_{}.log'.format(file, datetime.today().strftime('%Y%m%d')))
	# # print(f"log_path = {log_path}")
	# # logging.basicConfig(format='%(asctime)s %(levelname)-4s %(message)s', datefmt='%Y-%m-%d %H:%M:%S', filename=log_path, level=logging.INFO)
	# # log_path = os.path.join(path, 'logs', '{}_{}.log'.format(file, datetime.today().strftime('%Y%m%d')))
	# logger_factory = JtbcLogger(path=os.path.join(path, "logs"), file_name=file)
	# logger = logger_factory.logger
	#
	#
	# # 매일 1회 실행, -1일 자정 이후 실행
	# target_date = datetime.now() - timedelta(days=1)
	# target_date = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
	# logger.info("started job(ProduceNewsSummary) > {}".format(target_date.strftime('%Y-%m-%d %H:%M:%S')))
	# my_helper = Helper()
	# try:
	# 	start = time.time()
	#
	# 	obj_update_news = UpdateNewsInfo(my_helper)
	# 	news_ret, news_set = obj_update_news.getNewsData(last_time=target_date)
	# 	upsert_ret, upsert_list = obj_update_news.UpsertNewsBasicInfo(news_set=news_set, the_date=target_date)
	#
	# 	obj_produce_news = ProduceNewsSummary(my_helper)
	# 	# 기자별 생산량 지표
	# 	ret1, res1 = obj_produce_news.aggrProduceByReporter(the_date=target_date)
	# 	# 부서별 생산량 지표
	# 	ret2, res2 = obj_produce_news.aggrProduceByDepartment(the_date=target_date)
	# 	# 섹션별 생산량 지표
	# 	ret3, res3 = obj_produce_news.aggrProduceBySection(the_date=target_date)
	#
	# 	end = time.time()
	#
	# 	logger.info('Basic 기사정보 > {}건 처리'.format(len(upsert_list)))
	# 	logger.info('기자별 생산량 지표 > {}건 처리'.format(len(res1)))
	# 	logger.info('부서별 생산량 지표 > {}건 처리'.format(len(res2)))
	# 	logger.info('섹션별 생산량 지표 > {}건 처리'.format(len(res3)))
	# 	logger.info("taken time > {}".format(end - start))
	# 	logger.info("finished job(ProduceNewsSummary)\n\n")
	#
	# except Exception as es:
	# 	logger.error(f"[{my_helper.build_level.upper()}] ERROR: {traceback.format_exc()}")
	# 	logger.error(f"[{my_helper.build_level.upper()}] job(ProduceNewsSummary) was not working. "
	# 				 f"(target_date = {target_date.strftime('%Y-%m-%d %H:%M:%S')})\n\n\n")
	#
	# 	my_helper.get_slack().post_message(f"[{my_helper.build_level.upper()}] job(ProduceNewsSummary) ERROR "
	# 									   f"(target_date = {target_date.strftime('%Y-%m-%d %H:%M:%S')})\n"
	# 									   f"{traceback.format_exc()}")



