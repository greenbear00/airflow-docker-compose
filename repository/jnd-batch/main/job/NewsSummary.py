
from datetime import datetime, timedelta
import time
from util.helper import Helper
from main.module.NewsSummary import NewsSummary
from main.module.UpdateNewsInfo import UpdateNewsInfo
from util.JtbcLogger import JtbcLogger
from main.job.Job import Job

@Job(job_name="NewsSummary")
def runner(target_date:datetime, logger:JtbcLogger, my_helper:Helper):
	"""
	집계 시간: 20 * * * * (매시 20분에 전 1시간에 대해서 집계)

	:param target_date:
	:param logger:
	:param my_helper:
	:return:
	"""
	flag = True
	target_date = target_date-timedelta(hours=1)
	target_date = target_date.replace(minute=59, second=59, microsecond=999999)

	logger.info("started job(NewsSummary) > {}".format(target_date.strftime('%Y-%m-%d %H:%M:%S')))
	# my_helper = Helper()
	try:
		start = time.time()

		obj_news_summary = NewsSummary(my_helper)
		ret1, res1 = obj_news_summary.aggrHourlyNewsSummary(the_date=target_date)
		ret2, res2 = obj_news_summary.aggrDailyNewSummary(the_date=target_date)

		end = time.time()

		logger.info('Hourly > {}건 처리'.format(len(res1)))
		logger.info('Daily > {}건 처리'.format(len(res2)))
		logger.info("taken time > {}".format(end - start))

		obj_update_news = UpdateNewsInfo(my_helper)
		ds_ret1, ds1 = obj_update_news.getNewsData(last_time=target_date)
		ds_ret2, ds2 = obj_update_news.UpdateNewsSummary(news_set=ds1, last_time=target_date)
		logger.info("updated detail news data > {}".format(','.join(ds2)))

		logger.info(f"finished job(NewsSummary)\n\n")

	except Exception:
		raise
	return flag

if __name__ == '__main__':
	flag = runner(target_date=datetime.now())
	print("="*100)
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
	# log_path = os.path.join(path, 'logs', '{}_{}.log'.format(file, datetime.today().strftime('%Y%m%d')))
	# logger_factory = JtbcLogger(path=os.path.join(path, "logs"), file_name=file)
	# logger = logger_factory.logger
	#
	# # 매시 20분에 실행, -1시간 59분 59초
	# target_date = datetime.now() - timedelta(hours=1)
	# target_date = target_date.replace(minute=59, second=59, microsecond=999999)
	#
	# logger.info("started job(NewsSummary) > {}".format(target_date.strftime('%Y-%m-%d %H:%M:%S')))
	# my_helper = Helper()
	# try:
	# 	start = time.time()
	#
	# 	obj_news_summary = NewsSummary(my_helper)
	# 	ret1, res1 = obj_news_summary.aggrHourlyNewsSummary(the_date=target_date)
	# 	ret2, res2 = obj_news_summary.aggrDailyNewSummary(the_date=target_date)
	#
	# 	end = time.time()
	#
	# 	logger.info('Hourly > {}건 처리'.format(len(res1)))
	# 	logger.info('Daily > {}건 처리'.format(len(res2)))
	# 	logger.info("taken time > {}".format(end - start))
	#
	# 	obj_update_news = UpdateNewsInfo(my_helper)
	# 	ds_ret1, ds1 = obj_update_news.getNewsData(last_time=target_date)
	# 	ds_ret2, ds2 = obj_update_news.UpdateNewsSummary(news_set=ds1, last_time=target_date)
	# 	logger.info("updated detail news data > {}".format(','.join(ds2)))
	#
	# 	logger.info(f"finished job(NewsSummary)\n\n")
	#
	# except Exception as es:
	# 	logger.error(f"[{my_helper.build_level.upper()}] ERROR: {traceback.format_exc()}")
	# 	logger.error(f"[{my_helper.build_level.upper()}] job(NewsSummary) was not working. "
	# 					  f"(target_date = {target_date.strftime('%Y-%m-%d %H:%M:%S')})\n\n\n")
	#
	# 	my_helper.get_slack().post_message(f"[{my_helper.build_level.upper()}] job(NewsSummary) ERROR "
    #                                     f"(target_date = {target_date.strftime('%Y-%m-%d %H:%M:%S')})\n"
    #                                     f"{traceback.format_exc()}")


