import os
from util.JtbcLogger import JtbcLogger
from datetime import datetime, timedelta
import time
from util.helper import Helper
from main.module.ElectionSummary import ElectionSummary
import traceback
from pathlib import Path
from main.job.Job import Job

@Job(job_name="ElectionSummaryRunner")
def runner(target_date:datetime, logger:JtbcLogger, my_helper:Helper):
    """
    집계 시간: 0 1 * * * (매일 01시에 전날 데이터를 집계)
    :param target_date:
    :param logger:
    :param my_helper:
    :return:
    """
    flag = True

    # 매일 새벽 1시에 수행하며, 전날 데이터에 대해서 요약지표를 생성하여 일/월 지표 upsert
    target_date = target_date - timedelta(days=1)
    target_date = target_date.replace(minute=59, second=59, microsecond=999999)
    logger.info(
        f"started job(ElectionSummaryRunner) > target_date = {target_date.strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        start = time.time()

        election_summary = ElectionSummary(helper=my_helper, level=my_helper.build_level)
        election_summary.do_election_news_summary_info_by_daily(the_date=target_date)

        end = time.time()
        logger.info(f"taken time > {end - start}")
        logger.info(f"job(ElectionSummaryRunner) was done. (target_date = {target_date.strftime('%Y-%m-%d %H:%M:%S')})\n\n\n")
    except (ValueError, Exception):
        raise

    return flag



# class ElectionSummaryRunner:
#
#     def __init__(self):
#         if os.path.isdir('/box/jnd-batch'):
#             # prod
#             path = '/box/jnd-batch'
#         else:
#             # dev
#             path = Path(__file__).parent.parent.parent # data-batch
#
#         # 기존 log 정책
#         # log_path = os.path.join(path, 'logs', '{}_{}.log'.format(file, datetime.today().strftime('%Y%m%d')))
#         # '/Users/jmac/project/jtbc_news_data/data-batch/main/logs/NewsSummary_20211018.log'
#         logger_factory = JtbcLogger(path=os.path.join(path, "logs"), file_name=type(self).__name__)
#         self.logger = logger_factory.logger
#
#     def run(self, target_date : datetime = None):
#         flag = True
#
#         my_helper = Helper()
#         try:
#             # 매일 새벽 1시에 수행하며, 전날 데이터에 대해서 요약지표를 생성하여 일/월 지표 upsert
#             target_date = target_date - timedelta(days=1)
#             target_date = target_date.replace(minute=59, second=59, microsecond=999999)
#             self.logger.info("="*100)
#             self.logger.info(f"started job({type(self).__name__}) > target_date = {target_date.strftime('%Y-%m-%d %H:%M:%S')}")
#
#             start = time.time()
#
#             election_summary = ElectionSummary(helper=my_helper, level=my_helper.build_level)
#             election_summary.do_election_news_summary_info_by_daily(the_date=target_date)
#
#
#             end = time.time()
#             self.logger.info(f"taken time > {end - start}")
#             self.logger.info(
#                 f"job({type(self).__name__}) was done. (target_date = {target_date.strftime('%Y-%m-%d %H:%M:%S')})\n\n\n")
#
#         except (ValueError, Exception):
#             # exc_type, exc_value, exc_traceback = sys.exc_info()
#             # msg = repr(traceback.format_exception(exc_type, exc_value, exc_traceback))
#             # self.logger.error(msg)
#             # traceback.print_exc()는 출력 스트림이기 때문에 실제 logger file에 write가 안됨
#             self.logger.error(f"[{my_helper.build_level.upper()}] ERROR: {traceback.format_exc()}")
#             self.logger.error(f"[{my_helper.build_level.upper()}] job({type(self).__name__}) was not working. "
#                               f"(target_date = {target_date.strftime('%Y-%m-%d %H:%M:%S')})\n\n\n")
#
#             my_helper.get_slack().post_message(f"[{my_helper.build_level.upper()}] job({type(self).__name__}) Error "
#                                         f"(target_date = {target_date.strftime('%Y-%m-%d %H:%M:%S')})\n"
#                                         f"{traceback.format_exc()}")
#             flag = False
#
#         return flag


if __name__ == "__main__":
    # runner = ElectionSummaryRunner()
    # target_date = datetime.now() - timedelta(hours=1)
    # runner.run(target_date = target_date)
    flag = runner(target_date=datetime.now())
    print("=" * 100)
    print(flag)