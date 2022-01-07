from util.JtbcLogger import JtbcLogger
from datetime import datetime, timedelta
import time
from util.helper import Helper
from main.module.TVProInfo import TVProInfo
from main.job.Job import Job

@Job(job_name="MappingJTBCTVProRunner")
def runner(target_date:datetime, logger:JtbcLogger, my_helper:Helper):
    """
    집계 시간: 10 * * * * (10분마다 JTBC 방송 프로그램 중 NW(뉴스)에 해당하는 방송 프로그램 정보를 upsert함)
    현 시점 기준으로 jtbc-tv-db 에서 당일 기준으로 news_id와 hot_news를 prog_id(JTBC 뉴스 방송 프로그램)와 매핑함
    - DB에서는 prog_date(YYYYMMDD)를 기준으로 reg_date, mod_date만 존재하는데 시간에 대한 규칙성이 전혀 없음.

    :param target_date: 실행시점 datetime
    :return:
    """
    flag = True
    target_date = target_date - timedelta(days=1)
    logger.info(
        f"started job(MappingJTBCTVProRunner) > target_date = {target_date.strftime('%Y-%m-%d %H:%M:%S')}")
    try:
        start = time.time()

        tvpro_info = TVProInfo(helper=my_helper, level=my_helper.build_level)
        tvpro_info.do_update_mapping_tv_news_program(the_date=target_date)

        end = time.time()
        logger.info(f"taken time > {end - start}")
        logger.info(
            f"job(MappingJTBCTVProRunner) was done. (target_date = {target_date.strftime('%Y-%m-%d %H:%M:%S')})\n\n\n")

    except (ValueError, Exception):
        raise

    return flag


# class MappingJTBCTVProRunner:
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
#         """
#         현 시점 기준으로 jtbc-tv-db 에서 당일 기준으로 news_id와 hot_news를 prog_id(JTBC 뉴스 방송 프로그램)와 매핑함
#         - DB에서는 prog_date(YYYYMMDD)를 기준으로 reg_date, mod_date만 존재하는데 시간에 대한 규칙성이 전혀 없음.
#
#         :param target_date: 실행시점 datetime
#         :return:
#         """
#         flag = True
#
#         my_helper = Helper()
#         try:
#
#             self.logger.info("="*100)
#             self.logger.info(f"started job({type(self).__name__}) > target_date = {target_date.strftime('%Y-%m-%d %H:%M:%S')}")
#
#             start = time.time()
#
#             tvpro_info = TVProInfo(helper=my_helper, level=my_helper.build_level)
#             tvpro_info.do_update_mapping_tv_news_program(the_date=target_date)
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
    # runner = MappingJTBCTVProRunner()
    # target_date = datetime.now()
    # runner.run(target_date = target_date)
    flag = runner(target_date=datetime.now())
    print("="*100)
    print(flag)