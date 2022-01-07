import sys
import traceback
from typing import AnyStr
from datetime import datetime
from util.helper import Helper
from base.constVar import *
import os
from util.JtbcLogger import JtbcLogger
from pathlib import Path

class DetailNewsInfo:

	def __init__(self, helper:Helper):
		self.helper = helper
		self.jtbc_news_db = helper.get_news_db()

		if os.path.isdir('/box/jnd-batch'):
			# prod
			path = '/box/jnd-batch'
		else:
			# dev
			path= Path(__file__).parent.parent.parent

		logger_factory = JtbcLogger(path=os.path.join(path, "logs"), file_name=type(self).__name__)
		self.logger = logger_factory.logger

	def __del__(self):
		self.jtbc_news_db.close()

	def get_vi_elk_news_basic_by_service_dt(self, end_time:datetime, period:str= 'H'):
		"""
		VI_ELK_NEWS_BASIC의 service_dt에 있는 데이터를 가져옴
		service_dt의 기준은 end_time을 기준으로 해당 시간대 (예: 00:00 ~ 59:59)에 있는 데이터를 의미
		:param end_time: datetime
		:parm period: D or H (D:day, H:hour)
		:return:
			(list) VI_ELK_NEWS_BASIC view 데이터
			#       NEWS_ID                                  TITLE NEWS_SECTION SECTION_NAME SOURCE_CODE SOURCE_NAME    SERVICE_DT                    REPORTER_INFO                  MOD_DT
			# 0  NB12027418  남욱 "대장동 사업비용만 600억 써…돈 준 내역 있다"        30           사회           11          JTBC        202110190758  363,박병현,40,내셔널부                 NaT
			# 1  NB12027421  남욱 "'그분' 이재명은 아냐"…기획 입국 의혹 제기도         30           사회           11          JTBC        202110190755  110,정종문,4,사회부                   2021-10-19 07:57:03.453
			# 2  NB12027422  '이재명 국감' 대장동 충돌…"초과이익환수, 지침 따른 것"      10           정치           11          JTBC        202110190752  461,고승혁,1,정치부                   NaT

		"""
		result = {}
		db_cursor = self.jtbc_news_db.cursor(as_dict=True)

		# 이미 앞에서 end_timedms 59:59:9999로 셋팅되어 있음
		if period == "H":
			start_time = end_time.replace(minute=0, second=0)
		else:
			start_time = end_time.replace(hour=0, minute=0, second=0)
		start_time_str = start_time.strftime("%Y%m%d%H%M")
		end_time_str = end_time.strftime("%Y%m%d%H%M")
		self.logger.info(f"start_time={start_time.strftime('%Y-%m-%d %H:%M')}, end_time={end_time.strftime('%Y-%m-%d %H:%M')}")

		try:
			sql = f"""
			SELECT NEWS_ID, TITLE, NEWS_SECTION, SECTION_NAME, SOURCE_CODE, SOURCE_NAME, SERVICE_DT, REPORTER_INFO, MOD_DT
					FROM VI_ELK_NEWS_BASIC
					where service_dt between {start_time_str} and {end_time_str} and 
					source_code in ({SOURCE_CODE_JTBC}, {SOURCE_CODE_JAM})
					ORDER BY SERVICE_DT DESC
			"""
			self.logger.info(f"query = {sql}")

			db_cursor.execute(sql)
			result = db_cursor.fetchall()

		except Exception as es:
			raise
		finally:
			db_cursor.close()

		self.logger.info(f"{str(len(result)) if result else str(0)} new news was created "
						 f"between {start_time.strftime('%Y-%m-%d %H:%M')} and {end_time.strftime('%Y-%m-%d %H:%M')}")

		return result


	def getNewsDataById(self, id_list: list):
		news_dict = dict()
		db_cursor = self.jtbc_news_db.cursor(as_dict=True)
		try:
			if id_list:
				q = "','".join(id_list)
				q = "'{}'".format(q)
				sql = """
					SELECT NEWS_ID, TITLE, NEWS_SECTION, SECTION_NAME, SOURCE_CODE, SOURCE_NAME, SERVICE_DT, REPORTER_INFO, MOD_DT
					FROM VI_ELK_NEWS_BASIC
					WHERE NEWS_ID IN ({})
					ORDER BY SERVICE_DT DESC
				""".format(q)
				# self.logger.info(sql)

				db_cursor.execute(sql)
				news_set = db_cursor.fetchall()

				for idx, val in enumerate(news_set):
					news_dict[val['NEWS_ID']] = val
			return True, news_dict
		except Exception:
			# exc_type, exc_value, exc_traceback = sys.exc_info()
			# msg = repr(traceback.format_exception(exc_type, exc_value, exc_traceback))
			# self.logger.exception(msg)
			# self.logger.error(f"ERROR: {traceback.format_exc()}")
			raise
			# return False, news_dict
		finally:
			db_cursor.close()

	# noinspection PyMethodMayBeStatic
	def parseSourceCode(self, source_code, source_name):
		source_set = {
			"id": source_code,
			"name": source_name
		}
		return source_set

	# noinspection PyMethodMayBeStatic
	def parseSection(self, section_code, section_name):
		section_set = {
			"id": section_code,
			"name": section_name
		}
		return section_set

	# noinspection PyMethodMayBeStatic
	def parseReporter(self, reporter_raw: AnyStr) -> list:
		reporter_list = list()
		try:
			if '|' in reporter_raw:
				rp = reporter_raw.split('|')
				for idx, val in enumerate(rp):
					split_val = val.split(',')
					reporter = {
						"no": split_val[0],
						"name": split_val[1],
						"depart_no": split_val[2],
						"depart_name": split_val[3]
					}
					reporter_list.append(reporter)
			elif reporter_raw != '':
				split_val = reporter_raw.split(',')
				reporter = {
					"no": split_val[0],
					"name": split_val[1],
					"depart_no": split_val[2],
					"depart_name": split_val[3]
				}
				reporter_list.append(reporter)
		except Exception as es:
			raise
		return reporter_list

