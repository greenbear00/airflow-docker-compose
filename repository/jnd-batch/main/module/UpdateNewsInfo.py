from _datetime import datetime
import sys
import traceback

from main.aggregator.JTBCData import JTBCData
from main.module.DetailNewsInfo import DetailNewsInfo
from base import constVar
from elasticsearch.helpers import bulk
from pathlib import Path
import os

from main.module.TVProInfo import TVProInfo
from util.JtbcLogger import JtbcLogger

class UpdateNewsInfo:
	def __init__(self, helper):
		self.helper = helper
		self.es_client = helper.get_es()
		self.jtbc_news_db = helper.get_news_db()
		self.detailNewsInfo = DetailNewsInfo(helper)
		self.jtbcdata = JTBCData(helper=helper)
		self.tvpro = TVProInfo(helper=helper, level=helper.build_level)

		if os.path.isdir('/box/jnd-batch'):
			# prod
			path = '/box/jnd-batch'
		else:
			# dev
			path = Path(__file__).parent.parent.parent

		logger_factory = JtbcLogger(path=os.path.join(path, "logs"), file_name=type(self).__name__)
		self.logger = logger_factory.logger

	def __del__(self):
		self.es_client.transport.close()
		self.jtbc_news_db.close()

	def getNewsData(self, last_time: datetime = None):
		try:
			if last_time is None:
				raise Exception('날짜 지정 필수!')

			#  AND NEWS_ID = 'NB10545079
			start_time_str = last_time.replace(hour=0, minute=0).strftime("%Y%m%d%H%M")
			end_time_str = last_time.replace(hour=23, minute=59).strftime("%Y%m%d%H%M")
			# mod_dt_str = last_time.strftime("%Y-%m-%d %H:%M:%S")
			# service_dt_str = last_time.strftime('%Y%m%d%H%M')

			# 기존 쿼리 WHERE (MOD_DT >= '{0}' OR SERVICE_DT >='{1}')
			sql = f"""
				SELECT NEWS_ID, USED_YN, NEWS_SECTION, SECTION_NAME, SOURCE_CODE, SOURCE_NAME
				,TITLE, REPORTER_INFO, SERVICE_DT, MOD_DT
				FROM VI_ELK_NEWS_BASIC
				WHERE SERVICE_DT between {start_time_str} and {end_time_str}
				ORDER BY SERVICE_DT DESC
			"""
			#  AND NEWS_ID = 'NB12025213'
			# 2011-11-28 20:55:51.470 (수정된 최초 날짜)
			self.logger.info(sql)

			db_cursor = self.jtbc_news_db.cursor(as_dict=True)
			db_cursor.execute(sql)
			news_set = db_cursor.fetchall()
			return True, news_set
		except Exception:
			# exc_type, exc_value, exc_traceback = sys.exc_info()
			# msg = repr(traceback.format_exception(exc_type, exc_value, exc_traceback))
			# logging.exception(msg)
			# self.logger.error(f"ERROR: {traceback.format_exc()}")
			# db_cursor.close()
			raise
			# return False, news_set
		finally:
			# raise로 올려도 finally로 db close가 처리 됨
			# self.logger.info("closed....")
			db_cursor.close()

	def UpdateNewsSummary(self, news_set: list, last_time: datetime):
		"""
		NewsSummary의 경우, 한시간 단위로 집계하기 때문에
		해당 시간대에 view에서 source_code, news_secion, reporter_info, service_dt가 변경될 수 있으니
		이를 반영해달라는 니즈가 있어서 수행
		:param news_set:
		:param last_time:
		:return:
		"""
		update_news_ids = list()
		try:
			for idx, val in enumerate(news_set):
				source = self.detailNewsInfo.parseSourceCode(val['SOURCE_CODE'], val['SOURCE_NAME'])
				section = self.detailNewsInfo.parseSection(val['NEWS_SECTION'], val['SECTION_NAME'])
				reporter = self.detailNewsInfo.parseReporter(val['REPORTER_INFO'])
				service_date = datetime.strptime(val['SERVICE_DT'], '%Y%m%d%H%M')
				# print(val['SERVICE_DT'], service_date)

				# 당일 시간 대에 해당하는 데이터만 업데이트
				# ctx._source.reporter가 params의 key 이고,
				# params.reporter이 내가 변경할 값임
				query = {
					"script": {
						"source": """
							ctx._source.source=params.source;ctx._source.news_name=params.news_name;
							ctx._source.section=params.section;ctx._source.service_date=params.service_date;
							ctx._source.reporter=params.reporter
							""",
						"lang": "painless",
						"params": {
							"source": source,
							"news_name": str(val['TITLE']).strip(),
							"section": section,
							"service_date": "{}".format(service_date.strftime('%Y-%m-%dT%H:%M:%S+09:00')),
							"reporter": reporter
						}
					},
					"query": {
						"bool": {
							"must": [
								{
									"range": {
										"reg_date": {
											"gte": "{}".format(last_time.strftime('%Y-%m-%dT00:00:00+09:00'))
										}
									}
								},
								{
									"match": {
										"news_id": val['NEWS_ID']
									}
								}
							]
						}
					}
				}

				if self.es_client.indices.exists(constVar.HOURLY_NEWS_SUMMARY):
					self.es_client.update_by_query(
						index=constVar.HOURLY_NEWS_SUMMARY,
						body=query,
						doc_type=''
					)
				else:
					self.logger.warning(f"index={constVar.HOURLY_NEWS_SUMMARY} doesn't exist")

				if self.es_client.indices.exists(constVar.DAILY_NEWS_SUMMARY):
					self.es_client.update_by_query(
						index=constVar.DAILY_NEWS_SUMMARY,
						body=query,
						doc_type=''
					)
				else:
					self.logger.warning(f"index={constVar.DAILY_NEWS_SUMMARY} doesn't exist")

				update_news_ids.append(val['NEWS_ID'])

			return True, update_news_ids
		except Exception:
			# exc_type, exc_value, exc_traceback = sys.exc_info()
			# msg = repr(traceback.format_exception(exc_type, exc_value, exc_traceback))
			# logging.exception(msg)
			# self.logger.error(f"ERROR: {traceback.format_exc()}")
			raise

			# return False, update_news_ids

	def repeat_upsert_newsbasicinfo(self, the_date:datetime, index_name:str):
		try:
			_, news_set = self.getNewsData(last_time=the_date)
			if news_set:
				self.UpsertNewsBasicInfo(news_set=news_set, the_date=the_date, index_name=index_name)
			return True
		except Exception:
			raise

	def UpsertNewsBasicInfo(self, news_set: list, the_date:datetime=datetime.now(), index_name:str=None):
		"""

		:param news_set: DB 중 View에 해당하는 정보임
		:return:
		"""
		upsert_list = list()
		new_mapping_jtbc_map = {}
		try:

			news_ids = [a_data.get('NEWS_ID') for a_data in news_set]
			mapping_prog_ids = self.jtbcdata.collect_jtbc_tv_prog_id_by_news_ids(news_ids=news_ids, the_date=the_date)
			unique_prog_ids = list(set([a_data.get('prog_id') for a_data in mapping_prog_ids]))
			self.logger.info(f"news_id's unique_prog_ids ({the_date.strftime('%Y-%m-%d')}): {unique_prog_ids}")
			if unique_prog_ids:
				unique_prog_ids_info = self.tvpro.get_nw_tv_program_by_prog_ids(prog_ids=unique_prog_ids)
				new_mapping_jtbc_map = self.tvpro.update_prog_id_for_news_ids(mapping_news_ids=mapping_prog_ids,
																			  prod_ids_info=unique_prog_ids_info)

			self.logger.info(f"mapping news_ids for prog_id: {new_mapping_jtbc_map.keys()}")
			update_date = datetime.now().strftime('%Y-%m-%dT%H:%M:%S+09:00')
			for idx, val in enumerate(news_set):
				source = self.detailNewsInfo.parseSourceCode(val['SOURCE_CODE'], val['SOURCE_NAME'])
				section = self.detailNewsInfo.parseSection(val['NEWS_SECTION'], val['SECTION_NAME'])
				reporter = self.detailNewsInfo.parseReporter(val['REPORTER_INFO'])
				service_date = datetime.strptime(val['SERVICE_DT'], '%Y%m%d%H%M')

				modified_date = None
				if val['MOD_DT'] is not None:
					modified_date = val['MOD_DT'].strftime('%Y-%m-%dT%H:%M:%S+09:00')

				if new_mapping_jtbc_map.get(val.get('NEWS_ID')):
					raw = {
						# "news_id": val['NEWS_ID'],
						**new_mapping_jtbc_map.get(val.get('NEWS_ID')),
						"news_name": val['TITLE'],
						"used_yn": val['USED_YN'],
						"source": source,
						"section": section,
						"reporter": reporter,
						"service_date": service_date.strftime('%Y-%m-%dT%H:%M:%S+09:00'),
						"modified_date": modified_date,
						"update_date": update_date
					}
				else:
					raw = {
						"news_id": val['NEWS_ID'],
						"news_name": val['TITLE'],
						"used_yn": val['USED_YN'],
						"source": source,
						"section": section,
						"reporter": reporter,
						"service_date": service_date.strftime('%Y-%m-%dT%H:%M:%S+09:00'),
						"modified_date": modified_date,
						"update_date": update_date
					}

				# key = news_id
				index_id = "{0}".format(val['NEWS_ID'])
				data_set = {
					"_op_type": "update",
					"_id": index_id,
					"doc_as_upsert": True,
					"doc": raw
				}
				upsert_list.append(data_set)

			if index_name is None:
				bulk(self.es_client, upsert_list, index=constVar.BASIC_NEWS_INFO, refresh="wait_for")
			else:
				bulk(self.es_client, upsert_list, index=index_name, refresh="wait_for")

		except Exception:
			# exc_type, exc_value, exc_traceback = sys.exc_info()
			# msg = repr(traceback.format_exception(exc_type, exc_value, exc_traceback))
			# logging.exception(msg)
			# self.logger.error(f"ERROR: {traceback.format_exc()}")

			# return False, upsert_list
			raise
		return True, upsert_list
