from _datetime import datetime
import sys
import traceback
# import logging
from elasticsearch.helpers import bulk
from random import *
from base.baseData import BaseData
from main.aggregator.JTBCData import JTBCData
from base import constVar
# sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
import os
from util.JtbcLogger import JtbcLogger
from pathlib import Path

# noinspection DuplicatedCode
class KakaoData(BaseData):
	def __init__(self, helper):
		self.helper = helper
		self.es_client = helper.get_es()

		if os.path.isdir('/box/jnd-batch'):
			# prod
			path = '/box/jnd-batch'
		else:
			# dev
			path = Path(__file__).parent.parent.parent

		logger_factory = JtbcLogger(path=os.path.join(path, "logs"), file_name=type(self).__name__)
		self.logger = logger_factory.logger

	def getDataListHourly(self, the_date: datetime = None):
		hourly_set = dict()
		try:
			body = {
				"size": 0,
				"query": {
					"bool": {
						"must": [
							{
								"range": {
									"reg_date": {
										"gte": the_date.strftime('%Y-%m-%dT%H:00:00+09:00'),
										"lte": the_date.strftime('%Y-%m-%dT%H:59:59+09:00')
									}
								}
							}
						],
						"must_not": [
							{
								"term": {
									"news_id": ""
								}
							}
						]
					}
				},
				"aggs": {
					"by_news_id": {
						"terms": {
							"field": "news_id",
							"size": 999999999
						},
						"aggs": {
							"info": {
								"top_hits": {
									"size": 1,
									"_source": {
										"includes": ["news_nm", "pc_inc", "mobile_inc", "reaction_inc", "comment_inc", "total_inc"]
									}
								}
							}
						}
					}
				}
			}

			if self.es_client.indices.exists(constVar.DAUM_NEWS_HOUR_STAT):
				response = self.es_client.search(
					index=constVar.DAUM_NEWS_HOUR_STAT,
					body=body
				)

				for data in response['aggregations']['by_news_id']['buckets']:
					news_id = data['key']
					tpl = {
						'news_name': data['info']['hits']['hits'][0]['_source'].get('news_nm'),
						'pc_view': data['info']['hits']['hits'][0]['_source']['pc_inc'],
						'mobile_view': data['info']['hits']['hits'][0]['_source']['mobile_inc'],
						'view': data['info']['hits']['hits'][0]['_source']['total_inc'],
						'reaction': data['info']['hits']['hits'][0]['_source']['reaction_inc'],
						'comment': data['info']['hits']['hits'][0]['_source']['comment_inc']
					}
					hourly_set[news_id] = tpl
			else:
				self.logger.warning(f"index={constVar.DAUM_NEWS_HOUR_STAT} doesn't exist")
		except Exception:
			# exc_type, exc_value, exc_traceback = sys.exc_info()
			# msg = repr(traceback.format_exception(exc_type, exc_value, exc_traceback))
			# logging.exception(msg)
			# self.logger.error(f"ERROR: {traceback.format_exc()}")
			# return False, hourly_set
			raise
		return True, hourly_set

	def getDataListDaily(self, the_date: datetime = None):
		# 동일한 news_id로 데이터 두개 발생 고려(news_id 그룹핑)
		daily_set = dict()
		try:
			body = {
				"size": 0,
				"query": {
					"bool": {
						"must": [
							{
								"range": {
									"reg_date": {
										"gte": the_date.strftime('%Y-%m-%dT00:00:00+09:00'),
										"lte": the_date.strftime('%Y-%m-%dT23:59:59+09:00')
									}
								}
							}
						],
						"must_not": [
							{
								"term": {
									"news_id": ""
								}
							}
						]
					}
				},
				"aggs": {
					"by_news_id": {
						"terms": {
							"field": "news_id",
							"size": 999999999
						},
						"aggs": {
							"info": {
								"top_hits": {
									"size": 1,
									"_source": {
										"includes": ["news_nm", "pc_inc", "mobile_inc", "reaction_inc", "comment_inc", "total_inc"]
									}
								}
							}
						}
					}
				}
			}

			if self.es_client.indices.exists(constVar.DAUM_NEWS_DAY_STAT):
				response = self.es_client.search(
					index=constVar.DAUM_NEWS_DAY_STAT,
					body=body
				)

				for data in response['aggregations']['by_news_id']['buckets']:
					news_id = data['key']
					tpl = {
						'pc_view': data['info']['hits']['hits'][0]['_source']['pc_inc'],
						'mobile_view': data['info']['hits']['hits'][0]['_source']['mobile_inc'],
						'view': data['info']['hits']['hits'][0]['_source']['total_inc'],
						'reaction': data['info']['hits']['hits'][0]['_source']['reaction_inc'],
						'comment': data['info']['hits']['hits'][0]['_source']['comment_inc']
					}
					daily_set[news_id] = tpl
			else:
				self.logger.warning(f"index={constVar.DAUM_NEWS_DAY_STAT} doesn't exist")
		except Exception:
			# exc_type, exc_value, exc_traceback = sys.exc_info()
			# msg = repr(traceback.format_exception(exc_type, exc_value, exc_traceback))
			# logging.exception(msg)
			# self.logger.error(f"ERROR: {traceback.format_exc()}")
			# return False, daily_set
			raise
		return True, daily_set

	def aggrNewsDataById(self, the_date: datetime = None):
		return super().aggrNewsDataById(the_date)

	def pushDayTempData(self, the_date: datetime = None):
		try:
			jd = JTBCData(helper=self.helper)
			rt, dt = jd.getDataListDaily(the_date=the_date)

			data_list = list()
			for key, value in dt.items():
				mobile_view = randint(75, 150)
				pc_view = randint(50, 100)
				reaction = randint(0, 25)
				comment = randint(0, 50)
				data = {
					"_op_type": "update",
					"_id": '{}_{}'.format(the_date.strftime('%Y%m%d'), key),
					"doc_as_upsert": True,
					"doc": {
						'reg_date': the_date.strftime('%Y-%m-%dT00:00:00+09:00'),
						'news_id': key,
						'news_nm': value['news_nm'],
						'section': '',
						'service_date': None,
						'url': '',
						'jtbc_url': 'https://mnews.jtbc.joins.com/News/Article.aspx?news_id={}'.format(key),
						'aid': '',
						'oid': '',
						'pc_inc': pc_view,
						'pc_cum': 0,
						'mobile_inc': mobile_view,
						'mobile_cum': 0,
						'total_inc': int(pc_view) + int(mobile_view),
						'total_cum': 0,
						'reaction_inc': reaction,
						'reaction_cum': 0,
						'comment_inc': comment,
						'comment_cum': 0
					}
				}
				data_list.append(data)

			bulk(self.es_client, data_list, index=constVar.DAUM_NEWS_DAY_STAT)
		except Exception:
			raise

