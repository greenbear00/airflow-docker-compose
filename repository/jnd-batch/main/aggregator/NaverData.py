from _datetime import datetime
import sys
import traceback
from elasticsearch.helpers import bulk
from random import *
from base.baseData import BaseData
from main.aggregator.JTBCData import JTBCData
from base import constVar
# sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from util.helper import Helper
# import logging
import os
from util.JtbcLogger import JtbcLogger
from pathlib import Path

# noinspection DuplicatedCode
class NaverData(BaseData):
	def __init__(self, helper:Helper):
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
										"includes": ["news_nm", "pc_inc", "mobile_inc", "reaction_inc", "comment_inc", "total_inc", "click_inc"]
									}
								}
							}
						}
					}
				}
			}

			if self.es_client.indices.exists(constVar.NAVER_NEWS_HOUR_STAT):
				response = self.es_client.search(
					index=constVar.NAVER_NEWS_HOUR_STAT,
					body=body
				)

				for data in response['aggregations']['by_news_id']['buckets']:
					news_id = data['key']

					# issue: 개발망(naver-news-hour-stat/_search)에서
					# 일부 데이터는 pc_inc, mobile_inc, total_inc, reaction_inc, comment_inc 데이터가 없음
					# solution: .get으로 변환
					tpl = {
						'comment': 0,
						'mobile_view': 0,
						'pc_view': 0,
						'reaction': 0,
						'view': 0,
						'click': 0
					}

					if 'pc_inc' in data['info']['hits']['hits'][0]['_source']:
						tpl.update({
							'pc_view': data['info']['hits']['hits'][0]['_source']['pc_inc']
						})

					if 'mobile_inc' in data['info']['hits']['hits'][0]['_source']:
						tpl.update({
							'mobile_view': data['info']['hits']['hits'][0]['_source']['mobile_inc']
						})

					if 'total_inc' in data['info']['hits']['hits'][0]['_source']:
						tpl.update({
							'view': data['info']['hits']['hits'][0]['_source']['total_inc']
						})

					if 'reaction_inc' in data['info']['hits']['hits'][0]['_source']:
						tpl.update({
							'reaction': data['info']['hits']['hits'][0]['_source']['reaction_inc']
						})

					if 'comment_inc' in data['info']['hits']['hits'][0]['_source']:
						tpl.update({
							'comment': data['info']['hits']['hits'][0]['_source']['comment_inc']
						})

					if 'click_inc' in data['info']['hits']['hits'][0]['_source']:
						tpl.update({
							'click': data['info']['hits']['hits'][0]['_source']['click_inc']
						})

					hourly_set[news_id] = tpl
			else:
				self.logger.warning(f"index={constVar.NAVER_NEWS_HOUR_STAT} doesn't exist")
		except Exception:
		# 	exc_type, exc_value, exc_traceback = sys.exc_info()
		# 	msg = repr(traceback.format_exception(exc_type, exc_value, exc_traceback))
		# 	logging.exception(msg)
		# 	return False, hourly_set
		# except Exception as es:
		# 	raise es
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
										"includes": ["news_nm", "pc_inc", "mobile_inc", "reaction_inc", "comment_inc", "total_inc", "click_inc"]
									}
								}
							}
						}
					}
				}
			}

			if self.es_client.indices.exists(constVar.NAVER_NEWS_DAY_STAT):
				response = self.es_client.search(
					index=constVar.NAVER_NEWS_DAY_STAT,
					body=body
				)

				for data in response['aggregations']['by_news_id']['buckets']:
					news_id = data['key']
					tpl = {
						# 'pc_view': data['info']['hits']['hits'][0]['_source'].get('pc_inc'),
						# 'mobile_view': data['info']['hits']['hits'][0]['_source'].get('mobile_inc'),
						# 'view': data['info']['hits']['hits'][0]['_source'].get('total_inc'),
						# 'reaction': data['info']['hits']['hits'][0]['_source'].get('reaction_inc'),
						# 'comment': data['info']['hits']['hits'][0]['_source'].get('comment_inc')
						'pc_view': 0,
						'mobile_view': 0,
						'view': 0,
						'reaction': 0,
						'comment': 0,
						'click': 0
					}

					if 'pc_inc' in data['info']['hits']['hits'][0]['_source']:
						tpl.update({
							'pc_view': data['info']['hits']['hits'][0]['_source']['pc_inc']
						})

					if 'mobile_inc' in data['info']['hits']['hits'][0]['_source']:
						tpl.update({
							'mobile_view': data['info']['hits']['hits'][0]['_source']['mobile_inc']
						})

					if 'total_inc' in data['info']['hits']['hits'][0]['_source']:
						tpl.update({
							'view': data['info']['hits']['hits'][0]['_source']['total_inc']
						})

					if 'reaction_inc' in data['info']['hits']['hits'][0]['_source']:
						tpl.update({
							'reaction': data['info']['hits']['hits'][0]['_source']['reaction_inc']
						})

					if 'comment_inc' in data['info']['hits']['hits'][0]['_source']:
						tpl.update({
							'comment': data['info']['hits']['hits'][0]['_source']['comment_inc']
						})

					if 'click_inc' in data['info']['hits']['hits'][0]['_source']:
						tpl.update({
							'click': data['info']['hits']['hits'][0]['_source']['click_inc']
						})

					daily_set[news_id] = tpl
			else:
				self.logger.warning(f"index={constVar.NAVER_NEWS_DAY_STAT} doesn't exist")
		except Exception:
			# exc_type, exc_value, exc_traceback = sys.exc_info()
			# msg = repr(traceback.format_exception(exc_type, exc_value, exc_traceback))
			# logging.exception(msg)
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
				mobile_view = randint(150, 300)
				pc_view = randint(100, 200)
				reaction = randint(1, 50)
				comment = randint(1, 100)
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

			bulk(self.es_client, data_list, index=constVar.NAVER_NEWS_DAY_STAT)
		except Exception:
			raise


