from _datetime import datetime
from base import constVar
from typing import AnyStr
import sys
import traceback
from elasticsearch.helpers import bulk
from main.module.DetailNewsInfo import DetailNewsInfo
from util.JtbcLogger import JtbcLogger
import os
from pathlib import Path

# noinspection DuplicatedCode
class ProduceNewsSummary:
	def __init__(self, helper):
		self.helper = helper
		self.es_client = helper.get_es()
		self.detailNewsInfo = DetailNewsInfo(helper)

		if os.path.isdir('/box/jnd-batch'):
			# prod
			path = '/box/jnd-batch'
		else:
			# dev
			path = Path(__file__).parent.parent.parent

		logger_factory = JtbcLogger(path=os.path.join(path, "logs"), file_name=type(self).__name__)
		self.logger = logger_factory.logger

	# noinspection PyMethodMayBeStatic
	def matchReporterInfo(self, reporter_no: AnyStr, reporter_info: list):
		match = list(filter(lambda x: x['no'] == reporter_no, reporter_info))
		return match

	# noinspection PyMethodMayBeStatic
	def matchDepartmentInfo(self, depart_no: AnyStr, reporter_info: list):
		for item in reporter_info:
			if depart_no == item['depart_no']:
				return {
					"depart_no": item['depart_no'],
					"depart_name": item['depart_name']
				}
			else:
				continue
		return {}

	# noinspection PyMethodMayBeStatic
	def aggrProduceByReporter(self, the_date: datetime = None):
		reporter_dict = dict()
		daily_set = list()

		try:
			body = {
				"size": 0,
				"_source": False,
				"query": {
					"range": {
						"service_date": {
							"gte": the_date.strftime('%Y-%m-%dT00:00:00+09:00'),
							"lte": the_date.strftime('%Y-%m-%dT23:59:59+09:00')
						}
					}
				},
				"aggs": {
					"reporter": {
						"terms": {
							"field": "reporter.no",
							"size": 999999999
						},
						"aggs": {
							"reporter_info": {
								"top_hits": {
									"size": 1,
									"_source": {
										"includes": ["reporter.no", "reporter.name", "reporter.depart_no",
													 "reporter.depart_name"]
									}
								}
							},
							"jtbc": {
								"filter": {
									"term": {
										"source.id": "11"
									}
								},
								"aggs": {
									"produce": {
										"value_count": {
											"field": "news_id"
										}
									}
								}
							},
							"jam": {
								"filter": {
									"term": {
										"source.id": "17"
									}
								},
								"aggs": {
									"produce": {
										"value_count": {
											"field": "news_id"
										}
									}
								}
							},
							"total": {
								"filter": {
									"terms": {
										"source.id": [
											"11",
											"17"
										]
									}
								},
								"aggs": {
									"produce": {
										"value_count": {
											"field": "news_id"
										}
									}
								}
							}
						}
					}
				}
			}
			if self.es_client.indices.exists(constVar.BASIC_NEWS_INFO):
				response = self.es_client.search(
					index=constVar.BASIC_NEWS_INFO,
					body=body
				)
				for item in response['aggregations']['reporter']['buckets']:
					# print(item['key'], item['reporter_info']['hits']['hits'][0]['_source']['reporter'])
					reporter = self.matchReporterInfo(item['key'], item['reporter_info']['hits']['hits'][0]['_source']['reporter'])
					raw = {
						"reg_date": the_date.strftime('%Y-%m-%dT00:00:00+09:00'),
						"reporter": reporter,
						"jtbc": {
							"jtbc_produce": item['jtbc']['produce']['value'],
							"jam_produce": item['jam']['produce']['value'],
							"produce": item['total']['produce']['value']
						},
						"naver": {},
						"kakao": {}
					}
					reporter_dict[item['key']] = raw
			else:
				self.logger.warning(f"index={constVar.BASIC_NEWS_INFO} doesn't exist")

			body2 = {
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
							},
							{
								"terms": {
									"reporter.no": list(reporter_dict.keys())
								}
							}
						]
					}
				},
				"aggs": {
					"reporter": {
						"terms": {
							"field": "reporter.no",
							"size": 999999999
						},
						"aggs": {
							"jtbc.view": {
								"sum": {
									"field": "jtbc.view"
								}
							},
							"jtbc.reaction": {
								"sum": {
									"field": "jtbc.reaction"
								}
							},
							"jtbc.comment": {
								"sum": {
									"field": "jtbc.comment"
								}
							},
							"naver.view": {
								"sum": {
									"field": "naver.view"
								}
							},
							"naver.reaction": {
								"sum": {
									"field": "naver.reaction"
								}
							},
							"naver.comment": {
								"sum": {
									"field": "naver.comment"
								}
							},
							"kakao.view": {
								"sum": {
									"field": "kakao.view"
								}
							},
							"kakao.comment": {
								"sum": {
									"field": "kakao.comment"
								}
							}
						}
					}
				}
			}
			if self.es_client.indices.exists(constVar.HOURLY_NEWS_SUMMARY):
				response2 = self.es_client.search(
					index=constVar.HOURLY_NEWS_SUMMARY,
					body=body2
				)
				subset_dict = dict()
				# 기사 상세지표(hourly-news-summary) 일별 집계
				for item3 in response2['aggregations']['reporter']['buckets']:
					subset_dict[item3['key']] = item3
			else:
				self.logger.warning(f"index={constVar.HOURLY_NEWS_SUMMARY} doesn't exist")

			# 생산량 발생 기준으로 집계
			for key, value in reporter_dict.items():
				view_total = 0
				reaction_total = 0
				comment_total = 0

				if key in subset_dict.keys():
					subset_info = subset_dict[key]
					reporter_dict[key]['jtbc'].update({
						"comment": int(subset_info['jtbc.comment']['value']),
						"view": int(subset_info['jtbc.view']['value']),
						"reaction": int(subset_info['jtbc.reaction']['value'])
					})
					view_total += int(subset_info['jtbc.view']['value'])
					reaction_total += int(subset_info['jtbc.reaction']['value'])
					comment_total += int(subset_info['jtbc.comment']['value'])

					reporter_dict[key]['naver'].update({
						"comment": int(subset_info['naver.comment']['value']),
						"view": int(subset_info['naver.view']['value']),
						"reaction": int(subset_info['naver.reaction']['value']),
					})
					view_total += int(subset_info['naver.view']['value'])
					reaction_total += int(subset_info['naver.reaction']['value'])
					comment_total += int(subset_info['naver.comment']['value'])

					reporter_dict[key]['kakao'].update({
						"comment": int(subset_info['kakao.comment']['value']),
						"view":  int(subset_info['kakao.view']['value'])
					})
					view_total += int(subset_info['kakao.view']['value'])
					comment_total += int(subset_info['kakao.comment']['value'])

					reporter_dict[key].update(
						dict(view_total=view_total, reaction_total=reaction_total, comment_total=comment_total))

				index_id = "{0}_{1}".format(the_date.strftime('%Y%m%d'), key)
				data_set = {
					"_op_type": "update",
					"_id": index_id,
					"doc_as_upsert": True,
					"doc": reporter_dict[key]
				}
				daily_set.append(data_set)

			# 일 단위로 저장
			index_name = '{0}-{1}'.format(constVar.DAILY_PRODUCE_REPORTER, the_date.strftime('%Y'))
			bulk(self.es_client, daily_set, index=index_name)
			return True, daily_set
		except Exception:
			# exc_type, exc_value, exc_traceback = sys.exc_info()
			# msg = repr(traceback.format_exception(exc_type, exc_value, exc_traceback))
			# logging.exception(msg)
			# self.logger.error(f"ERROR: {traceback.format_exc()}")
			# return False, daily_set
			raise

	# noinspection PyMethodMayBeStatic
	def aggrProduceByDepartment(self, the_date: datetime = None):
		depart_dict = dict()
		daily_set = list()

		try:
			body = {
				"size": 0,
				"_source": False,
				"query": {
					"range": {
						"service_date": {
							"gte": the_date.strftime('%Y-%m-%dT00:00:00+09:00'),
							"lte": the_date.strftime('%Y-%m-%dT23:59:59+09:00')
						}
					}
				},
				"aggs": {
					"department": {
						"terms": {
							"field": "reporter.depart_no",
							"size": 999999999
						},
						"aggs": {
							"reporter_info": {
								"top_hits": {
									"size": 1,
									"_source": {
										"includes": ["reporter.depart_no", "reporter.depart_name"]
									}
								}
							},
							"jtbc": {
								"filter": {
									"term": {
										"source.id": "11"
									}
								},
								"aggs": {
									"produce": {
										"value_count": {
											"field": "news_id"
										}
									}
								}
							},
							"jam": {
								"filter": {
									"term": {
										"source.id": "17"
									}
								},
								"aggs": {
									"produce": {
										"value_count": {
											"field": "news_id"
										}
									}
								}
							},
							"total": {
								"filter": {
									"terms": {
										"source.id": [
											"11",
											"17"
										]
									}
								},
								"aggs": {
									"produce": {
										"value_count": {
											"field": "news_id"
										}
									}
								}
							}
						}
					}
				}
			}
			if self.es_client.indices.exists(constVar.BASIC_NEWS_INFO):
				response = self.es_client.search(
					index=constVar.BASIC_NEWS_INFO,
					body=body
				)

				for item in response['aggregations']['department']['buckets']:
					# print(item['key'], item['reporter_info']['hits']['hits'][0]['_source']['reporter'])
					depart = self.matchDepartmentInfo(item['key'], item['reporter_info']['hits']['hits'][0]['_source']['reporter'])
					raw = {
						"reg_date": the_date.strftime('%Y-%m-%dT00:00:00+09:00'),
						"reporter": depart,
						"jtbc": {
							"jtbc_produce": item['jtbc']['produce']['value'],
							"jam_produce": item['jam']['produce']['value'],
							"produce": item['total']['produce']['value']
						},
						"naver": {},
						"kakao": {}
					}
					depart_dict[item['key']] = raw
			else:
				self.logger.warning(f"index={constVar.BASIC_NEWS_INFO} doesn't exist")

			body2 = {
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
							},
							{
								"terms": {
									"reporter.depart_no": list(depart_dict.keys())
								}
							}
						]
					}
				},
				"aggs": {
					"depart": {
						"terms": {
							"field": "reporter.depart_no",
							"size": 999999999
						},
						"aggs": {
							"jtbc.view": {
								"sum": {
									"field": "jtbc.view"
								}
							},
							"jtbc.reaction": {
								"sum": {
									"field": "jtbc.reaction"
								}
							},
							"jtbc.comment": {
								"sum": {
									"field": "jtbc.comment"
								}
							},
							"naver.view": {
								"sum": {
									"field": "naver.view"
								}
							},
							"naver.reaction": {
								"sum": {
									"field": "naver.reaction"
								}
							},
							"naver.comment": {
								"sum": {
									"field": "naver.comment"
								}
							},
							"kakao.view": {
								"sum": {
									"field": "kakao.view"
								}
							},
							"kakao.comment": {
								"sum": {
									"field": "kakao.comment"
								}
							}
						}
					}
				}
			}
			if self.es_client.indices.exists(constVar.HOURLY_NEWS_SUMMARY):
				response2 = self.es_client.search(
					index=constVar.HOURLY_NEWS_SUMMARY,
					body=body2
				)

				subset_dict = dict()
				# 기사 상세지표(hourly-news-summary) 일별 집계
				for item3 in response2['aggregations']['depart']['buckets']:
					subset_dict[item3['key']] = item3
			else:
				self.logger.warning(f"index={constVar.HOURLY_NEWS_SUMMARY} doesn't exist")

			# 생산량 발생 기준으로 집계
			for key, value in depart_dict.items():
				view_total = 0
				reaction_total = 0
				comment_total = 0

				if key in subset_dict.keys():
					subset_info = subset_dict[key]
					depart_dict[key]['jtbc'].update({
						"comment": int(subset_info['jtbc.comment']['value']),
						"view": int(subset_info['jtbc.view']['value']),
						"reaction": int(subset_info['jtbc.reaction']['value'])
					})
					view_total += int(subset_info['jtbc.view']['value'])
					reaction_total += int(subset_info['jtbc.reaction']['value'])
					comment_total += int(subset_info['jtbc.comment']['value'])

					depart_dict[key]['naver'].update({
						"comment": int(subset_info['naver.comment']['value']),
						"view": int(subset_info['naver.view']['value']),
						"reaction": int(subset_info['naver.reaction']['value']),
					})
					view_total += int(subset_info['naver.view']['value'])
					reaction_total += int(subset_info['naver.reaction']['value'])
					comment_total += int(subset_info['naver.comment']['value'])

					depart_dict[key]['kakao'].update({
						"comment": int(subset_info['kakao.comment']['value']),
						"view":  int(subset_info['kakao.view']['value'])
					})
					view_total += int(subset_info['kakao.view']['value'])
					comment_total += int(subset_info['kakao.comment']['value'])

					depart_dict[key].update(
						dict(view_total=view_total, reaction_total=reaction_total, comment_total=comment_total))

				index_id = "{0}_{1}".format(the_date.strftime('%Y%m%d'), key)
				data_set = {
					"_op_type": "update",
					"_id": index_id,
					"doc_as_upsert": True,
					"doc": depart_dict[key]
				}
				daily_set.append(data_set)

			# 일 단위로 저장
			index_name = '{0}-{1}'.format(constVar.DAILY_PRODUCE_DEPART, the_date.strftime('%Y'))
			bulk(self.es_client, daily_set, index=index_name)
			return True, daily_set

		except Exception:
			# exc_type, exc_value, exc_traceback = sys.exc_info()
			# msg = repr(traceback.format_exception(exc_type, exc_value, exc_traceback))
			# logging.exception(msg)
			# self.logger.error(f"ERROR: {traceback.format_exc()}")

			# return False, daily_set
			raise

	def aggrProduceBySection(self, the_date: datetime = None):
		section_dict = dict()
		daily_set = list()

		try:
			body = {
				"size": 0,
				"_source": False,
				"query": {
					"range": {
						"service_date": {
							"gte": the_date.strftime('%Y-%m-%dT00:00:00+09:00'),
							"lte": the_date.strftime('%Y-%m-%dT23:59:59+09:00')
						}
					}
				},
				"aggs": {
					"section": {
						"terms": {
							"field": "section.id",
							"size": 999999999
						},
						"aggs": {
							"section_info": {
								"top_hits": {
									"size": 1,
									"_source": {
										"includes": ["section.id", "section.name"]
									}
								}
							},
							"jtbc": {
								"filter": {
									"term": {
										"source.id": "11"
									}
								},
								"aggs": {
									"produce": {
										"value_count": {
											"field": "news_id"
										}
									}
								}
							},
							"jam": {
								"filter": {
									"term": {
										"source.id": "17"
									}
								},
								"aggs": {
									"produce": {
										"value_count": {
											"field": "news_id"
										}
									}
								}
							},
							"total": {
								"filter": {
									"terms": {
										"source.id": [
											"11",
											"17"
										]
									}
								},
								"aggs": {
									"produce": {
										"value_count": {
											"field": "news_id"
										}
									}
								}
							}
						}
					}
				}
			}
			if self.es_client.indices.exists(constVar.BASIC_NEWS_INFO):
				response = self.es_client.search(
					index=constVar.BASIC_NEWS_INFO,
					body=body
				)

				for item in response['aggregations']['section']['buckets']:
					# print(item['key'], item['section_info']['hits']['hits'][0]['_source']['section']['name'])
					section = self.detailNewsInfo.parseSection(item['key'], item['section_info']['hits']['hits'][0]['_source']['section']['name'])
					raw = {
						"reg_date": the_date.strftime('%Y-%m-%dT00:00:00+09:00'),
						"section": section,
						"jtbc": {
							"jtbc_produce": item['jtbc']['produce']['value'],
							"jam_produce": item['jam']['produce']['value'],
							"produce": item['total']['produce']['value']
						},
						"naver": {},
						"kakao": {}
					}
					section_dict[item['key']] = raw
			else:
				self.logger.warning(f"index={constVar.BASIC_NEWS_INFO} doesn't exist")

			body2 = {
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
							},
							{
								"terms": {
									"section.id": list(section_dict.keys())
								}
							}
						]
					}
				},
				"aggs": {
					"section": {
						"terms": {
							"field": "section.id",
							"size": 999999999
						},
						"aggs": {
							"jtbc.view": {
								"sum": {
									"field": "jtbc.view"
								}
							},
							"jtbc.reaction": {
								"sum": {
									"field": "jtbc.reaction"
								}
							},
							"jtbc.comment": {
								"sum": {
									"field": "jtbc.comment"
								}
							},
							"naver.view": {
								"sum": {
									"field": "naver.view"
								}
							},
							"naver.reaction": {
								"sum": {
									"field": "naver.reaction"
								}
							},
							"naver.comment": {
								"sum": {
									"field": "naver.comment"
								}
							},
							"kakao.view": {
								"sum": {
									"field": "kakao.view"
								}
							},
							"kakao.comment": {
								"sum": {
									"field": "kakao.comment"
								}
							}
						}
					}
				}
			}
			if self.es_client.indices.exists(constVar.HOURLY_NEWS_SUMMARY):
				response2 = self.es_client.search(
					index=constVar.HOURLY_NEWS_SUMMARY,
					body=body2
				)

				subset_dict = dict()
				# 기사 상세지표(hourly-news-summary) 일별 집계
				for item3 in response2['aggregations']['section']['buckets']:
					subset_dict[item3['key']] = item3
			else:
				self.logger.warning(f"index={constVar.HOURLY_NEWS_SUMMARY} doesn't exist")

			# 생산량 발생 기준으로 집계
			for key, value in section_dict.items():
				view_total = 0
				reaction_total = 0
				comment_total = 0

				if key in subset_dict.keys():
					subset_info = subset_dict[key]
					section_dict[key]['jtbc'].update({
						"comment": int(subset_info['jtbc.comment']['value']),
						"view": int(subset_info['jtbc.view']['value']),
						"reaction": int(subset_info['jtbc.reaction']['value'])
					})
					view_total += int(subset_info['jtbc.view']['value'])
					reaction_total += int(subset_info['jtbc.reaction']['value'])
					comment_total += int(subset_info['jtbc.comment']['value'])

					section_dict[key]['naver'].update({
						"comment": int(subset_info['naver.comment']['value']),
						"view": int(subset_info['naver.view']['value']),
						"reaction": int(subset_info['naver.reaction']['value']),
					})
					view_total += int(subset_info['naver.view']['value'])
					reaction_total += int(subset_info['naver.reaction']['value'])
					comment_total += int(subset_info['naver.comment']['value'])

					section_dict[key]['kakao'].update({
						"comment": int(subset_info['kakao.comment']['value']),
						"view":  int(subset_info['kakao.view']['value'])
					})
					view_total += int(subset_info['kakao.view']['value'])
					comment_total += int(subset_info['kakao.comment']['value'])

					section_dict[key].update(
						dict(view_total=view_total, reaction_total=reaction_total, comment_total=comment_total))

				index_id = "{0}_{1}".format(the_date.strftime('%Y%m%d'), key)
				data_set = {
					"_op_type": "update",
					"_id": index_id,
					"doc_as_upsert": True,
					"doc": section_dict[key]
				}
				daily_set.append(data_set)

			# 일 단위로 저장
			index_name = '{0}-{1}'.format(constVar.DAILY_PRODUCE_SECTION, the_date.strftime('%Y'))
			bulk(self.es_client, daily_set, index=index_name)
			return True, daily_set
		except Exception:
			# exc_type, exc_value, exc_traceback = sys.exc_info()
			# msg = repr(traceback.format_exception(exc_type, exc_value, exc_traceback))
			# logging.exception(msg)
			# self.logger.error(f"ERROR: {traceback.format_exc()}")

			# return False, daily_set

			raise
