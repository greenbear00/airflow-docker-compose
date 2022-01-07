import sys
from _datetime import datetime
import traceback
# import logging
from base.baseData import BaseData
from base import constVar
# sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
import os
from util.JtbcLogger import JtbcLogger
from pathlib import Path
import calendar
import numpy as np


def _get_view_member_info(elk_response: list) -> (int, dict, dict, dict):
	member_info = {}
	member_total = 0
	member_parm_origin_info = {'app': 0, 'pc': 0, 'mobile': 0}
	member_type_info = {}
	for data in elk_response:
		member_type = data.get('key')
		member_type_total = data.get('doc_count')
		if member_type != "" and member_type is not None:
			# member_type_total = 0
			member_type_parm_origin_info = {}
			for sub_data in (data.get('by_parm_origin') or {}).get('buckets') or []:
				param_origin_key = sub_data.get('key')
				value = sub_data.get('doc_count') or 0
				# member_data[key]=value
				# elk_data[f"member.{member_type}.{param_origin_key}"] = value
				member_type_parm_origin_info[param_origin_key] = value
				# member_type_total += value
				ori_param_total = member_parm_origin_info.get(param_origin_key) + value
				member_parm_origin_info.update({param_origin_key: ori_param_total})
			member_info[member_type] = member_type_parm_origin_info
			# elk_data[f"member_{member_type}_total"] = member_type_total
			member_type_info[f"member_{member_type}"] = member_type_total
			member_total += member_type_total
	return member_total, member_info, member_type_info, member_parm_origin_info


def _get_unique_member_info(elk_response: list) -> (int, dict, dict, dict):
	member_info = {}
	member_total = 0
	member_parm_origin_info = {'app': 0, 'pc': 0, 'mobile': 0}
	member_type_info = {}
	for data in elk_response:
		member_type = data.get('key')
		if member_type != "" and member_type is not None:
			member_type_total = 0
			member_type_parm_origin_info = {}
			for sub_data in (data.get('by_parm_origin') or {}).get('buckets') or []:
				param_origin_key = sub_data.get('key')
				value = (sub_data.get('member_count') or {}).get('value') or None
				# member_data[key]=value
				# elk_data[f"member.{member_type}.{param_origin_key}"] = value
				member_type_parm_origin_info[param_origin_key] = value
				member_type_total += value
				ori_param_total = member_parm_origin_info.get(param_origin_key) + value
				member_parm_origin_info.update({param_origin_key: ori_param_total})
			member_info[member_type] = member_type_parm_origin_info
			# elk_data[f"member_{member_type}_total"] = member_type_total
			member_type_info[f"member_{member_type}"] = member_type_total
			member_total += member_type_total
	return member_total, member_info, member_type_info, member_parm_origin_info


class JTBCData(BaseData):
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

	def getReactionHourlyMap(self, the_date: datetime = None):
		if the_date is None:
			the_date = datetime.today()
		index_name = 'jtbcnews-raw-{}'.format(the_date.strftime('%Y.%m.%d'))

		# 반응 이모티콘 클릭, 취소 건 계산
		body = {
			"_source": False,
			"size": 0,
			"query": {
				"bool": {
					"must": [
						{
							"range": {
								"reqtime": {
									"gte": the_date.strftime('%Y-%m-%dT%H:00:00+09:00'),
									"lte": the_date.strftime('%Y-%m-%dT%H:59:59+09:00')
								}
							}
						},
						{
							"terms": {
								"action_name": [
									"reaction",
									"unreaction"
								]
							}
						}
					]
				}
			},
			"aggs": {
				"news_id": {
					"terms": {
						"field": "news_id",
						"size": 999999999
					},
					"aggs": {
						"reaction": {
							"filter": {
								"term": {"action_name": "reaction"}
							},
							"aggs": {
								"in": {
									"value_count": {
										"field": "action_name"
									}
								}
							}
						},
						"unreaction": {
							"filter": {
								"term": {"action_name": "unreaction"}
							},
							"aggs": {
								"out": {
									"value_count": {
										"field": "action_name"
									}
								}
							}
						}
					}
				}
			}
		}
		my_dict = dict()
		try:

			if self.es_client.indices.exists(index=index_name):
				response = self.es_client.search(
					index=index_name,
					body=body
				)

				for data in response['aggregations']['news_id']['buckets']:
					value = {
						"in": data['reaction']['in']['value'],
						"out": data['unreaction']['out']['value'],
						"reaction": int(data['reaction']['in']['value']) - int(data['unreaction']['out']['value'])
					}
					my_dict[str(data['key']).upper()] = value
			else:
				self.logger.warning(f"index={index_name} doesn't exist.")
		except Exception:
			raise

		return my_dict

	def getCommentHourlyMap(self, the_date: datetime = None):
		if the_date is None:
			raise Exception('날짜 지정 필수!')

		index_name = 'jtbcnews-raw-{}'.format(the_date.strftime('%Y.%m.%d'))

		# 댓글 등록, 삭제 건 반영
		body = {
			"query": {
				"bool": {
					"must": [
						{
							"range": {
								"reqtime": {
									"gte": the_date.strftime('%Y-%m-%dT%H:00:00+09:00'),
									"lte": the_date.strftime('%Y-%m-%dT%H:59:59+09:00')
								}
							}
						},
						{
							"terms": {
								"action_name": [
									"comment",
									"uncomment"
								]
							}
						}
					]
				}
			},
			"_source": False,
			"size": 0,
			"aggs": {
				"news_id": {
					"terms": {
						"field": "news_id",
						"size": 999999999
					},
					"aggs": {
						"comment": {
							"filter": {"term": {"action_name": "comment"}},
							"aggs": {
								"in": {
									"value_count": {
										"field": "action_name"
									}
								}
							}
						},
						"uncomment": {
							"filter": {"term": {"action_name": "uncomment"}},
							"aggs": {
								"out": {
									"value_count": {
										"field": "action_name"
									}
								}
							}
						}
					}
				}
			}
		}

		my_dict = dict()
		try:
			if self.es_client.indices.exists(index=index_name):
				response = self.es_client.search(
					index=index_name,
					body=body
				)

				for data in response['aggregations']['news_id']['buckets']:
					value = {
						"in": data['comment']['in']['value'],
						"out": data['uncomment']['out']['value'],
						"comment": int(data['comment']['in']['value']) - int(data['uncomment']['out']['value'])
					}
					my_dict[str(data['key']).upper()] = value
			else:
				self.logger.warning(f"index={index_name} doesn't exist")
		except Exception:
			raise
		return my_dict

	def getCommentById(self, the_date: datetime = None, news_id: str = ''):
		if the_date is None:
			the_date = datetime.today()
		index_name = 'jtbcnews-raw-{}'.format(the_date.strftime('%Y.%m.%d'))

		body = {
			"query": {
				"bool": {
					"must": [
						{
							"terms": {
								"action_name": [
									"comment",
									"uncomment"
								]
							}
						},
						{
							"match": {
								"news_id": news_id
							}
						},
						{
							"range": {
								"reqtime": {
									"lt": the_date.strftime('%Y-%m-%dT%H:%M:%S+09:00')
								}
							}
						}
					]
				}
			},
			"_source": False,
			"size": 0,
			"aggs": {
				"news_id": {
					"terms": {
						"field": "news_id",
						"size": 999999999
					},
					"aggs": {
						"comment": {
							"filter": {"term": {"action_name": "comment"}},
							"aggs": {
								"in": {
									"value_count": {
										"field": "action_name"
									}
								}
							}
						},
						"uncomment": {
							"filter": {"term": {"action_name": "uncomment"}},
							"aggs": {
								"out": {
									"value_count": {
										"field": "action_name"
									}
								}
							}
						}
					}
				}
			}
		}
		value = {}
		try:
			if self.es_client.indices.exists(index=index_name):
				response = self.es_client.search(
					index=index_name,
					body=body
				)

				data = response['aggregations']['news_id']['buckets'][0]
				value = {
					"news_id": data['key'],
					"in": data['comment']['in']['value'],
					"out": data['uncomment']['out']['value'],
					"comment": int(data['comment']['in']['value']) - int(data['uncomment']['out']['value'])
				}
			else:
				self.logger.warning(f"index={index_name} doesn't exist")
		except Exception:
			raise
		return value

	def getReactionById(self, the_date: datetime = None, news_id: str = ''):
		if the_date is None:
			the_date = datetime.today()
		index_name = 'jtbcnews-raw-{}'.format(the_date.strftime('%Y.%m.%d'))

		body = {
			"query": {
				"bool": {
					"must": [
						{
							"terms": {
								"action_name": [
									"reaction",
									"unreaction"
								]
							}
						},
						{
							"match": {
								"news_id": news_id
							}
						},
						{
							"range": {
								"reqtime": {
									"lt": the_date.strftime('%Y-%m-%dT%H:%M:%S+09:00')
								}
							}
						}
					]
				}
			},
			"_source": False,
			"size": 0,
			"aggs": {
				"news_id": {
					"terms": {
						"field": "news_id",
						"size": 999999999
					},
					"aggs": {
						"reaction": {
							"filter": {"term": {"action_name": "reaction"}},
							"aggs": {
								"in": {
									"value_count": {
										"field": "action_name"
									}
								}
							}
						},
						"unreaction": {
							"filter": {"term": {"action_name": "unreaction"}},
							"aggs": {
								"out": {
									"value_count": {
										"field": "action_name"
									}
								}
							}
						}
					}
				}
			}
		}
		value = {}
		try:
			if self.es_client.indices.exists(index=index_name):
				response = self.es_client.search(
					index=index_name,
					body=body
				)

				data = response['aggregations']['news_id']['buckets'][0]
				value = {
					"news_id": data['key'],
					"in": data['reaction']['in']['value'],
					"out": data['unreaction']['out']['value'],
					"comment": int(data['reaction']['in']['value']) - int(data['unreaction']['out']['value'])
				}
			else:
				self.logger.warning(f"index={index_name} doesn't exist")
		except Exception:
			raise
		return value

	def aggrNewsDataById(self, the_date: datetime = None):
		# news_id 기준 데이터 리턴
		pass


	def getDataListHourly(self, the_date: datetime):
		# 최종 데이터 리턴
		hourly_set = dict()
		date_range = {
			"gte": the_date.strftime('%Y-%m-%dT%H:00:00+09:00'),
			"lte": the_date.strftime('%Y-%m-%dT%H:59:59+09:00')
		}
		self.logger.info(f"date_range: {the_date.strftime('%Y-%m-%dT%H:00:00+09:00')} ~ "
						 f"{the_date.strftime('%Y-%m-%dT%H:59:59+09:00')}")
		try:
			index_name = 'jtbcnews-raw-{}'.format(the_date.strftime('%Y.%m.%d'))

			'''
				,
				{
					"match": {
						"news_id": "NB12020981"
					}
				}
			'''
			# news_id, news_nm 없는 경우 중지/삭제된 기사로 제외
			body = {
				"size": 0,
				"query": {
					"bool": {
						"must": [
							{
								"term": {
									"action_name": {
										"value": "viewarticle"
									}
								}
							},
							{
								"range": {
									"reqtime": date_range
								}
							}
						],
						"must_not": [
							{
								"match": {
									"news_id": ""
								}
							},
							{
								"match": {
									"news_nm": ""
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
							"news_name": {
								"top_hits": {
									"size": 1,
									"_source": {
										"includes": "news_nm"
									}
								}
							},
							"by_origin": {
								"terms": {
									"field": "parm_origin",
									"size": 999999999
								}
							}
						}
					}
				}
			}

			if self.es_client.indices.exists(index=index_name):
				response = self.es_client.search(
					index=index_name,
					body=body
				)

				comment_set = self.getCommentHourlyMap(the_date=the_date)
				reaction_set = self.getReactionHourlyMap(the_date=the_date)

				for data in response['aggregations']['by_news_id']['buckets']:
					news_id = str(data['key']).upper()
					tpl = {
						'news_name': data['news_name']['hits']['hits'][0]['_source']['news_nm'],
						'mobile_view': 0,
						'pc_view': 0,
						'app_view': 0
					}

					for origin in data['by_origin']['buckets']:
						if 'mobile' in str(origin['key']).lower():
							tpl.update({
								'mobile_view': origin['doc_count']
							})
						elif 'pc' in str(origin['key']).lower():
							tpl.update({
								'pc_view': origin['doc_count']
							})
						elif 'app' in str(origin['key']).lower():
							tpl.update({
								'app_view': origin['doc_count']
							})

					if news_id in reaction_set.keys():
						value = reaction_set[news_id]
						tpl.update({
							"reaction": value['reaction']
						})
					else:
						tpl.update({
							"reaction": 0
						})

					if news_id in comment_set.keys():
						value = comment_set[news_id]
						tpl.update({
							"comment": value['comment']
						})
					else:
						tpl.update({
							"comment": 0
						})

					hourly_set[news_id] = tpl
			else:
				self.logger.warning(f"index={index_name} doesn't exist.")

		except Exception:
			# exc_type, exc_value, exc_traceback = sys.exc_info()
			# msg = repr(traceback.format_exception(exc_type, exc_value, exc_traceback))
			# logging.exception(msg)
			# self.logger.error(f"ERROR: {traceback.format_exc()}")
			# return False, hourly_set
			raise
		return True, hourly_set

	def collect_prog_id_for_news_id_in_hourly_summary_index(self, the_date: datetime):
		"""
		하루 동안 hourly_news_summary 에서 prog_id가 있는 news_id 데이터만 추출
		:param the_date:
		:return:
		"""
		mapping_news_ids = dict()
		try:
			date_range = {
				"gte": the_date.strftime('%Y-%m-%dT00:00:00+09:00'),
				"lte": the_date.strftime('%Y-%m-%dT23:59:59+09:00')
			}

			body = {
				"size": 0,
				"query": {
					"bool": {
						"must": [
							{
								"range": {
									"reg_date": date_range
								}
							},
							{
								"exists": {
									"field": "prog_id"
								}
							}
						]
					}
				},
				"aggs": {
					"news_id": {
						"terms": {
							"field": "news_id",
							"size": 999999999
						},
						"aggs": {
							"news_info": {
								"top_hits": {
									"size": 1,
									"_source": {
										"includes": ["prog_id", "prog_name", "contents_div"]
									},
									"sort": [
										{
											"reg_date": {
												"order": "desc"
											}
										}
									]
								}
							}
						}
					}
				}
			}

			if self.es_client.indices.exists(index=constVar.HOURLY_NEWS_SUMMARY):
				response = self.es_client.search(
					index=constVar.HOURLY_NEWS_SUMMARY,
					# for test
					# index='origin-hourly-news-summary.2021.12.02',
					body=body
				)

				for data in response['aggregations']['news_id']['buckets']:
					news_id = data.get('key')
					# mapping_news_ids[]
					tmp_prog_id_info = (((data.get('news_info') or {}).get('hits') or {}).get('hits') or [])
					prog_id_info_set = tmp_prog_id_info[0].get('_source') if tmp_prog_id_info else {}
					mapping_news_ids[news_id] = prog_id_info_set
			else:
				self.logger.warning(f"index={constVar.HOURLY_NEWS_SUMMARY} doesn't exist")

		except Exception:
			raise

		return mapping_news_ids

	def getDataListDaily(self, the_date: datetime):
		# alias기반 hourly-news-summary에서 news_id를 기반으로 view, comment, reaction 등의 수치 지표 계산
		daily_set = dict()
		date_range = {
							"gte": the_date.strftime('%Y-%m-%dT00:00:00+09:00'),
							"lte": the_date.strftime('%Y-%m-%dT23:59:59+09:00')
						}
		self.logger.info(f"date_range: {the_date.strftime('%Y-%m-%dT00:00:00+09:00')} ~ "
						 f"{the_date.strftime('%Y-%m-%dT23:59:59+09:00')}")
		try:
			body = {
				"size": 0,
				"query": {
					"bool": {
						"must": [
							{
								"range": {
									"reg_date": date_range
								}
							}
						]
					}
				},
				"aggs": {
					"news_id": {
						"terms": {
							"field": "news_id",
							"size": 999999999
						},
						"aggs": {
							"news_info": {
								"top_hits": {
									"size": 1,
									"_source": {
										"includes": ["news_name", "source", "section", "service_date", "reporter"]
										# "prog_id", "prog_name", "contents_div"]
									},
									"sort": [
										{
											"reg_date": {
												"order": "desc"
											}
										}
									]
								}
							},
							"jtbc.pc_view": {
								"sum": {
									"field": "jtbc.pc_view"
								}
							},
							"jtbc.mobile_view": {
								"sum": {
									"field": "jtbc.mobile_view"
								}
							},
							"jtbc.app_view": {
								"sum": {
									"field": "jtbc.app_view"
								}
							},
							"jtbc.view": {
								"sum": {
									"field": "jtbc.view"
								}
							},
							"jtbc.comment": {
								"sum": {
									"field": "jtbc.comment"
								}
							},
							"jtbc.reaction": {
								"sum": {
									"field": "jtbc.reaction"
								}
							}
						}
					}
				}
			}

			if self.es_client.indices.exists(constVar.HOURLY_NEWS_SUMMARY):
				response = self.es_client.search(
					index=constVar.HOURLY_NEWS_SUMMARY,
					# for test
					# index='origin-hourly-news-summary.2021.12.02',
					body=body
				)

				for data in response['aggregations']['news_id']['buckets']:
					news_id = str(data['key']).upper()

					tpl = {
						'news_name': data['news_info']['hits']['hits'][0]['_source']['news_name'],
						'mobile_view': int(data['jtbc.mobile_view']['value']),
						'pc_view': int(data['jtbc.pc_view']['value']),
						'app_view': int(data['jtbc.app_view']['value']),
						'view': int(data['jtbc.view']['value']),
						'reaction': int(data['jtbc.reaction']['value']),
						'comment': int(data['jtbc.comment']['value'])

					}

					if 'section' in data['news_info']['hits']['hits'][0]['_source']:
						tpl.update({
							'section': data['news_info']['hits']['hits'][0]['_source']['section']
						})

					if 'source' in data['news_info']['hits']['hits'][0]['_source']:
						tpl.update({
							'source': data['news_info']['hits']['hits'][0]['_source']['source']
						})

					if 'reporter' in data['news_info']['hits']['hits'][0]['_source']:
						tpl.update({
							'reporter': data['news_info']['hits']['hits'][0]['_source']['reporter']
						})

					if 'service_date' in data['news_info']['hits']['hits'][0]['_source']:
						tpl.update({
							'service_date': data['news_info']['hits']['hits'][0]['_source']['service_date']
						})

					daily_set[news_id] = tpl
			else:
				self.logger.warning(f"index={constVar.HOURLY_NEWS_SUMMARY} doesn't exist")

		except Exception:
			# exc_type, exc_value, exc_traceback = sys.exc_info()
			# msg = repr(traceback.format_exception(exc_type, exc_value, exc_traceback))
			# logging.exception(msg)
			# self.logger.error(f"ERROR: {traceback.format_exc()}")
			# return False, daily_set
			raise
		return True, daily_set

	def collect_user_and_member_info_by_parmorigin_on_daterange(self, the_date: datetime = None,
																period: str = 'D') -> dict:
		"""
		일 또는 월(period: D or M)에 따라 유니크한 접속자(user_id)수와 로그인(member_id)의 수를 계산함
		:param the_date:
		:return:
		"""
		if period == 'D':
			date_range = {
				"gte": the_date.strftime('%Y-%m-%dT00:00:00+09:00'),
				"lte": the_date.strftime('%Y-%m-%dT23:59:59+09:00')
			}
			self.logger.info(f"get user summary info by daily from "
							 f"{date_range.get('gte')} "
							 f"to {date_range.get('lte')}")
			self.logger.info("get the day information for the user and member by param_origin")

		else:
			# calendar.monthrange : 해당달의 1일 포지션(0부터 시작)과 마지막일을 리턴함
			last_day = the_date.replace(day=calendar.monthrange(the_date.year, the_date.month)[1])
			date_range = {
				"gte": the_date.strftime('%Y-%m-01T00:00:00+09:00'),
				"lte": last_day.strftime('%Y-%m-%dT23:59:59+09:00')
			}
			self.logger.info(f"get user summary info by montly from "
							 f"{date_range.get('gte')} "
							 f"to {date_range.get('lte')}")
			self.logger.info("get the month information for the user and member by param_origin")

		elk_data = {}
		index_name = 'jtbcnews-raw-{}'.format(the_date.strftime('%Y.%m.%d'))
		try:
			body = {
				"size": 0,
				"query": {
					"range": {
						"reqtime": date_range
					}
				},
				"aggs": {
					"unique_user_id_by_parm_origin": {
						"terms": {
							"field": "parm_origin",
							"order": {
								"_key": "asc"
							}
						},
						"aggs": {
							"by_parm_user_id_count": {
								"cardinality": {
									"field": "parm_user_id"
								}
							}
						}
					},
					"get_unique_member_id_by_member_type": {
						"terms": {
							"field": "parm_member_type",
							"order": {
								"_key": "asc"
							}
						},
						"aggs": {
							"by_parm_origin": {
								"terms": {
									"field": "parm_origin",
									"order": {
										"_key": "asc"
									}
								},
								"aggs": {
									"member_count": {
										"cardinality": {
											"field": "parm_member_id"
										}
									}
								}
							}
						}
					}
				}
			}

			if self.es_client.indices.exists(index_name):

				response = self.es_client.search(
					index=index_name,
					body=body
				)

				# unique member
				member_total, member_info, member_type_info, member_parm_origin_info = \
					_get_unique_member_info(
						elk_response=response['aggregations']['get_unique_member_id_by_member_type']['buckets'])
				# elk_data["member"] = member_info
				# elk_data["member_total"] = member_total
				# elk_data["member_parm_origin"] = member_parm_origin_info
				# elk_data['member_type'] = member_type_info
				elk_data["member"] = {
					"total": member_total,
					"info": member_info,
					"by_parm_origin": member_parm_origin_info,
					"by_member_type": member_type_info
				}

				# unique user {}
				user_total = 0
				user_info = {}
				for data in response['aggregations']['unique_user_id_by_parm_origin']['buckets']:
					param_origin_key = data.get('key') or None
					value = (data.get('by_parm_user_id_count') or {}).get('value') or None
					# user_data[key] = value
					user_info[param_origin_key] = value
					user_total += value
				elk_data[f"user"] = {"total": user_total,
									 "info": user_info}

			else:
				self.logger.warning(f"index={index_name} doesn't exist")

		except (ValueError, Exception) as e:
			raise

		return elk_data

	def collect_news_view_by_parmorigin_on_daterange(self, the_date: datetime = None, period: str = 'D') -> dict:
		"""
		일 또는 월(period: D or M)에 따라 parm_origin(app, mobile, pc)기반 뉴스 조회수
		:param the_date:
		:return:
		"""
		if period == 'D':
			date_range = {
				"gte": the_date.strftime('%Y-%m-%dT00:00:00+09:00'),
				"lte": the_date.strftime('%Y-%m-%dT23:59:59+09:00')
			}
			self.logger.info(f"get news summary info by daily from "
							 f"{date_range.get('gte')} to {date_range.get('lte')}")
			self.logger.info("get the day information for the news view by param_origin")

		else:
			# calendar.monthrange : 해당달의 1일 포지션(0부터 시작)과 마지막일을 리턴함
			last_day = the_date.replace(day=calendar.monthrange(the_date.year, the_date.month)[1])
			date_range = {
				"gte": the_date.strftime('%Y-%m-01T00:00:00+09:00'),
				"lte": last_day.strftime('%Y-%m-%dT23:59:59+09:00')
			}
			self.logger.info(f"get news summary info by montly from "
							 f"{date_range.get('gte')} to {date_range.get('lte')}")
			self.logger.info("get the month information for the news view by param_origin")

		elk_data = {}
		index_name = 'jtbcnews-raw-{}'.format(the_date.strftime('%Y.%m.%d'))
		try:
			body = {
				"size": 0,
				"query": {
					"bool": {
						"must": [
							{
								"term": {
									"action_name": {
										"value": "viewarticle"
									}
								}
							},
							{
								"range": {
									"reqtime": date_range
								}
							}
						],
						"must_not": [
							{
								"match": {
									"news_id": ""
								}
							},
							{
								"match": {
									"news_nm": ""
								}
							}
						]
					}
				},
				"aggs": {
					"news_view_by_param_origin": {
						"terms": {
							"field": "parm_origin",
							"order": {
								"_key": "asc"
							}
						},
						"aggs": {
							"by_parm_origin": {
								"cardinality": {
									"field": "action_name"
								}
							}
						}
					},
					"get_unique_member_id_by_member_type": {
						"terms": {
							"field": "parm_member_type",
							"order": {
								"_key": "asc"
							}
						},
						"aggs": {
							"by_parm_origin": {
								"terms": {
									"field": "parm_origin",
									"order": {
										"_key": "asc"
									}
								},
								"aggs": {
									"member_count": {
										"cardinality": {
											"field": "parm_member_id"
										}
									}
								}
							}
						}
					}
				}
			}

			if self.es_client.indices.exists(index_name):

				response = self.es_client.search(
					index=index_name,
					body=body
				)

				# news view by parm_origin
				view_total = 0
				view_info = {}
				for data in response['aggregations']['news_view_by_param_origin']['buckets']:
					key = data.get('key') or None
					value = data.get('doc_count') or None
					view_info[f"{key}_view"] = value
					view_total += value
				elk_data[f"jtbc"] = {"view": view_total,
									 "by_parm_origin": view_info}
				# elk_data["jtbc"] = view_info

				# member_id 기준으로 자세히 파보자.
				member_total, member_info, member_type_info, member_parm_origin_info = \
					_get_view_member_info(
						elk_response=response['aggregations']['get_unique_member_id_by_member_type']['buckets'])
				# elk_data["jtbc.member"] = member_info
				# elk_data["jtbc.member_total"] = member_total
				# elk_data["jtbc.member_parm_origin"] = member_parm_origin_info
				# elk_data['jtbc.member_type'] = member_type_info

				elk_data["jtbc.member"] = {
					"total": member_total,
					"info": member_info,
					"by_parm_origin": member_parm_origin_info,
					"by_member_type": member_type_info
				}
			else:
				self.logger.warning(f"index={index_name} doesn't exist")

		except (ValueError, Exception) as e:
			raise

		return elk_data

	def collect_election_news_view_by_parmorigin(self, the_date: datetime = None,
												 parm_referer_keys: list = ['*election*', '*melection*']) -> dict:
		"""
		당일 기준 뉴스 조회수를 parm_origin(app, mobile, pc)+ua_brawser_family기반으로 집계
		:param the_date:
		:return:
		"""

		date_range = {
			"gte": the_date.strftime('%Y-%m-%dT00:00:00+09:00'),
			"lte": the_date.strftime('%Y-%m-%dT23:59:59+09:00')
		}
		self.logger.info(f"get news summary info by daily from "
						 f"{date_range.get('gte')} to {date_range.get('lte')}")
		self.logger.info("get the day information for the news view by param_origin")

		# "*election* and *melection*"
		filter_query = " or ".join(parm_referer_keys)

		elk_data = {}
		index_name = 'jtbcnews-raw-{}'.format(the_date.strftime('%Y.%m.%d'))
		try:
			body = {
				"size": 0,
				"query": {
					"bool": {
						"must": [
							{
								"term": {
									"action_name": {
										"value": "viewarticle"
									}
								}
							},
							{
								"range": {
									"reqtime": date_range
								}
							},
							{
								"query_string": {
									"default_field": "parm_referer",
									"query": filter_query
								}
							}
						],
						"must_not": [
							{
								"match": {
									"news_id": ""
								}
							},
							{
								"match": {
									"news_nm": ""
								}
							},
							{
								"match": {
									"parm_referer": ""
								}
							}
						]
					}
				},
				"aggs": {
					"filtered_news_view": {
						"terms": {
							"field": "parm_origin",
							"order": {
								"_key": "asc"
							}
						},
						"aggs": {
							"by_ua_brawser_family": {
								"terms": {
									"field": "ua_browser_family",
									"order": {
										"_key": "asc"
									}
								},
								"aggs": {
									"by_parm_origin": {
										"cardinality": {
											"field": "action_name"
										}
									}
								}
							}
						}
					}
				}
			}

			if self.es_client.indices.exists(index=index_name):
				response = self.es_client.search(
					index=index_name,
					body=body
				)

				# news view by parm_origin
				view_total = 0
				view_info = {}
				total_brawser_info = {}
				for data in response['aggregations']['filtered_news_view']['buckets']:
					key = data.get('key') or None
					value = data.get('doc_count') or None
					view_info[f"{key}_view"] = value
					view_total += value

					brawser_info_by_parm_origin = {}
					for brawser_data in (data.get('by_ua_brawser_family') or {}).get('buckets') or []:
						a_brawser = brawser_data.get('key')
						a_brawser_value = brawser_data.get('doc_count')
						brawser_info_by_parm_origin[a_brawser] = a_brawser_value
					total_brawser_info[key] = brawser_info_by_parm_origin

				elk_data["jtbc_election"] = {"view": view_total,
											 "by_parm_origin": view_info,
											 "by_ua_brawser_family": total_brawser_info}
			else:
				self.logger.warning(f"index={index_name} doesn't exist")


		except (ValueError, Exception) as e:
			raise

		return elk_data

	def collect_election_news_by_news_id_on_day(self, the_date: datetime, parm_referer_keys: list = ['*election*',
																									 '*melection*']) -> list:
		"""
		당일 election관련 뉴스만 news_id 기준으로 parm_origin+ua_braoser_family 기반으로 뉴스 조회수 집계
		:param the_date:
		:param parm_referer_keys:
		:return:
		"""

		daily_li = []
		# "*election* and *melection*"
		filter_query = " or ".join(parm_referer_keys)
		index_name = 'jtbcnews-raw-{}'.format(the_date.strftime('%Y.%m.%d'))
		try:

			# news_id, news_nm 없는 경우 중지/삭제된 기사로 제외
			body = {
				"size": 0,
				"query": {
					"bool": {
						"must": [
							{
								"term": {
									"action_name": {
										"value": "viewarticle"
									}
								}
							},
							{
								"range": {
									"reqtime": {
										"gte": the_date.strftime('%Y-%m-%dT00:00:00+09:00'),
										"lte": the_date.strftime('%Y-%m-%dT23:59:59+09:00')
									}
								}
							},
							{
								"query_string": {
									"default_field": "parm_referer",
									"query": filter_query
								}
							}
						],
						"must_not": [
							{
								"match": {
									"news_id": ""
								}
							},
							{
								"match": {
									"news_nm": ""
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
							"news_name": {
								"top_hits": {
									"size": 1,
									"_source": {
										"includes": "news_nm"
									}
								}
							},
							"by_origin": {
								"terms": {
									"field": "parm_origin",
									"size": 999999999
								},
								"aggs": {
									"by_ua_brawser_family": {
										"terms": {
											"field": "ua_browser_family",
											"size": 999999999
										}
									}
								}
							}
						}
					}
				}
			}
			if self.es_client.indices.exists(index_name):
				response = self.es_client.search(
					index=index_name,
					body=body
				)

				for data in response['aggregations']['by_news_id']['buckets']:
					news_id = str(data['key']).upper()

					parm_origin = {}
					for a_parm_origin in (data.get('by_origin') or {}).get('buckets') or []:
						a_brawser = a_parm_origin.get('key')
						a_brawser_value = a_parm_origin.get('doc_count')
						parm_origin[a_brawser] = a_brawser_value
						brawser_family = {}
						for a_brawser in a_parm_origin.get('by_ua_brawser_family').get('buckets') or []:
							brawser_family[a_brawser.get('key')] = a_brawser.get('doc_count')

					daily_li.append(
						{
							'news_id': news_id,
							'news_nm': data['news_name']['hits']['hits'][0]['_source']['news_nm'],
							'view': data['doc_count'],
							'by_parm_origin': parm_origin,
							'by_ua_brawser_family': brawser_family
						}
					)
			else:
				self.logger.warning(f"index={index_name} doesn't exist")
		except Exception:
			raise

		return daily_li

	def collect_jtbc_tv_prog_id_by_news_ids(self, news_ids: list, the_date: datetime) -> list:
		"""
		다중 news_id에 대해서 일치하는 prog_id를 찾음 (현재 news_id와 prog_id는 1:1 매칭)
		index: origin-mapping-tv-news-program-YYYY
		:param news_ids: list
		:param the_date:
		:return:
			[{'news_id': 'NB12035334', 'prog_id': 'PR10000403'},
 			{'news_id': 'NB12035335', 'prog_id': 'PR10000403'},...]
		"""
		prog_ids = []
		index_name = f"origin-{constVar.JTBC_MAPPING_TV_NEW_PROGRAM}-{the_date.strftime('%Y')}"

		try:
			if news_ids:
				self.logger.info(f"news_ids for searching program_id size is {len(news_ids)}")
				# elastic에서 create query: maxClauseCount is set to 1024 관련 에러가 나서 1000개씩 짤라서 query 수행으로 변경
				for a_split_news_ids in np.array_split(news_ids, np.ceil(len(news_ids) / self.helper.query_split_size)):
					a_split_news_ids = list(a_split_news_ids)
					self.logger.info(f"search news_ids in elastic index {index_name}")

					should_match_query = []
					for a_id in a_split_news_ids:
						should_match_query.append({
							"match": {
								"news_id": a_id
							}
						})


					body = {
						"size": 10,
						"query": {
							"bool": {
								"should": should_match_query
							}
						},
						"aggs": {
							"by_news_id": {
								"terms": {
									"field": "news_id",
									"order": {
										"_key": "asc"
									},
									"size": 999999999
								},
								"aggs": {
									"by_prog_id": {
										"terms": {
											"field": "prog_id",
											"order": {
												"_key": "asc"
											},
											"size": 999999999
										}
										# ,
										# "aggs": {
										# 	"by_count": {
										# 		"cardinality": {
										# 			"field": "prog_id"
										# 		}
										# 	}
										# }
									}
								}
							}
						}
					}

					if self.es_client.indices.exists(index_name):
						response = self.es_client.search(
							index=index_name,
							body=body
						)

						prog_ids_by_news_id_li = (
							((response.get('aggregations') or {}).get('by_news_id') or {}).get('buckets') or [])
						if prog_ids_by_news_id_li:
							for a_news_id in a_split_news_ids:
								prod_id_li = []
								for a_prog_info in [(a_li.get('by_prog_id') or {}) for a_li in prog_ids_by_news_id_li if
													a_li.get('key') == a_news_id]:
									prod_id_li.extend(a_prog_info.get('buckets'))

								if prod_id_li:
									prog_ids.append({
										"news_id": a_news_id,
										# "prog_id": [ a_prod_id.get('key') for a_prod_id in prod_id_li]
										"prog_id": prod_id_li[0].get('key')
									})
					else:
						self.logger.warning(f"index={index_name} doesn't exist")
			else:
				self.logger.info(f"news_ids for searching program_id size is 0")
		except Exception:
			raise

		return prog_ids

	def collect_jtbc_tv_prog_id_by_news_id(self, news_id: str, the_date: datetime) -> list:
		"""
		news_id에 대해서 매칭되는 prog_id를 찾음 (1:1 매칭, 결과가 없을 경우 []을 리턴함

		:param news_id:
		:param the_date:
		:return:
		"""

		prog_ids = []
		index_name = f"origin-{constVar.JTBC_MAPPING_TV_NEW_PROGRAM}-{the_date.strftime('%Y')}"

		try:
			body = {
				"size": 0,
				"query": {
					"bool": {
						"must": [
							{
								"match": {
									"news_id": news_id
								}
							}
						]
					}
				},
				"aggs": {
					"news_id": {
						"terms": {
							"field": "prog_id",
							"size": 999999999
						}
					}
				}
			}

			if self.es_client.indices.exists(index_name):
				response = self.es_client.search(
					index=index_name,
					body=body
				)

				li = (((response.get('aggregations') or {}).get('news_id') or {}).get('buckets') or [])
				if li:
					prog_ids = [a_data.get('key') for a_data in li]
			else:
				self.logger.warning(f"index={index_name} doesn't exist")

		except Exception:
			raise

		return prog_ids
