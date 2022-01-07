from _datetime import datetime
import sys
import traceback
from main.aggregator.JTBCData import JTBCData
from main.aggregator.KakaoData import KakaoData
from main.aggregator.NaverData import NaverData
from main.module.DetailNewsInfo import DetailNewsInfo
from elasticsearch.helpers import bulk
from base import constVar
import os
from util.JtbcLogger import JtbcLogger
from pathlib import Path


# noinspection DuplicatedCode
class PlatformSummary:
	def __init__(self, helper):
		self.helper = helper
		self.es_client = helper.get_es()
		self.jtbcData = JTBCData(helper)
		self.naverData = NaverData(helper)
		self.kakaoData = KakaoData(helper)
		self.detailNewsInfo = DetailNewsInfo(helper)

		if os.path.isdir('/box/jnd-batch'):
			# prod
			path = '/box/jnd-batch'
		else:
			# dev
			path = Path(__file__).parent.parent.parent

		logger_factory = JtbcLogger(path=os.path.join(path, "logs"), file_name=type(self).__name__)
		self.logger = logger_factory.logger

	def aggrDailyPlatformSummary(self, the_date: datetime = None):
		daily_set = list()
		jtbc_view = naver_view = kakao_view = 0
		jtbc_comment = naver_comment = kakao_comment = 0
		jtbc_reaction = naver_reaction = kakao_reaction = 0
		view_total = comment_total = comment_total = 0

		if the_date is None:
			raise ValueError(f'the_data is None')
		try:
			self.logger.info(f"the_date = {the_date}")

			jtbc_ret, jtbc_map = self.jtbcData.getDataListDaily(the_date=the_date)
			naver_ret, naver_map = self.naverData.getDataListDaily(the_date=the_date)
			kakao_ret, kakao_map = self.kakaoData.getDataListDaily(the_date=the_date)

			jtbc_view = sum(jtbc_map[item]['view'] for item in jtbc_map)
			jtbc_comment = sum(jtbc_map[item]['comment'] for item in jtbc_map)
			jtbc_reaction = sum(jtbc_map[item]['reaction'] for item in jtbc_map)

			naver_view = sum(naver_map[item]['view'] for item in naver_map)
			naver_comment = sum(naver_map[item]['comment'] for item in naver_map)
			naver_reaction = sum(naver_map[item]['reaction'] for item in naver_map)

			kakao_view = sum(kakao_map[item]['view'] for item in kakao_map)
			kakao_comment = sum(kakao_map[item]['comment'] for item in kakao_map)
			kakao_reaction = sum(kakao_map[item]['reaction'] for item in kakao_map)

			if jtbc_map:
				index_id = "{0}_{1}".format(the_date.strftime('%Y%m%d'), 'jtbc')
				data = {
					"_op_type": "update",
					"_id": index_id,
					"doc_as_upsert": True,
					"doc": {
						"reg_date": the_date.strftime('%Y-%m-%dT00:00:00+09:00'),
						"platform": 'jtbc',
						"view": jtbc_view,
						"reaction": jtbc_reaction,
						"comment": jtbc_comment
					}
				}
				daily_set.append(data)

			if naver_map:
				index_id = "{0}_{1}".format(the_date.strftime('%Y%m%d'), 'naver')
				data = {
					"_op_type": "update",
					"_id": index_id,
					"doc_as_upsert": True,
					"doc": {
						"reg_date": the_date.strftime('%Y-%m-%dT00:00:00+09:00'),
						"platform": 'naver',
						"view": naver_view,
						"reaction": naver_reaction,
						"comment": naver_comment
					}
				}
				daily_set.append(data)

			if kakao_map:
				index_id = "{0}_{1}".format(the_date.strftime('%Y%m%d'), 'kakao')
				data = {
					"_op_type": "update",
					"_id": index_id,
					"doc_as_upsert": True,
					"doc": {
						"reg_date": the_date.strftime('%Y-%m-%dT00:00:00+09:00'),
						"platform": 'kakao',
						"view": kakao_view,
						"reaction": kakao_reaction,
						"comment": kakao_comment
					}
				}
				daily_set.append(data)

			# 년 단위로 인덱스 저장
			index_name = '{0}-{1}'.format(constVar.DAILY_PLATFORM_SUMMARY, the_date.strftime('%Y'))
			bulk(self.es_client, daily_set, index=index_name)
			return True, daily_set
		except Exception:
			# exc_type, exc_value, exc_traceback = sys.exc_info()
			# msg = repr(traceback.format_exception(exc_type, exc_value, exc_traceback))
			# logging.exception(msg)
			# self.logger.error(f"ERROR: {traceback.format_exc()}")
			# return False, daily_set
			raise
