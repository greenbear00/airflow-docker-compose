from _datetime import datetime
import sys
import traceback
from main.aggregator.JTBCData import JTBCData
from main.aggregator.KakaoData import KakaoData
from main.aggregator.NaverData import NaverData
from main.module.DetailNewsInfo import DetailNewsInfo
from main.module.TVProInfo import TVProInfo
from elasticsearch.helpers import bulk
from base import constVar
import os
from pathlib import Path
from util.JtbcLogger import JtbcLogger


# noinspection DuplicatedCode
class NewsSummary:
	def __init__(self, helper):
		self.helper = helper
		self.es_client = helper.get_es()
		self.jtbcData = JTBCData(helper)
		self.naverData = NaverData(helper)
		self.kakaoData = KakaoData(helper)
		self.tvpro = TVProInfo(helper=helper, level=helper.build_level)
		self.detailNewsInfo = DetailNewsInfo(helper)


		if os.path.isdir('/box/jnd-batch'):
			# prod
			path = '/box/jnd-batch'
		else:
			# dev
			path = Path(__file__).parent.parent.parent

		logger_factory = JtbcLogger(path=os.path.join(path, "logs"), file_name=type(self).__name__)
		self.logger = logger_factory.logger



	def aggrHourlyNewsSummary(self, the_date: datetime = None):
		hourly_set = list()
		new_mapping_jtbc_map = dict()
		try:
			if the_date is None:
				raise Exception('날짜 지정 필수!')

			jtbc_ret, jtbc_map = self.jtbcData.getDataListHourly(the_date=the_date)
			naver_ret, naver_map = self.naverData.getDataListHourly(the_date=the_date)
			kakao_ret, kakao_map = self.kakaoData.getDataListHourly(the_date=the_date)
			news_info_ret, news_info_map = self.detailNewsInfo.getNewsDataById(id_list=jtbc_map.keys())
			# 이미 jtbc_map에는 aggregation되어서 news_id가 unique하게 수집됨
			mapping_prog_ids = self.jtbcData.collect_jtbc_tv_prog_id_by_news_ids(news_ids=list(jtbc_map.keys()),
																	   the_date=the_date)
			unique_prog_ids = list(set([a_data.get('prog_id') for a_data in mapping_prog_ids]))
			self.logger.info(f"news_id's unique_prog_ids: {unique_prog_ids}")
			if unique_prog_ids:
				unique_prog_ids_info = self.tvpro.get_nw_tv_program_by_prog_ids(prog_ids=unique_prog_ids)
				new_mapping_jtbc_map = self.tvpro.update_prog_id_for_news_ids(mapping_news_ids=mapping_prog_ids,
																	  		  prod_ids_info=unique_prog_ids_info)

			self.logger.info(f"mapping news_ids for prog_id: {new_mapping_jtbc_map.keys()}")
			now_time = datetime.now().strftime('%Y-%m-%dT%H:%M:%S+09:00')
			for key, value in jtbc_map.items():
				# base > jtbc 데이터 기준
				jtbc_subset = {}
				naver_subset = {}
				kakao_subset = {}

				# 주의) the_date가 2021-10-29 00:59:59분에 news_id ['NB12028619']는 뉴스 방송 프로그램에 매핑된 정보 없음
				if new_mapping_jtbc_map.get(key):
					tpl = {
						"reg_date": the_date.strftime('%Y-%m-%dT%H:00:00+09:00'),
						# "news_id": key,
						**new_mapping_jtbc_map.get(key),
						"news_name": value['news_name'],
						"update_date": now_time
					}
				else:
					tpl = {
						"reg_date": the_date.strftime('%Y-%m-%dT%H:00:00+09:00'),
						"news_id": key,
						"news_name": value['news_name'],
						"update_date": now_time
					}

				if key in news_info_map.keys():
					niv = news_info_map[key]
					source = self.detailNewsInfo.parseSourceCode(niv['SOURCE_CODE'], niv['SOURCE_NAME'])
					section = self.detailNewsInfo.parseSection(niv['NEWS_SECTION'], niv['SECTION_NAME'])
					reporter = self.detailNewsInfo.parseReporter(niv['REPORTER_INFO'])
					service_date = datetime.strptime(niv['SERVICE_DT'], '%Y%m%d%H%M')
					tpl.update({
						"source": source,
						"news_name": niv['TITLE'],
						"section": section,
						"reporter": reporter,
						"service_date": "{}".format(service_date.strftime('%Y-%m-%dT%H:%M:%S+09:00'))
					})

				jtbc_view_total = int(value['pc_view']) + int(value['mobile_view']) + int(value['app_view'])
				jtbc_subset.update({
					"pc_view": value['pc_view'],
					"mobile_view": value['mobile_view'],
					"app_view": value['app_view'],
					"view": jtbc_view_total,
					"comment": value['comment'],
					"reaction": value['reaction'],
				})

				view_total = jtbc_view_total
				reaction_total = int(value['reaction'])
				comment_total = int(value['comment'])

				if key in naver_map.keys():
					naver_value = naver_map[key]
					naver_subset.update({
						"pc_view": naver_value['pc_view'],
						"mobile_view": naver_value['mobile_view'],
						"view": naver_value['view'],
						"reaction": naver_value['reaction'],
						"comment": naver_value['comment'],
						"click": naver_value['click']
					})
					view_total += int(naver_value['view'])
					reaction_total += int(naver_value['reaction'])
					comment_total += int(naver_value['comment'])

				if key in kakao_map.keys():
					kakao_value = kakao_map[key]
					# 카카오 반응수 없음
					kakao_subset.update({
						"pc_view": kakao_value['pc_view'],
						"mobile_view": kakao_value['mobile_view'],
						"view": kakao_value['view'],
						"comment": kakao_value['comment']
					})
					view_total += int(kakao_value['view'])
					reaction_total += int(kakao_value['reaction'])
					comment_total += int(kakao_value['comment'])

				tpl.update({
					"jtbc": jtbc_subset,
					"naver": naver_subset,
					"kakao": kakao_subset,
					"view_total": view_total,
					"reaction_total": reaction_total,
					'comment_total': comment_total
				})

				# key = news_id
				index_id = "{0}_{1}".format(the_date.strftime('%Y%m%d%H'), key)
				data_set = {
					"_op_type": "update",
					"_id": index_id,
					"doc_as_upsert": True,
					"doc": tpl
				}
				hourly_set.append(data_set)

			bulk(self.es_client, hourly_set, index=constVar.HOURLY_NEWS_SUMMARY, refresh="wait_for")
			return True, hourly_set
		except Exception as es:
			# exc_type, exc_value, exc_traceback = sys.exc_info()
			# msg = repr(traceback.format_exception(exc_type, exc_value, exc_traceback))
			# logging.exception(msg)
			# self.logger.error(f"ERROR: {traceback.format_exc()}")
			raise
			# return False, hourly_set

	def aggrDailyNewSummary(self, the_date: datetime = None):
		daily_set = list()
		try:
			if the_date is None:
				raise Exception('날짜 지정 필수!')

			jtbc_ret, jtbc_map = self.jtbcData.getDataListDaily(the_date=the_date)
			naver_ret, naver_map = self.naverData.getDataListDaily(the_date=the_date)
			kakao_ret, kakao_map = self.kakaoData.getDataListDaily(the_date=the_date)

			mapping_news_ids = self.jtbcData.collect_prog_id_for_news_id_in_hourly_summary_index(the_date=the_date)
			self.logger.info(f"broadcast news_ids: {mapping_news_ids.keys()}")
			now_time = datetime.now().strftime('%Y-%m-%dT%H:%M:%S+09:00')
			for key, value in jtbc_map.items():
				# base > jtbc 데이터 기준
				jtbc_subset = {}
				naver_subset = {}
				kakao_subset = {}
				if mapping_news_ids.get(key):
					# self.logger.info(f"broadcast news_id: {key}")
					tpl = {
						"reg_date": the_date.strftime('%Y-%m-%dT00:00:00+09:00'),
						"news_id": key,
						"news_name": value['news_name'],
						**mapping_news_ids.get(key),
						"update_date": now_time
					}
				else:
					tpl = {
						"reg_date": the_date.strftime('%Y-%m-%dT00:00:00+09:00'),
						"news_id": key,
						"news_name": value['news_name'],
						"update_date": now_time
					}

				if 'section' in value:
					tpl.update({
						'section': value['section']
					})

				if 'source' in value:
					tpl.update({
						'source': value['source']
					})

				if 'reporter' in value:
					tpl.update({
						'reporter': value['reporter']
					})

				if 'service_date' in value:
					tpl.update({
						'service_date': value['service_date']
					})

				jtbc_view_total = int(value['pc_view']) + int(value['mobile_view']) + int(value['app_view'])
				jtbc_subset.update({
					"pc_view": value['pc_view'],
					"mobile_view": value['mobile_view'],
					"app_view": value['app_view'],
					"view": jtbc_view_total,
					"comment": value['comment'],
					"reaction": value['reaction']
				})

				view_total = jtbc_view_total
				reaction_total = int(value['reaction'])
				comment_total = int(value['comment'])

				if key in naver_map.keys():
					naver_value = naver_map[key]
					naver_subset.update({
						"pc_view": naver_value['pc_view'],
						"mobile_view": naver_value['mobile_view'],
						"view": naver_value['view'],
						"reaction": naver_value['reaction'],
						"comment": naver_value['comment'],
						"click": naver_value['click']
					})
					view_total += int(naver_value['view'])
					reaction_total += int(naver_value['reaction'])
					comment_total += int(naver_value['comment'])

				if key in kakao_map.keys():
					kakao_value = kakao_map[key]
					# 카카오 반응수 없음
					kakao_subset.update({
						"pc_view": kakao_value['pc_view'],
						"mobile_view": kakao_value['mobile_view'],
						"view": kakao_value['view'],
						"comment": kakao_value['comment']
					})
					view_total += int(kakao_value['view'])
					reaction_total += int(kakao_value['reaction'])
					comment_total += int(kakao_value['comment'])

				tpl.update({
					"jtbc": jtbc_subset,
					"naver": naver_subset,
					"kakao": kakao_subset,
					"view_total": view_total,
					"reaction_total": reaction_total,
					"comment_total": comment_total
				})

				# key = news_id
				index_id = "{0}_{1}".format(the_date.strftime('%Y%m%d'), key)
				data_set = {
					"_op_type": "update",
					"_id": index_id,
					"doc_as_upsert": True,
					"doc": tpl
				}
				daily_set.append(data_set)

			bulk(self.es_client, daily_set, index=constVar.DAILY_NEWS_SUMMARY, refresh="wait_for")
			return True, daily_set
		except Exception:
			# exc_type, exc_value, exc_traceback = sys.exc_info()
			# msg = repr(traceback.format_exception(exc_type, exc_value, exc_traceback))
			# logging.exception(msg)
			# self.logger.error(f"ERROR: {traceback.format_exc()}")
			raise
			# return False, daily_set

