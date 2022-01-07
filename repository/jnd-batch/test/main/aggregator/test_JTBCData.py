import unittest
from datetime import datetime, timedelta
from main.aggregator.JTBCData import JTBCData
from main.module.TVProInfo import TVProInfo
from util.helper import Helper
import pprint
import os
from util.JtbcLogger import JtbcLogger
from pathlib import Path
import time


class TestJTBCData(unittest.TestCase):

	def setUp(self) -> None:
		path = Path(__file__).parent.parent.parent.parent
		self.logger = JtbcLogger(path=os.path.join(path, "logs"), file_name=self.__class__.__name__).logger
		helper = Helper()
		self.jtbc_data = JTBCData(helper)
		self.tvpro = TVProInfo(helper=helper, level=helper.build_level)

	# def __init__(self, *args, **kwargs):
	# 	super(TestJTBCData, self).__init__(*args, **kwargs)
	@unittest.skip("later")
	def test_collect_election_news_by_news_id_on_day(self):
		target_date = datetime(2021, 11, 20)
		# data = self.jtbc_data.get_newsview_by_parmorigin(the_date=target_date, period='D')
		data = self.jtbc_data.collect_election_news_by_news_id_on_day(the_date=target_date,
																	  parm_referer_keys=['*election*', '*melection*'])
		print(f"target_date={target_date}")
		pprint.pp(data)

	@unittest.skip("later")
	def test_collect_election_news_view_by_parmorigin(self):
		target_date = datetime(2021, 11, 20)
		# data = self.jtbc_data.get_newsview_by_parmorigin(the_date=target_date, period='D')
		data = self.jtbc_data.collect_election_news_view_by_parmorigin(the_date=target_date,
																	   parm_referer_keys=['*election*', '*melection*'])
		print(f"target_date={target_date}")
		pprint.pp(data)
	@unittest.skip("later")
	def test_collect_jtbc_tv_prog_id_by_news_id(self):
		# index: origin-mapping-tv-news-program-YYYY에서 news_id에 대한 prog_id를 조회
		news_id = "NB12031120"
		the_date = datetime(2021, 12, 1)
		data = self.jtbc_data.collect_jtbc_tv_prog_id_by_news_id(news_id=news_id, the_date=the_date)
		pprint.pp(data)

	@unittest.skip("later")
	def test_collect_prog_id_for_news_id_in_hourly_summary_index(self):
		"하루 동안 hourly_news_summary 에서 prog_id가 있는 news_id 데이터만 추출"
		the_date = datetime(2021, 10, 22)
		mapping_news_ids = self.jtbc_data.collect_prog_id_for_news_id_in_hourly_summary_index(the_date=the_date)
		pprint.pp(mapping_news_ids)

	def test_repeat_mapping_prog_ids_for_news_in_hourly_news_summary(self):
		"신규 index에 prog_id, prog_name, contents_div를 추가 함 (news_id랑 매핑된 JTBC 뉴스 프로그램 맵핑)"
		start_time = time.time()
		the_date = datetime(2021, 10, 22)
		# the_date = datetime(2021,12,3)
		end_time = datetime.now()

		while the_date <end_time:
			index_name = 'origin-hourly-news-summary-2021.12.08'
			flag = self.mapping_prog_ids_for_news_in_hourly_news_summary(index_name=index_name,
																  the_date=the_date)
			if not flag:
				break

			the_date = the_date + timedelta(days=1)
		if flag:
			self.logger.info(f"Job was done : {time.time()-start_time}")
		else:
			self.logger.warning(f"migration failed...{the_date}")

	def mapping_prog_ids_for_news_in_hourly_news_summary(self, index_name:str, the_date:datetime):
		# the_date = datetime(2021, 12, 2)
		# data = self.jtbc_data.mapping_prog_id_for_news_id_in_hourly_news_summary(the_date=the_date)
		if index_name == None:
			index_name = 'origin-hourly-news-summary-2021.12.08'
		try:

			date_range = {
				"gte": the_date.strftime('%Y-%m-%dT00:00:00+09:00'),
				"lte": the_date.strftime('%Y-%m-%dT23:59:59+09:00')
			}
			body = {
				"size": 100,
				"query": {
					"range": {
						"reg_date": date_range
					}
				},
				"sort": [
					{
						"reg_date": {
							"order": "asc"
						}
					}
				]
			}
			# body2 = {
			# 	"query": {
			# 		"bool": {
			# 			"must": [
			# 				{
			# 					"term": {
			# 						"news_id": {
			# 							"value": "NB12035813"
			# 						}
			# 					}
			# 				}
			# 			],
			# 			"filter": [
			# 				{
			# 					"range": {
			# 						"reg_date": date_range
			# 					}
			# 				}
			# 			]
			# 		}
			# 	}
			# }
			# # access search context for 5 seconds before moving on to the next step, .
			scroll = '30s'
			response = self.jtbc_data.es_client.search(
				index=index_name,
				body=body,
				scroll=scroll
			)

			scroll_id = response['_scroll_id']
			scroll_size = response['hits']['total']['value']

			while scroll_size > 0:
				now_time = datetime.now().strftime('%Y-%m-%dT%H:%M:%S+09:00')
				new_mapping_jtbc_map = {}
				self.logger.info(f"<<< THE_DATE = {the_date.strftime('%Y-%m-%dT%H:%M:%S+09:00')} >>>")
				self.logger.info(f"scroll_size: {scroll_size}")
				# 해당 시간동안 수집한 news_id(list)
				collected_news_id = [x['_source']['news_id'] for x in response['hits']['hits']]
				unique_colleted_news_id = list(set(collected_news_id))
				self.logger.info(f"the_date = {the_date.strftime('%Y-%m-%dT%H:%M:%S+09:00')}")
				# self.logger.info(f'scroll unique news_id : {unique_colleted_news_id}')

				# news_id에 대해서 JTBC 방송프로그램에 등록된 news_id 찾기
				mapping_prog_ids = self.jtbc_data.collect_jtbc_tv_prog_id_by_news_ids(news_ids=unique_colleted_news_id,
																					  the_date=the_date)
				unique_prog_ids = list(set([a_data.get('prog_id') for a_data in mapping_prog_ids]))
				self.logger.info(f"news_id's unique_prog_ids: {unique_prog_ids}")
				if unique_prog_ids:
					unique_prog_ids_info = self.tvpro.get_nw_tv_program_by_prog_ids(prog_ids=unique_prog_ids)
					new_mapping_jtbc_map = self.tvpro.update_prog_id_for_news_ids(mapping_news_ids=mapping_prog_ids,
																				  prod_ids_info=unique_prog_ids_info)

				self.logger.info(f"mapping news_ids for prog_id: {list(set(new_mapping_jtbc_map.keys()))}")

				# for doc in response['hits']['hits']:
				# 	# do something with retrieved documents with current scroll
				# 	doc['_source']['news_id']
				filtered_news_ids = list(filter(lambda x: x['_source']['news_id'] in new_mapping_jtbc_map.keys(),
												response['hits']['hits']))
				for doc in filtered_news_ids:
					# 추가 메시지 update
					self.jtbc_data.es_client.update(
						index=index_name,
						id=doc['_id'],
						body={
							"doc":
								{
									**new_mapping_jtbc_map.get(doc['_source']['news_id']),
									"update_date": now_time
								}

						}
					)
				self.logger.info(
					f"mapping news_ids for prog_id : {len(filtered_news_ids)}/{len(response['hits']['hits'])}")

				response = self.jtbc_data.es_client.scroll(scroll_id=scroll_id, scroll=scroll)

				scroll_id = response['_scroll_id']
				scroll_size = len(response['hits']['hits'])
				self.logger.info('\n\n\n')

		except Exception as es:
			self.logger.error(f"ERROR: {es}")
			return False
		return True

	@unittest.skip("later")
	def test_collect_jtbc_tv_prog_id_by_news_id(self):
		# index: origin-mapping-tv-news-program-YYYY에서 news_id에 대한 prog_id를 조회
		news_ids = ["NB12030805", "NB12030695", "NB12024994", "NB10600629", "NB10597321",
					"NB12031279", "NB12031074", "NB12030798"]
		the_date = datetime(2021, 12, 1)
		data = self.jtbc_data.collect_jtbc_tv_prog_id_by_news_ids(news_ids=news_ids, the_date=the_date)
		pprint.pp(data)

	@unittest.skip("later")
	def test_get_newsview_by_parmorigin(self):
		target_date = datetime(2021, 11, 21)
		# data = self.jtbc_data.get_newsview_by_parmorigin(the_date=target_date, period='D')
		data = self.jtbc_data.collect_news_view_by_parmorigin_on_daterange(the_date=target_date, period='D')
		print(f"target_date={target_date}")
		pprint.pp(data)

	@unittest.skip("later")
	def test_get_user_and_member_info_by_parmorigin(self):
		target_date = datetime(2021, 11, 21)
		# data = self.jtbc_data.getUserAndMemberByParamOriginByDailiy(the_date=target_date, period='D')
		data = self.jtbc_data.collect_user_and_member_info_by_parmorigin_on_daterange(the_date=target_date, period='M')
		print(f"target_date={target_date}")
		pprint.pp(data)

	@unittest.skip("run later.")
	def test_get_reaction_map(self):
		target_date = datetime(2021, 8, 17)
		res = self.jtbc_data.getReactionHourlyMap(the_date=target_date)
		pprint.pprint(res)

	@unittest.skip("run later.")
	def test_get_comment_map(self):
		# target_date = datetime(2021, 8, 17, datetime.now().hour, datetime.now().minute, 59)
		# 댓글 지우는 건수가 더 많은 케이스가 있어서 확인 필요
		target_date = datetime(2021, 8, 17, 23, 59, 59)
		res = self.jtbc_data.getCommentHourlyMap(the_date=target_date)
		pprint.pprint(res)

	@unittest.skip("run later.")
	def test_getCommentById(self):
		the_date = datetime(2021, 8, 17, 16, 20, 30)
		self.jtbc_data.getCommentById(the_date=the_date, news_id='nb12020143')

	@unittest.skip("run later.")
	def test_getReactionById(self):
		the_date = datetime(2021, 8, 17, 16, 20, 30)
		self.jtbc_data.getReactionById(the_date=the_date, news_id='nb12020213')

	@unittest.skip("run later.")
	def test_getDataListHourly(self):
		target_date = datetime(2021, 9, 15, 16, 59, 59)
		ret, res = self.jtbc_data.getDataListHourly(the_date=target_date)
		pprint.pprint(res)

	@unittest.skip("run later.")
	def test_aggr_jtbc_data(self):
		target_date = datetime(2021, 10, 7, 15, 59)
		ret, res = self.jtbc_data.getDataListDaily(the_date=target_date)
		pprint.pprint(res)


if __name__ == "__main__":
	##########################################
	# local test시, 아래 전체 test suite를 실행시키기 위해서는
	# working directory를 /Users/jmac/project/jtbc_news_data/data-batch/로 셋팅하기
	# (상위를 바라봐야 하기 때문에)
	# 그리고 각각 test function을 실행시킬때는
	# [Edit Configuration]에서 module_name's target를
	# test_JTBCData.TestJTBCData.test_getNEWSViewByPARAMOriginBYDaily 로 수정
	##########################################
	# unittest.main()

	import sys

	# suite = unittest.TestSuite()
	# suite.addTests([TestHelper("test_parser"), TestHelper("test_helper")])
	suite = unittest.TestLoader().loadTestsFromTestCase(TestJTBCData)
	result = unittest.TextTestRunner(verbosity=2).run(suite)
	print(f"unittest result: {result}")
	print(f"result.wasSuccessful()={result.wasSuccessful()}")
	# 정상종료는 $1 에서 0을 리턴함
	sys.exit(not result.wasSuccessful())
