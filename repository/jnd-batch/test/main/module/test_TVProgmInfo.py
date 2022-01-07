import unittest
from main.module.TVProInfo import TVProInfo
from util.helper import Helper
import pprint
from datetime import datetime, timedelta
from util.JtbcLogger import JtbcLogger
from pathlib import Path
import os

class TestTVProgmInfo(unittest.TestCase):

	def setUp(self) -> None:
		path = Path(__file__).parent.parent.parent.parent
		logger_factory = JtbcLogger(path=os.path.join(path, "logs"), file_name=self.__class__.__name__)
		self.logger = logger_factory.logger

		helper = Helper()
		self.tv_pro = TVProInfo(helper=helper)

	def test_get_tv_program(self):
		result = self.tv_pro.get_tv_program()
		# select * from DB_JTBC_CON.dbo.TB_Code_MGT where GRP_CODE='CD'
		# GRP_CODE, DTL_CODE, ORDER_NO, CODE_NAME, CODE_ENG_NAME
		# CD,CU,3,시사교양,culture,admin,2011-09-28 00:00:00.000,http://tv.jtbc.joins.com/,Y,etribe_nomad,2016-11-20 19:25:46.040
		# CD,DR,1,드라마,drama,admin,2011-09-28 00:00:00.000,http://tv.jtbc.joins.com/,Y,etribe_nomad,2016-11-20 19:25:25.743
		# CD,EN,2,예능,enter,admin,2011-09-28 00:00:00.000,http://tv.jtbc.joins.com/,Y,etribe_nomad,2016-11-20 19:25:36.803
		# CD,ET,7,기타,media,admin,2011-10-04 17:13:30.267,http://tv.jtbc.joins.com/,Y,etribe_nomad,2016-11-20 19:26:13.440
		# CD,NW,4,보도,newson,admin,2011-09-28 00:00:00.000,http://newson.jtbc.joins.com/,Y,"",1900-01-01 00:00:00.000
		# CD,SP,5,스포츠,"",etribe_nomad,2016-10-25 12:38:29.117,"",N,etribe_nomad,2016-10-25 13:34:25.113
		# import pandas as pd
		# pdf = pd.DataFrame(result)
		# pdf
		for data in result:
			print(data)



	def test_get_news_tv_db(self):
		result = self.tv_pro.get_tv_program()
		for data in result:
			if data.get('PROG_ID') in ['PR10000403', 'PR10010250', 'PR10010301', 'PR10011338']:
				print(data.get('PROG_ID'), data.get('PROG_NAME'))


	def test_get_news_tv_db_by_prog_ids(self):
		prog_ids = ['PR10000403', 'PR10000405']
		result = self.tv_pro.get_nw_tv_program_by_prog_ids(prog_ids=prog_ids)
		pprint.pp(result)

		for a_prog_id in prog_ids:
			prog_name=None
			a_prog_ids_info = list(filter(lambda x: x.get('PROG_ID') == a_prog_id, result))
			if a_prog_ids_info:
				prog_name = a_prog_ids_info[0].get('PROG_NAME')
			print(f"{a_prog_id} -> {prog_name}")


	def test_generate_elk_data(self):
		"jtbc 방송 프로그램 중 뉴스 방송 프로그램에 대한 정보를 elk data로 형변환"
		result = self.test_get_news_tv_db()
		elk_doc = self.tv_pro.cast_to_elk_data_for_tv_progam(result)
		pprint.pp(elk_doc)

	def test_do_update_jtbc_program_info(self):
		the_date = datetime.now().replace(month=11, day=19)
		self.tv_pro.do_update_jtbc_program_info(the_date=the_date)


	def test_get_news_id_by_tv_program_based_on_date(self):
		"db에서 기사 id(news_id)와 jtbc 뉴스 방송 프로그램 id(prog_id) 정보 가져옴"
		the_date = datetime.now().replace(month=11,day=19)
		result = self.tv_pro.get_mapping_tv_news_program(the_date=the_date)
		pprint.pp(result)


	def test_cast_to_elk_data_for_mapping_tv_news_program(self):
		"elk data로 기사 id(news_id)와 jtbc 뉴스 방송 프로그램 id(prog_id)와 매핑"
		the_date = datetime.now().replace(month=11,day=20)
		result = self.tv_pro.get_mapping_tv_news_program(the_date=the_date)
		# pprint.pp(result)
		result2 = self.tv_pro.cast_to_elk_data_for_mapping_tv_news_program(the_date=the_date, data=result)
		pprint.pp(result2)

	def test_do_update_mapping_tv_news_program(self):
		" test 기사 id(news_id)와 jtbc 뉴스 방송 프로그램 id(prog_id)와 매핑"
		the_date = datetime.now().replace(month=11, day=20)
		self.tv_pro.delete_index_template('mapping-tv-news-program-template')
		self.tv_pro.delete_index(index_name=f"origin-mapping-tv-news-program-{the_date.strftime('%Y')}")

		self.tv_pro.do_update_mapping_tv_news_program(the_date=the_date)

	def test_do_update_mapping_tv_news_program_migration(self):
		" test 기사 id(news_id)와 jtbc 뉴스 방송 프로그램 id(prog_id)와 매핑"
		the_date = datetime.now().replace(month=11, day=28, hour=0, minute=0, second=0)
		# end_date = datetime.now().replace(month=11, day=28, hour=0, minute=0, second=0)
		end_date = datetime.now()
		# self.tv_pro.delete_index_template('mapping-tv-news-program-template')
		# self.tv_pro.delete_index(index_name=f"origin-mapping-tv-news-program-{the_date.strftime('%Y')}")

		while the_date < end_date:
			self.logger.info(f"<<< THE_DATE = {the_date.strftime('%Y-%m-%dT%H:%M:%S+09:00')} >>>")
			self.tv_pro.do_update_mapping_tv_news_program(the_date=the_date)
			# the_date = the_date + timedelta(hours=1)
			the_date = the_date + timedelta(days=1)
			self.logger.info("\n\n\n")







