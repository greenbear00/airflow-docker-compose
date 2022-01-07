from main.aggregator.ElasticGenerator import ElasticGenerator
from schema.SchemaGenerator import SchemaGenerator
from util.helper import Helper
import os
from util.JtbcLogger import JtbcLogger
from pathlib import Path
from base import constVar
from datetime import datetime

def _remove_empty_in_list(li:list):
	li.remove('') if '' in li else None
	return li

class TVProInfo(ElasticGenerator):

	def __init__(self, helper: Helper, level: str = 'dev'):
		self.helper = helper

		self.sg = SchemaGenerator(obj=self, level=level)
		self.es_client = helper.get_es()
		self.jtbc_tv_db = helper.get_tv_db()
		self.jtbc_news_db = helper.get_news_db()

		if os.path.isdir('/box/jnd-batch'):
			# prod
			path = '/box/jnd-batch'
		else:
			# dev
			path = Path(__file__).parent.parent.parent

		logger_factory = JtbcLogger(path=os.path.join(path, "logs"), file_name=type(self).__name__)
		self.logger = logger_factory.logger

		super().__init__(path=path, es_client=self.es_client)

	def __del__(self):
		self.es_client.transport.close()
		# self.jtbc_tv_db.close()

	def _elk_write(self, the_date: datetime, elk_doc:list, doc_type:str='mapping'):
		try:
			reg_date = the_date.strftime('%Y-%m-%dT%H:%M:%S+09:00')
			if doc_type=='mapping':
				template_name, template, index_name, alias_name, aliases = \
					self.sg.get_template(the_date=the_date, template_name='mapping-tv-news-program-template',
										 index_name=constVar.JTBC_MAPPING_TV_NEW_PROGRAM)
			else:
				template_name, template, index_name, alias_name, aliases = \
					self.sg.get_template(the_date=the_date, template_name='jtbc-program-info-template',
										 index_name=constVar.JTBC_PROGRAM_INFO)
			self.do_elasitc_write(index_name=index_name, template_name=template_name, template=template,
								  alias_name=alias_name, aliases=aliases, elk_doc=elk_doc)
		except (ValueError, Exception):
			raise
		self.logger.info(f"upsert to {index_name} index by reg_date({reg_date})")

	def cast_to_elk_data_for_tv_progam(self, data: list) -> list:
		"""
		jtbc 뉴스 프로그램 정보에 대해서 elk 데이터로 형변환 수행
		:param data:
		:return:
		"""
		elk_data = []
		for a_data in data:
			elk_data.append(
				{
					'_op_type': "update",
					'_id': a_data.get('PROG_ID'),
					"doc_as_upsert": True,
					"doc": a_data
				}
			)
		return elk_data

	def cast_to_elk_data_for_mapping_tv_news_program(self, the_date:datetime, data:list)->list:
		"""
		동일한 prog_date에 대해서 jtbc 뉴스 기사와 실제 방송 프로그램과 매핑된 정보를 elk 데이터로 형 변환
		"""
		elk_data = []
		for a_data in data:
			prog_id = a_data.pop('PROG_ID')
			prog_date = a_data.pop('PROG_DATE')
			modify_date = dict(filter(lambda x: isinstance(x[1], datetime), a_data.items()))
			news_id_n = _remove_empty_in_list(li=list(a_data.get('NEWS_ID_N').split(',')))
			hot_news = _remove_empty_in_list(li=list(a_data.get('HOT_NEWS').split(',')))

			# for news_id in news_id_n:
			# 	elk_data.append(
			# 		{
			# 			'_op_type': "update",
			# 			'_id': f"{prog_date}_{prog_id}_{news_id}",
			# 			"doc_as_upsert": True,
			# 			"doc":
			# 				{
			# 					"prg_id": prog_id,
			# 					'prog_date': prog_date,
			# 					'news_id': news_id,
			# 					'hot_news': news_id if news_id in hot_news else None,
			# 					'update_date': datetime.now().strftime('%Y-%m-%dT%H:%M:%S+09:00'),
			# 					**{key.lower(): value.strftime('%Y-%m-%dT%H:%M:%S+09:00') for key, value in
			# 					   modify_date.items()},
			#
			# 				}
			#
			# 		}
			# 	)

			self.logger.info(f"prog_date = {prog_date}")
			self.logger.info(f" - prog_id = {prog_id}")
			self.logger.info(f" - news_id = {news_id_n}")
			elk_data.append(
				{
					'_op_type': "update",
					'_id': f"{prog_date}_{prog_id}",
					"doc_as_upsert": True,
					"doc": {
						'prog_id': prog_id,
						'news_id': news_id_n,
						'hot_news': hot_news,
						'prog_date': prog_date,
						'update_date': datetime.now().strftime('%Y-%m-%dT%H:%M:%S+09:00'),
						**{key.lower(): value.strftime('%Y-%m-%dT%H:%M:%S+09:00') for key, value in modify_date.items()}
					}
				}
			)
		return elk_data


	def get_mapping_tv_news_program(self, the_date: datetime):
		"""
		select * from DB_JTBC_CON.dbo.TB_Code_MGT where GRP_CODE='CD'
		:param the_date:
		:return:
		"""
		result = {}
		db_cursor = self.jtbc_tv_db.cursor(as_dict=True)

		try:
			sql = f"""
							select * from db_jtbc_news.dbo.tb_news_replay where prog_date=
							 {the_date.strftime('%Y%m%d')}
							"""
			self.logger.info(f"query = {sql}")

			db_cursor.execute(sql)
			result = db_cursor.fetchall()

		except Exception as es:
			raise
		finally:
			db_cursor.close()

		return result


	def update_prog_id_for_news_ids(self, mapping_news_ids:list, prod_ids_info:list)->dict:
		"""
		news_id에 대해 prog_id가 매핑된 데이터에 대해서
		porg_id 기준으로 실제 JTBC 방송 프로그램 정보들을 가져와서
		기존 데이터(mapping_news_id)에 prog_name과 contents_div를 추가 함

		:param mapping_news_ids: JTBCData.collect_jtbc_tv_prog_id_by_news_ids()를 통해 수집한 news_id에 따른 porg_id 정보
			예: [{'news_id': 'NB12035085', 'prog_id': 'PR10010250'},..., {'news_id': 'NB12035089', 'prog_id':
			'PR10010250'}]
		:param prod_ids_info: TVProInfo.get_nw_tv_program_by_prog_ids()를 통해 수집한 prog_ids에 따른 방송 프로그램 정보(list)
		:return:
			{...
			 'NB12035085': {'news_id': 'NB12035085',
							'prog_id': 'PR10010250',
							'prog_name': '아침&',
							'contents_div': 'NW'},
			 'NB12035089': {'news_id': 'NB12035089',
							'prog_id': 'PR10010250',
							'prog_name': '아침&',
					'contents_div': 'NW'}}
		"""

		summary_pro_info = {}
		for a_pro_info in prod_ids_info:
			summary_pro_info[a_pro_info.get('PROG_ID')] = {
				'prog_name': a_pro_info.get('PROG_NAME'),
				'contents_div': a_pro_info.get('CONTENTS_DIV')
			}

		new_mapping_news_ids = {}
		for a_data in mapping_news_ids:
			new_mapping_news_ids[a_data.get('news_id')] = {**a_data, **summary_pro_info.get(a_data.get(
				'prog_id'))}
		return new_mapping_news_ids

	def get_nw_tv_program_by_prog_ids(self, prog_ids:list)-> list:
		"""
		prog_id를 기반하여 JTBC 방송 프로그램 정보를 추출한다.
		:param prog_ids:
		:return:
		"""
		result = {}
		db_cursor = self.jtbc_tv_db.cursor(as_dict=True)
		if prog_ids:
			prog_ids_query = f"('{prog_ids[0]}')" if len(prog_ids)==1 else tuple(map(str, prog_ids))

			try:
				# select * from tb_prog_basic where CONTENTS_DIV ='NW' and prog_id in {prog_ids_query}
				sql = f"""
						select * from tb_prog_basic where prog_id in {prog_ids_query}
						"""
				self.logger.info(f"query = {sql}")

				db_cursor.execute(sql)
				result = db_cursor.fetchall()

			except Exception as es:
				raise
			finally:
				db_cursor.close()

		return result

	def get_tv_program(self)->list:
		result = {}
		db_cursor = self.jtbc_tv_db.cursor(as_dict=True)

		try:
			sql = f"""
							select * from tb_prog_basic 
							"""
			self.logger.info(f"query = {sql}")

			db_cursor.execute(sql)
			result = db_cursor.fetchall()

		except Exception as es:
			raise
		finally:
			db_cursor.close()

		return result

	def get_nw_tv_program(self)->list:
		"""
		jtbc 방송 프로그램 중 뉴스관련 정보 전체를 가져옴(매일 01시에 수행)
		:return:
		"""
		result = {}
		db_cursor = self.jtbc_tv_db.cursor(as_dict=True)

		try:
			sql = f"""
					select * from tb_prog_basic where CONTENTS_DIV ='NW'
					"""
			self.logger.info(f"query = {sql}")

			db_cursor.execute(sql)
			result = db_cursor.fetchall()

		except Exception as es:
			raise
		finally:
			db_cursor.close()

		return result

	def do_update_jtbc_program_info(self, the_date: datetime):
		"""
		jtbc 방송 프로그램 중 뉴스관련 프로그램 정보 가져옴(매일 01시에 수행)
		:param the_date:
		:return:
		"""
		jtbc_news_program_info = self.get_tv_program()
		elk_doc = self.cast_to_elk_data_for_tv_progam(data=jtbc_news_program_info)
		self._elk_write(the_date=the_date, elk_doc=elk_doc, doc_type='news_tv_info')


	def do_update_mapping_tv_news_program(self, the_date:datetime):
		"""
		기사 id(news_id)와 jtbc 뉴스 방송 프로그램 id(prog_id) 매핑
		index: origin-mapping-tv-news-program-YYYY
		:param the_date:
		:return:
		"""
		mapping_data = self.get_mapping_tv_news_program(the_date=the_date)
		elk_doc = self.cast_to_elk_data_for_mapping_tv_news_program(the_date=the_date, data=mapping_data)
		self._elk_write(the_date=the_date, elk_doc=elk_doc, doc_type='mapping')

