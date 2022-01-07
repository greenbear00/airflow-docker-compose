from base import constVar
from main.aggregator.JTBCData import JTBCData
from util.helper import Helper
from datetime import datetime
import os
from util.JtbcLogger import JtbcLogger
from schema.SchemaGenerator import SchemaGenerator
from main.aggregator.ElasticGenerator import ElasticGenerator
from pathlib import Path


class ElectionSummary(ElasticGenerator):
	def __init__(self, helper: Helper, level: str = 'dev'):
		self.sg = SchemaGenerator(obj=self, level=level)

		self.es_client = helper.get_es()
		self.jtbcData = JTBCData(helper)

		self.level = level

		if os.path.isdir('/box/jnd-batch'):
			# prod
			path = '/box/jnd-batch'
		else:
			# dev
			path = Path(__file__).parent.parent.parent

		logger_factory = JtbcLogger(path=os.path.join(path, "logs"), file_name=type(self).__name__)
		self.logger = logger_factory.logger

		super().__init__(path=path, es_client=self.es_client)

	def collect_election_news_view_summary(self, the_date: datetime) -> list:
		elk_data = {}
		if the_date is None:
			raise ValueError(f"the_date is None")

		try:
			view_info = self.jtbcData.collect_election_news_view_by_parmorigin(the_date=the_date,
																			   parm_referer_keys=['*election*',
																								  '*melection*'])
			if view_info:
				for k, v in view_info.items():
					elk_data[k] = v

			reg_date = the_date.replace(hour=0, minute=0, second=0, microsecond=0).strftime(
				'%Y-%m-%dT00:00:00+09:00')

			elk_data['reg_date'] = reg_date
			elk_data['update_date'] = datetime.now().strftime('%Y-%m-%dT%H:%M:%S+09:00')

			# daily: 당일 1번 update 하는 것으로 데이터가 1개 존재
			doc_id = the_date.strftime('%Y%m%d')
			elk_doc = [
				{
					'_op_type': "update",
					'_id': doc_id,
					"doc_as_upsert": True,
					"doc": elk_data
				}
			]
			self.logger.info(f"reg_date = {reg_date}")
		except (ValueError, Exception):
			raise

		return elk_doc

	def collect_election_news_summary_by_news_id(self, the_date: datetime) -> list:
		elk_docs = []
		if the_date is None:
			raise ValueError(f"the_date is None")

		try:
			news_li = self.jtbcData.collect_election_news_by_news_id_on_day(the_date=the_date,
																			parm_referer_keys=['*election*', '*melection*'])
			if news_li:
				for a_news in news_li:

					reg_date = the_date.replace(hour=0, minute=0, second=0, microsecond=0).strftime(
						'%Y-%m-%dT00:00:00+09:00')

					a_news['reg_date'] = reg_date
					a_news['update_date'] = datetime.now().strftime('%Y-%m-%dT%H:%M:%S+09:00')

					doc_id = reg_date.split("T")[0].replace("-", "") +"_"+ a_news.get('news_id')
					elk_docs.append(
						{
							'_op_type': "update",
							'_id': doc_id,
							"doc_as_upsert": True,
							"doc": a_news
						}
					)
				self.logger.info(f"reg_date = {reg_date}")
			else:
				self.logger.warning(f"news_id doesn't exist")
		except (ValueError, Exception):
			raise

		return elk_docs

	def _elk_write(self, the_date: datetime, elk_doc: list = None, data_type: str = 'view') -> list:
		try:
			if data_type == "view":
				reg_date = the_date.strftime('%Y-%m-%dT00:00:00+09:00')
				# index = origin-jtbcnews-user-daily-YYYY
				template_name, template, index_name, alias_name, aliases = \
					self.sg.get_template(the_date=the_date, template_name='election-user-daily-template',
										 index_name=constVar.ELECTION_USER_DAILY_SUMMARY)

			else:
				reg_date = the_date.strftime('%Y-%m-%dT00:00:00+09:00')
				# index = origin-jtbcnews-user-daily-YYYY
				template_name, template, index_name, alias_name, aliases = \
					self.sg.get_template(the_date=the_date,
										 template_name='election-daily-news-template',
										 index_name=constVar.ELECTION_DAILY_NEWS_SUMMARY)

			self.do_elasitc_write(index_name=index_name, template_name=template_name, template=template,
								  alias_name=alias_name, aliases=aliases, elk_doc=elk_doc)

		except (ValueError, Exception):
			raise
		self.logger.info(f"upsert to {index_name} index by reg_date({reg_date})")

	def do_election_news_summary_info_by_daily(self, the_date: datetime):
		# election-user-daily는 선거 관련 사용자 지표
		elk_doc = self.collect_election_news_view_summary(the_date=the_date)
		self._elk_write(the_date=the_date, elk_doc=elk_doc, data_type="view")

		# election-daily-news는 선거 관련 뉴스 일별 지표
		elk_doc2 = self.collect_election_news_summary_by_news_id(the_date=the_date)
		self._elk_write(the_date=the_date, elk_doc=elk_doc2, data_type="news_id")
