from base import constVar
from main.aggregator.JTBCData import JTBCData
from util.helper import Helper
from datetime import datetime
import os
from util.JtbcLogger import JtbcLogger
from schema.SchemaGenerator import SchemaGenerator
from main.aggregator.ElasticGenerator import ElasticGenerator
from pathlib import Path


class UserSummary(ElasticGenerator):
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

	def get_user_summary_info(self, the_date: datetime, period:str='D') -> list:
		elk_data = {}
		if the_date is None:
			raise ValueError(f"the_date is None")

		try:
			user_info = self.jtbcData.collect_user_and_member_info_by_parmorigin_on_daterange(the_date=the_date, period=period)
			view_info = self.jtbcData.collect_news_view_by_parmorigin_on_daterange(the_date=the_date, period=period)

			if user_info:
				for k, v in user_info.items():
					elk_data[k] = v
			if view_info:
				for k, v in view_info.items():
					elk_data[k] = v

			if period=='D':
				reg_date = the_date.replace(hour=0, minute=0, second=0, microsecond=0).strftime(
					'%Y-%m-%dT00:00:00+09:00')
			else:
				reg_date = the_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0).strftime(
					'%Y-%m-%dT00:00:00+09:00')

			elk_data['reg_date'] = reg_date
			elk_data['update_date'] = datetime.now().strftime('%Y-%m-%dT%H:%M:%S+09:00')

			# daily: 당일 1번 update 하는 것으로 데이터가 1개 존재
			# montly: 월 1번 update 하는 것으로 데이터가 1개 존재
			if period=='D':
				doc_id = the_date.strftime('%Y%m%d')
			else:
				doc_id = the_date.strftime('%Y%m')
			elk_doc = [
				{
					'_op_type': "update",
					'_id': doc_id,
					"doc_as_upsert": True,
					"doc": elk_data
				}
			]
			self.logger.info(f"period={period}, reg_date = {reg_date}")
		except (ValueError, Exception):
			raise

		return elk_doc

	def _elk_write(self, the_date: datetime, elk_doc: list = None, period:str='D') -> list:
		try:
			if period == "D":
				reg_date = the_date.strftime('%Y-%m-%dT00:00:00+09:00')
				# index = origin-jtbcnews-user-daily-YYYY
				template_name, template, index_name, alias_name, aliases = \
					self.sg.get_template(the_date=the_date, template_name='user-daily-template',
										 index_name=constVar.USER_DAILY_SUMMARY)
			else:
				reg_date = the_date.strftime('%Y-%m-01T00:00:00+09:00')

				# index = origin-jtbcnews-user-montly-YYYY
				template_name, template, index_name, alias_name, aliases = \
					self.sg.get_template(the_date=the_date, template_name='user-monthly-template',
										 index_name=constVar.USER_MONTHLY_SUMMARY)
			self.do_elasitc_write(index_name=index_name, template_name=template_name, template=template,
								  alias_name=alias_name, aliases=aliases, elk_doc=elk_doc)
		except (ValueError, Exception):
			raise
		self.logger.info(f"upsert to {index_name} index by reg_date({reg_date})")

	def do_user_summary_info_by_daily(self, the_date: datetime):
		elk_doc = self.get_user_summary_info(the_date=the_date, period='D')
		self._elk_write(the_date=the_date, elk_doc=elk_doc, period='D')

	def do_user_summary_info_by_montly(self, the_date: datetime):
		elk_doc = self.get_user_summary_info(the_date=the_date, period='M')
		self._elk_write(the_date=the_date, elk_doc=elk_doc, period='M')
