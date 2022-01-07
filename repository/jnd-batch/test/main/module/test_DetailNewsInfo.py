from unittest import TestCase
from main.module.DetailNewsInfo import DetailNewsInfo
from util.helper import Helper
import pprint
from datetime import datetime
from util.JtbcLogger import JtbcLogger
from pathlib import Path
import os

class TestUpdateNewsInfo(TestCase):

	def __init__(self, *args, **kwargs):
		path = Path(__file__).parent.parent.parent.parent
		logger_factory = JtbcLogger(path=os.path.join(path, "logs"), file_name=self.__class__.__name__)
		logger = logger_factory.logger

		helper = Helper()
		self.detail_news = DetailNewsInfo(helper=helper)

		super(TestUpdateNewsInfo, self).__init__(*args, **kwargs)

	def test_get_news_data_by_id(self):
		ids = ["NB11066695", "NB12025181"]
		res = self.detail_news.getNewsDataById(id_list=ids)
		pprint.pprint(res)

	def test_get_VI_ELK_NEWS_BASIC_By_SERVICE_DT(self):
		end_time = datetime.now().replace(month=10, day=14, hour=17, minute=14)
		self.detail_news.get_vi_elk_news_basic_by_service_dt(end_time=end_time)

	def test_parseReporter(self):
		raw = "10,오대영,4,사회부|313,안나경,10,아나운서"
		res = self.detail_news.parseReporter(reporter_raw=raw)
		pprint.pprint(res)

	def test_parseSection(self):
		section_code = "50"
		section_name = "문화"
		res = self.detail_news.parseSection(section_code, section_name)
		pprint.pprint(res)

	def test_parseSourceCode(self):
		source_code = "11"
		source_name = "JTBC"
		res = self.update_news.parseSourceCode(source_code, source_name)
		pprint.pprint(res)

