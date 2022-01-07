from pathlib import Path
import unittest
import os
import sys
from datetime import datetime, timedelta
import time

from main.module.DetailNewsInfo import DetailNewsInfo
from util.JtbcLogger import JtbcLogger

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from main.module.UpdateNewsInfo import UpdateNewsInfo
from util.helper import Helper
import pprint


class TestUpdateNewsInfo(unittest.TestCase):

	def __init__(self, *args, **kwargs):

		path = Path(__file__).parent.parent.parent.parent
		self.logger = JtbcLogger(path=os.path.join(path, "logs"), file_name=self.__class__.__name__).logger
		helper = Helper('dev')
		self.update_news = UpdateNewsInfo(helper=helper)
		super(TestUpdateNewsInfo, self).__init__(*args, **kwargs)

	@unittest.skip("later")
	def test_getNewsDataMap(self):
		# 2011-11-28 20:55:51.470 (수정된 최초 날짜)
		last_time = datetime(2021, 11, 1, 00, 00, 00)
		ret, ds = self.update_news.getNewsData(last_time=last_time)
		print(len(ds))
		pprint.pprint(ds)

	@unittest.skip("later")
	def test_UpdateNewsSummary(self):
		last_time = datetime(2021, 9, 28, 00, 00, 00)
		ret, ds = self.update_news.getNewsData(last_time=last_time)
		res = self.update_news.UpdateNewsSummary(news_set=ds, last_time=last_time)
		pprint.pprint(res)

	@unittest.skip("later")
	def test_UpsertNewsBasicInfo(self):
		last_time = datetime(2021, 10, 7, 00, 00, 00)
		ret, ds = self.update_news.getNewsData(last_time=last_time)
		res = self.update_news.UpsertNewsBasicInfo(news_set=ds)
		pprint.pprint(res)

	@unittest.skip("later")
	def test_mapping_prog_ids_for_news_in_basic_news_info(self):
		the_date = datetime(2021, 12, 1)
		self.repeat_UpsertNewsBasicInfo(the_date=the_date)

	def test_repeat_mapping_prog_ids_for_news_in_basic_news_info(self):
		start_time = time.time()
		the_date = datetime(2021, 10, 22)
		end_time = datetime.now()

		while the_date < end_time:
			flag = self.repeat_UpsertNewsBasicInfo(the_date=the_date)
			if not flag:
				break
			the_date = the_date + timedelta(days=1)

		if flag:
			self.logger.info(f"Job was done : {time.time() - start_time}")
		else:
			self.logger.warning(f"migration failed...{the_date}")

	def repeat_UpsertNewsBasicInfo(self, the_date: datetime):
		try:
			index_name = "origin-basic-news-info-2021.12.08"
			self.logger.info(f"<<< THE_DATE = {the_date.strftime('%Y-%m-%d')} >>>")
			flag = self.update_news.repeat_upsert_newsbasicinfo(the_date=the_date, index_name=index_name)

		except Exception as es:

			self.logger.error(f"ERROR = {es}")
			flag = False

		return flag


if __name__ == '__main__':
	# unittest.main()

	import sys

	# suite = unittest.TestSuite()
	# suite.addTests([TestHelper("test_parser"), TestHelper("test_helper")])
	suite = unittest.TestLoader().loadTestsFromTestCase(TestUpdateNewsInfo)
	result = unittest.TextTestRunner(verbosity=2).run(suite)
	print(f"unittest result: {result}")
	print(f"result.wasSuccessful()={result.wasSuccessful()}")
	# 정상종료는 $1 에서 0을 리턴함
	sys.exit(not result.wasSuccessful())
