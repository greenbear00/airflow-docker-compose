from unittest import TestCase
import os
import sys
from datetime import datetime

from main.aggregator.NaverData import NaverData
from util.helper import Helper
import pprint


class TestNaverData(TestCase):
	helper = Helper()
	naver_data = NaverData(helper=helper)

	def __init__(self, *args, **kwargs):
		super(TestNaverData, self).__init__(*args, **kwargs)

	def test_get_data_list_hourly(self):
		target_date = datetime(2021, 10, 15, 12)
		ret, res = self.naver_data.getDataListHourly(the_date=target_date)
		pprint.pprint(res)

	def test_get_data_list_daily(self):
		target_date = datetime(2021, 11, 1, 12)
		ret, res = self.naver_data.getDataListDaily(the_date=target_date)
		pprint.pprint(res)

	def test_aggr_news_data_by_id(self):
		pass

	def test_push_temp_data(self):
		the_date = datetime(2021, 8, 17, 23, 59, 59)
		self.naver_data.pushDayTempData(the_date=the_date)