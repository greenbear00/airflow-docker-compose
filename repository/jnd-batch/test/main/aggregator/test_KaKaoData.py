from unittest import TestCase
import os
import sys
from datetime import datetime

from main.aggregator.KakaoData import KakaoData
from util.helper import Helper
import pprint


class TestKakaoData(TestCase):
	helper = Helper('dev')
	kakao_data = KakaoData(helper=helper)

	def __init__(self, *args, **kwargs):
		super(TestKakaoData, self).__init__(*args, **kwargs)

	def test_get_data_list_hourly(self):
		target_date = datetime(2021, 8, 23, 14)
		ret, res = self.kakao_data.getDataListHourly(the_date=target_date)
		pprint.pprint(res)

	def test_get_data_list_daily(self):
		target_date = datetime(2021, 8, 23, 23, 59, 59)
		ret, res = self.kakao_data.getDataListDaily(the_date=target_date)
		pprint.pprint(res)

	def test_aggr_news_data_by_id(self):
		self.fail()

	def test_push_temp_data(self):
		the_date = datetime(2021, 8, 17, 23, 59, 59)
		self.kakao_data.pushDayTempData(the_date=the_date)