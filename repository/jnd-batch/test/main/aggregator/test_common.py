from unittest import TestCase
import os
import sys


from main.aggregator.JTBCData import JTBCData
from util.helper import Helper


class TestCommon(TestCase):
	helper = Helper()
	jtbc_data = JTBCData(helper)

	def __init__(self, *args, **kwargs):
		super(TestCommon, self).__init__(*args, **kwargs)

	# def test_aggr_jtbc_data(self):
	# 	target_date = datetime(2021, 8, 23, datetime.now().hour, datetime.now().minute, 59)
	# 	ret, res = self.jtbc_data.getDataListDaily(the_date=target_date)
	# 	pprint.pprint(res)