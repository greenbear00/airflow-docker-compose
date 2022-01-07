import unittest
from datetime import datetime
from main.module.ElectionSummary import ElectionSummary
from util.helper import Helper
import pprint
class TestElectionSummary(unittest.TestCase):

	def setUp(self) -> None:
		helper = Helper()
		self.summary = ElectionSummary(helper)

	def test_collect_election_news_view_summary(self):
		the_date = datetime(2021,11,17)
		# period = D or M
		elk_data = self.summary.collect_election_news_view_summary(the_date=the_date)
		pprint.pp(elk_data)

		self.assertEqual(len(elk_data), 1, "fail")

	def test_collect_news_summary_by_news_id(self):
		the_date = datetime(2021,11,21)
		# period = D or M
		elk_data = self.summary.collect_election_news_summary_by_news_id(the_date=the_date)
		pprint.pp(elk_data)

		self.assertEqual(len(elk_data), 1, "fail")
