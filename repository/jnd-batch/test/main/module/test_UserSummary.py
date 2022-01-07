import unittest
from datetime import datetime
from main.module.UserSummary import UserSummary
from util.helper import Helper
import pprint
class TestUserSummary(unittest.TestCase):

	def setUp(self) -> None:
		helper = Helper()
		self.summary = UserSummary(helper)

	def test_get_user_summary_info(self):
		the_date = datetime(2021,11,21)
		# period = D or M
		elk_data = self.summary.get_user_summary_info(the_date=the_date, period='M')
		pprint.pp(elk_data)

		self.assertEqual(len(elk_data), 1, "fail")
