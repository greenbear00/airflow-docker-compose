from unittest import TestCase
from datetime import datetime
# sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from main.module.PlatformSummary import PlatformSummary
from util.helper import Helper


class TestProduceNewsSummary(TestCase):
	helper = Helper()
	pfs = PlatformSummary(helper=helper)

	def __init__(self, *args, **kwargs):
		super(TestProduceNewsSummary, self).__init__(*args, **kwargs)

	def test_aggrDailyPlatformSummary(self, the_date: datetime = None):
		the_date = datetime(2021, 11, 1)
		ret, res = self.pfs.aggrDailyPlatformSummary(the_date=the_date)
		import pprint
		pprint.pprint(res)
