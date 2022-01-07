import unittest
from datetime import datetime

# sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from main.module.ProduceNewsSumary import ProduceNewsSummary
from util.helper import Helper
import pprint


class TestProduceNewsSummary(unittest.TestCase):

	def __init__(self, *args, **kwargs):
		helper = Helper()
		self.pns = ProduceNewsSummary(helper=helper)
		super(TestProduceNewsSummary, self).__init__(*args, **kwargs)

	def test_aggr_produce_by_reporter(self):
		start_hour = datetime(2021, 11, 22, 0, 59, 59)
		ret, ds = self.pns.aggrProduceByReporter(the_date=start_hour)
		pprint.pprint(ds)

	@unittest.skip("later")
	def test_aggr_produce_by_department(self):
		start_date = datetime(2021, 10, 25, 0, 59, 59)
		ret, ds = self.pns.aggrProduceByDepartment(the_date=start_date)
		pprint.pprint(ds)

	@unittest.skip("later")
	def test_aggr_produce_by_section(self):
		start_date = datetime(2021, 10, 25, 0, 59, 59)
		ret, ds = self.pns.aggrProduceBySection(the_date=start_date)
		pprint.pprint(ds)

	@unittest.skip("later")
	def test_match_reporter_info(self):
		reporter_info = [
			{
				"no": "275",
				"depart_no": "51",
				"name": "손석희",
				"depart_name": "총괄사장"
			},
			{
				"no": "313",
				"depart_no": "10",
				"name": "안나경",
				"depart_name": "아나운서"
			}
		]
		res = self.pns.matchReporterInfo("275", reporter_info)
		pprint.pprint(res)

	@unittest.skip("later")
	def test_match_department_info(self):
		reporter_info = [
			{
				"no": "275",
				"depart_no": "51",
				"name": "손석희",
				"depart_name": "총괄사장"
			},
			{
				"no": "313",
				"depart_no": "10",
				"name": "안나경",
				"depart_name": "아나운서"
			}
		]
		res = self.pns.matchDepartmentInfo("10", reporter_info)
		pprint.pprint(res)


if __name__ == '__main__':
	# unittest.main()

	import sys

	# suite = unittest.TestSuite()
	# suite.addTests([TestHelper("test_parser"), TestHelper("test_helper")])
	suite = unittest.TestLoader().loadTestsFromTestCase(TestProduceNewsSummary)
	result = unittest.TextTestRunner(verbosity=2).run(suite)
	print(f"unittest result: {result}")
	print(f"result.wasSuccessful()={result.wasSuccessful()}")
	# 정상종료는 $1 에서 0을 리턴함
	sys.exit(not result.wasSuccessful())
