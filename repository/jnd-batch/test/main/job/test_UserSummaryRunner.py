import sys
import unittest
from main.job.UserSummaryRunner import runner
from datetime import datetime, timedelta



class UserSummaryRunnerTest(unittest.TestCase):

    def setUp(self) -> None:
        # self.runner = UserSummaryRunner()
        pass

    def test_runner(self):
        target_date = datetime.now()
        # flag = self.runner.run(target_date)
        flag = runner(target_date)

        self.assertEqual(True, flag, "not working")

    @unittest.skip("run later.")
    def test_old_runner(self):
        # 네이버, 다음의 경우 2021.10.22일 부터 정상적인 데이터임 (_inc 데이터 오류로 인해 기존 데이터는 무시)
        target_date = datetime.now().replace(year=2021, month=10, day=22, hour=0, minute=0, second=0, microsecond=0)
        result = {}
        flag_result = {}
        while target_date < datetime.now():
            flag = runner(target_date)
            result[target_date] = flag
            flag_result[target_date] = True
            target_date = target_date + timedelta(days=1)

        self.assertEqual(flag_result, result, "not working.")

    def tearDown(self) -> None:
        print("testcase done")



if __name__ == '__main__':
    # unittest.main()

    import sys

    # suite = unittest.TestSuite()
    # suite.addTests([TestHelper("test_parser"), TestHelper("test_helper")])
    suite = unittest.TestLoader().loadTestsFromTestCase(UserSummaryRunnerTest)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    print(f"unittest result: {result}")
    print(f"result.wasSuccessful()={result.wasSuccessful()}")
    # 정상종료는 $1 에서 0을 리턴함
    sys.exit(not result.wasSuccessful())