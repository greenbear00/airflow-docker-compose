import sys
import unittest
from main.job.NewsSummary import runner
from datetime import datetime, timedelta



class TestNewsSummary(unittest.TestCase):

    def setUp(self) -> None:
        pass

    def test_runner(self):
        target_date = datetime.now()
        target_date = datetime(2021,12,12) # do runner from 2021-12-06 02:00 to 2021-12-06 02:59
        flag = runner(target_date=target_date)
        print("=" * 100)
        print(flag)

        self.assertEqual(True, flag, "not working")

    @unittest.skip("run later.")
    def test_old_runner(self):
        # 네이버, 다음의 경우 2021.10.22일 부터 정상적인 데이터임 (_inc 데이터 오류로 인해 기존 데이터는 무시)
        # news_id(기사 id)와 prog_id(방송 프로그램 id)간에 맵핑 정보를 가지고 있는 테이블은 prog_date가 일 단위 기준으로
        # migration시 days=1 씩 증가해되 됨
        # target_date = datetime.now().replace(year=2021, month=10, day=22, hour=0, minute=0, second=0, microsecond=0)
        target_date = datetime.now().replace(year=2021, month=12, day=6, hour=0, minute=0, second=0, microsecond=0)
        result = {}
        flag_result = {}
        while target_date < datetime.now():
            flag = runner(target_date=target_date)
            result[target_date] = flag
            flag_result[target_date] = True
            target_date = target_date + timedelta(hours=1)

        self.assertEqual(flag_result, result, "not working.")

    def tearDown(self) -> None:
        print("testcase done")



if __name__ == '__main__':
    # unittest.main()

    import sys

    # suite = unittest.TestSuite()
    # suite.addTests([TestHelper("test_parser"), TestHelper("test_helper")])
    suite = unittest.TestLoader().loadTestsFromTestCase(TestNewsSummary)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    print(f"unittest result: {result}")
    print(f"result.wasSuccessful()={result.wasSuccessful()}")
    # 정상종료는 $1 에서 0을 리턴함
    sys.exit(not result.wasSuccessful())
