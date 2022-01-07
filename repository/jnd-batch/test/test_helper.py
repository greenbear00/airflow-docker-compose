import unittest
from util.helper import Helper, Parser
from datetime import datetime


class TestHelper(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(TestHelper, self).__init__(*args, **kwargs)

    @unittest.skip("later..")
    def test_get_news_db(self):
        # obj_helper = Helper('dev')
        obj_helper = Helper()
        news_db = obj_helper.get_news_db()
        self.assertTrue(news_db)
        cursor = news_db.cursor(as_dict=True)
        # 기사정보 배치 만들때 최초에는 NEWS_ID를 조건으로 넘겨서 해당 기사정보를 가져와서 Upsert 해야함
        # 이후에는 MOD_DT를 체크해서 업데이트 반영
        sql = '''
            SELECT TOP 10 * FROM VI_ELK_NEWS_BASIC
        '''
        cursor.execute(sql)
        items = cursor.fetchall()
        import pprint
        pprint.pprint(items)
        cursor.close()

    def test_get_es(self):
        # obj_helper = Helper('dev')
        obj_helper = Helper()
        es = obj_helper.get_es()

        # self.assertEqual(es.transport.hosts[0]['host'], '192.168.171.167')
        print(obj_helper.build_level)
        self.assertEqual(es.transport.hosts[0]['host'], obj_helper.config['elastic']['hosts'].split(":")[0])

    def test_parser(self):
        p = Parser()
        # print(p.build_level)
        # print(p.auto_apply_alias)
        obj_helper = Helper(env=p.build_level)

        self.assertEqual(obj_helper.build_level, p.build_level)

    def test_helper(self):
        # helper = Helper(env="prod")
        helper = Helper()
        slack_client = helper.get_slack()
        print("build_level: ", helper.build_level)
        print("slack instance: ", slack_client)
        print("slack enable: ", helper.get_slack().enable)
        print("slack channel_name: ", helper.get_slack().channel_name)
        print("slack token: ", helper.get_slack().token)

        p = Parser()
        self.assertEqual(p.build_level, helper.build_level)
        self.assertEqual(p.slack_token, slack_client.token)
        self.assertEqual(p.slack_channel, slack_client.channel_name)
        self.assertEqual(p.slack_enable, slack_client.enable)

        if helper.build_level == "dev":
            msg = "1) Register crontab based on /box/jnd-batch."
        else:
            # msg = f"1) test /box/jnd-batch-tmp/script\n"\
            #         f"2) test crontab and check logs\n"\
            #         f"3) mv /box/jnd-batch /box/jnd-batch-{datetime.now().strftime('%Y%m%d')}\n"\
            #         f"4) mv /box/jnd-batch-tmp /box/jnd-batch\n"\
            #         f"5) Register crontab based on /box/jnd-batch.\n"\
            #         f"6) mkdir /box/jnd-batch/logs\n" \
            #         f"7) cp /box/jnd-batch-{datetime.now().strftime('%Y%m%d')}/logs /box/jnd-batch\n"
            msg = "1) Register crontab based on /box/jnd-batch."

        print(msg)
        helper.get_slack().post_message(msg=f"[{helper.build_level.upper()}] hi, bamboo build job was successful "
                                            f"at {datetime.now()}. \n"
                                            f"{msg}")


if __name__ == "__main__":
    # unittest.main()
    import sys

    # suite = unittest.TestSuite()
    # suite.addTests([TestHelper("test_parser"), TestHelper("test_helper")])
    suite = unittest.TestLoader().loadTestsFromTestCase(TestHelper)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    print(f"unittest result: {result}")
    print(f"result.wasSuccessful()={result.wasSuccessful()}")
    # 정상종료는 $1 에서 0을 리턴함
    sys.exit(not result.wasSuccessful())
