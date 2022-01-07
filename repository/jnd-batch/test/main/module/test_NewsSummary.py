import unittest
import os
import sys
from datetime import datetime, timedelta
from main.module.NewsSummary import NewsSummary
from main.aggregator.JTBCData import JTBCData
from main.aggregator.KakaoData import KakaoData
from main.aggregator.NaverData import NaverData
from main.module.TVProInfo import TVProInfo
from util.helper import Helper
import pprint
from pathlib import Path
from util.JtbcLogger import JtbcLogger
import time


class TestNewsSummary(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        path = Path(__file__).parent.parent.parent.parent
        self.logger = JtbcLogger(path=os.path.join(path, "logs"), file_name=self.__class__.__name__).logger

        helper = Helper()
        self.newsSummary = NewsSummary(helper=helper)
        self.naverdata = NaverData(helper=helper)
        self.jtbcdata = JTBCData(helper=helper)
        self.tvpro = TVProInfo(helper=helper, level=helper.build_level)
        super(TestNewsSummary, self).__init__(*args, **kwargs)

    @unittest.skip("later")
    def test_mapping_news_id_and_program_id(self):
        # 2021-11-28일 00시 부터 +1시간씩 증가하여 2021-12-01 16시까지 검증한 결과
        # news_id에 대해서 모두 prog_id가 1:1 매핑되며, prog_id가 없는 news_id는 없음을 확인함
        start_hour = datetime(2021, 11, 28, 0, 0, 0)
        while start_hour <datetime.now():
            _, jtbc_map = self.jtbcdata.getDataListDaily(the_date=start_hour)
            data = self.jtbcdata.collect_jtbc_tv_prog_id_by_news_ids(news_ids=list(jtbc_map.keys()), the_date=start_hour)
            for a_data in data:
                if len(a_data.get('prog_id'))!=1:
                    print(a_data)
                    print("ERROR")
                    break
            start_hour = start_hour+timedelta(hours=1)

    @unittest.skip("later")
    def test_mapping_news_id_and_program_id(self):
        # 2021-11-28일 00시 부터 +1시간씩 증가하여 2021-12-01 16시까지 검증한 결과
        # news_id에 대해서 모두 prog_id가 1:1 매핑되며, prog_id가 없는 news_id는 없음을 확인함
        start_hour = datetime(2021, 11, 28, 0, 0, 0)
        _, jtbc_map = self.jtbcdata.getDataListDaily(the_date=start_hour)
        data = self.jtbcdata.collect_jtbc_tv_prog_id_by_news_ids(news_ids=list(jtbc_map.keys()), the_date=start_hour)
        pprint.pp(data)

        unique_prog_ids = list(set([a_data.get('prog_id') for a_data in data]))
        print("unique_prog_ids: ", unique_prog_ids)

        unique_prog_ids_info = self.tvpro.get_nw_tv_program_by_prog_ids(prog_ids=unique_prog_ids)
        pprint.pp(unique_prog_ids_info)

        # summary_pro_info = {}
        # for a_pro_info in unique_prog_ids_info:
        # 	summary_pro_info[a_pro_info.get('PROG_ID')] = {
        # 		'PROG_NAME': a_pro_info.get('PROG_NAME'),
        # 		'CONTENTS_DIV': a_pro_info.get('CONTENTS_DIV')
        # 	}
        #
        # for a_data in data:
        # 	a_data.update(**summary_pro_info.get(a_data.get('prog_id')))
        mapping_data = self.tvpro.update_prog_id_for_news_ids(mapping_news_ids=data, prod_ids_info=unique_prog_ids_info)

        print("="*100)
        pprint.pp(mapping_data)

    @unittest.skip("later")
    def test_aggr_hourly_news_summary(self):
        start_hour = datetime(2021, 11, 30, 12, 0, 0)
        ret, dt = self.newsSummary.aggrHourlyNewsSummary(the_date=start_hour)
        pprint.pprint(dt)

    @unittest.skip("later")
    def test_repeat_aggr_hourly_news_sumamry(self):
        # start_hour = datetime(2021, 10, 22, 00, 00, 00)
        # end_hour = datetime(2021, 11, 1, 00, 00, 00)
        start_hour = datetime(2021, 11, 1, 00, 00, 00)
        end_hour = datetime(2021, 12, 1, 0, 0, 0)

        while start_hour <= end_hour:
            print("\n\n\n", "="*100)
            print(start_hour.strftime('%Y.%m.%d %H:%M:%S'))
            ret, dt = self.newsSummary.aggrHourlyNewsSummary(the_date=start_hour)
            # print(dt)

            # if start_hour.strftime('%H') == '23':
            # print('in here')
            # ret2, dt2 = self.newsSummary.aggrDailyNewSummary(the_date=start_hour)
            # print('daily:', dt2)

            start_hour = start_hour + timedelta(hours=1)

    @unittest.skip("later")
    def test_diff_daily_news_summary(self):
        the_date = datetime(2021, 11, 1)
        ret1, naver_map = self.naverdata.getDataListDaily(the_date=the_date)
        ret2, jtbc_map = self.jtbcdata.getDataListDaily(the_date=the_date)
        diff_keys = set(naver_map.keys()) - set(jtbc_map.keys())
        c = dict()
        for key in diff_keys:
            c[key] = naver_map.get(key)
        pprint.pprint(c.keys())

    @unittest.skip("later")
    def test_aggr_daily_new_summary(self):
        target_date = datetime(2021, 12, 3, 7, 59, 59)
        ret, dt = self.newsSummary.aggrDailyNewSummary(the_date=target_date)
        # pprint.pprint(dt)

    def test_repeat_mapping_prog_ids_for_news_in_daily_news_summary(self):
        """
        기존 hourly-news-summary에서 porg_id가 맵핑된 정보를 가져와서
        신규 index인 origin-daily-news-summary-2021.12.03에 정보 upsert
        """
        start_time = time.time()
        the_date = datetime(2021, 10, 22)
        # end_time = datetime(2021, 10, 23)
        end_time = datetime.now()
        while the_date <end_time:
            index_name = 'origin-daily-news-summary-2021.12.08'
            flag = self.mapping_prog_ids_for_news_in_daily_news_summary(index_name=index_name,
                                                                  the_date=the_date)
            if flag == False:
                break

            the_date = the_date + timedelta(days=1)
        if flag:
            self.logger.info(f"Job was done : {time.time()-start_time}")
        else:
            self.logger.warning(f"migration failed...{the_date}")



    def mapping_prog_ids_for_news_in_daily_news_summary(self, index_name:str, the_date:datetime):
        # the_date = datetime(2021, 12, 2)
        # data = self.jtbc_data.mapping_prog_id_for_news_id_in_hourly_news_summary(the_date=the_date)
        if index_name == None:
            index_name = 'origin-daily-news-summary-2021.12.08'
        try:

            mapping_news_ids = self.jtbcdata.collect_prog_id_for_news_id_in_hourly_summary_index(the_date=the_date)
            self.logger.info(f"broadcast news_ids: {mapping_news_ids.keys()}")

            date_range = {
                "gte": the_date.strftime('%Y-%m-%dT00:00:00+09:00'),
                "lte": the_date.strftime('%Y-%m-%dT23:59:59+09:00')
            }
            body = {
                "size": 100,
                "query": {
                    "range": {
                        "reg_date": date_range
                    }
                },
                "sort": [
                    {
                        "reg_date": {
                            "order": "asc"
                        }
                    }
                ]
            }
            # body2 = {
            # 	"query": {
            # 		"bool": {
            # 			"must": [
            # 				{
            # 					"term": {
            # 						"news_id": {
            # 							"value": "NB12035813"
            # 						}
            # 					}
            # 				}
            # 			],
            # 			"filter": [
            # 				{
            # 					"range": {
            # 						"reg_date": date_range
            # 					}
            # 				}
            # 			]
            # 		}
            # 	}
            # }
            # # access search context for 5 seconds before moving on to the next step,
            scroll = '30s'
            response = self.jtbcdata.es_client.search(
                index=index_name,
                body=body,
                scroll=scroll
            )

            scroll_id = response['_scroll_id']
            scroll_size = response['hits']['total']['value']

            while scroll_size > 0:
                now_time = datetime.now().strftime('%Y-%m-%dT%H:%M:%S+09:00')
                # new_mapping_jtbc_map = {}
                self.logger.info(f"<<< THE_DATE = {the_date.strftime('%Y-%m-%dT%H:%M:%S+09:00')} >>>")
                self.logger.info(f"scroll_size: {scroll_size}")
                # # 해당 시간동안 수집한 news_id(list)
                # collected_news_id = [x['_source']['news_id'] for x in response['hits']['hits']]
                # unique_colleted_news_id = list(set(collected_news_id))
                self.logger.info(f"the_date = {the_date.strftime('%Y-%m-%dT%H:%M:%S+09:00')}")
                # self.logger.info(f'scroll unique news_id : {unique_colleted_news_id}')

                # # news_id에 대해서 JTBC 방송프로그램에 등록된 news_id 찾기
                # mapping_prog_ids = self.jtbcdata.collect_jtbc_tv_prog_id_by_news_ids(news_ids=unique_colleted_news_id,
                # 																	  the_date=the_date)
                # unique_prog_ids = list(set([a_data.get('prog_id') for a_data in mapping_prog_ids]))
                # self.logger.info(f"news_id's unique_prog_ids: {unique_prog_ids}")
                # if unique_prog_ids:
                # 	unique_prog_ids_info = self.tvpro.get_nw_tv_program_by_prog_ids(prog_ids=unique_prog_ids)
                # 	new_mapping_jtbc_map = self.tvpro.update_prog_id_for_news_ids(mapping_news_ids=mapping_prog_ids,
                # 																  prod_ids_info=unique_prog_ids_info)
                #
                # self.logger.info(f"mapping news_ids for prog_id: {list(set(new_mapping_jtbc_map.keys()))}")

                # for doc in response['hits']['hits']:
                # 	# do something with retrieved documents with current scroll
                # 	doc['_source']['news_id']
                filtered_news_ids = list(filter(lambda x: x['_source']['news_id'] in mapping_news_ids.keys(),
                                                response['hits']['hits']))
                for doc in filtered_news_ids:
                    # 추가 메시지 update
                    self.jtbcdata.es_client.update(
                        index=index_name,
                        id=doc['_id'],
                        body={
                            "doc":
                                {
                                    **mapping_news_ids.get(doc['_source']['news_id']),
                                    "update_date": now_time
                                }

                        }
                    )
                self.logger.info(
                    f"mapping news_ids for prog_id : {len(filtered_news_ids)}/{len(response['hits']['hits'])}")

                response = self.jtbcdata.es_client.scroll(scroll_id=scroll_id, scroll=scroll)

                scroll_id = response['_scroll_id']
                scroll_size = len(response['hits']['hits'])
                self.logger.info('\n\n\n')

        except Exception as es:
            self.logger.error(f"ERROR: {es}")
            return False
        return True

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
