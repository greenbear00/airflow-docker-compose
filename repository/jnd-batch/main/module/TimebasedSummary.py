from base import constVar
from main.aggregator.JTBCData import JTBCData
from main.aggregator.KakaoData import KakaoData
from main.aggregator.NaverData import NaverData
from main.module.DetailNewsInfo import DetailNewsInfo
from util.helper import Helper
from datetime import datetime
from elasticsearch.helpers import bulk
import os
from util.JtbcLogger import JtbcLogger
from schema.SchemaGenerator import SchemaGenerator
from main.aggregator.ElasticGenerator import ElasticGenerator
from pathlib import Path

class TimebasedSummary(ElasticGenerator):
    def __init__(self, helper:Helper, level:str='dev'):
        self.sg = SchemaGenerator(obj=self, level=level)

        self.es_client = helper.get_es()
        self.jtbcData = JTBCData(helper)
        self.naverData = NaverData(helper)
        self.kakaoData = KakaoData(helper)
        self.detailNewsInfo = DetailNewsInfo(helper)

        self.level = level

        if os.path.isdir('/box/jnd-batch'):
            # prod
            path = '/box/jnd-batch'
        else:
            # dev
            path = Path(__file__).parent.parent.parent

        logger_factory = JtbcLogger(path=os.path.join(path, "logs"), file_name=type(self).__name__)
        self.logger = logger_factory.logger

        super().__init__(path=path, es_client=self.es_client)



    def aggrHourlyNewsSummary(self, the_date: datetime = None):

        elk_data = {}
        jtbc_view_total = naver_view_total = kakao_view_total = 0
        jtbc_reaction_total = naver_reaction_total = kakao_reaction_total = 0
        jtbc_comment_total = naver_comment_total = kakao_comment_total = 0

        if the_date is None:
            raise ValueError(f'the_data is None')
        try:
            self.logger.info(f"the_date = {the_date}")

            # 이전 1시간 기준으로 JTBC CMS에서 기사 생산건수 가져오기
            # create_news_result
            #       NEWS_ID                                  TITLE NEWS_SECTION SECTION_NAME SOURCE_CODE SOURCE_NAME    SERVICE_DT                    REPORTER_INFO                  MOD_DT
            # 0  NB12027418  남욱 "대장동 사업비용만 600억 써…돈 준 내역 있다"        30           사회           11          JTBC        202110190758  363,박병현,40,내셔널부                 NaT
            # 1  NB12027421  남욱 "'그분' 이재명은 아냐"…기획 입국 의혹 제기도         30           사회           11          JTBC        202110190755  110,정종문,4,사회부                   2021-10-19 07:57:03.453
            # 2  NB12027422  '이재명 국감' 대장동 충돌…"초과이익환수, 지침 따른 것"      10           정치           11          JTBC        202110190752  461,고승혁,1,정치부                   NaT
            produce_result = self.detailNewsInfo.get_vi_elk_news_basic_by_service_dt(end_time=the_date)

            jtbc_ret, jtbc_map = self.jtbcData.getDataListHourly(the_date=the_date)
            naver_ret, naver_map = self.naverData.getDataListHourly(the_date=the_date)
            kakao_ret, kakao_map = self.kakaoData.getDataListHourly(the_date=the_date)


            # jtbc 조회수, 반응수, 댓글수
            if jtbc_map:
                jtbc_pc_view = sum(v.get('pc_view') for k, v in jtbc_map.items())
                jtbc_mobile_view = sum(v.get('mobile_view') for k, v in jtbc_map.items())
                jtbc_app_view = sum(v.get('app_view') for k, v in jtbc_map.items())
                jtbc_view_total = jtbc_pc_view+jtbc_mobile_view+jtbc_app_view
                jtbc_reaction_total = sum(v.get('reaction') for k, v in jtbc_map.items())
                jtbc_comment_total = sum(v.get('comment') for k, v in jtbc_map.items())
                # jtbc_create_news = [ {'news_id': new_news['NEWS_ID'], 'news_name': new_news['TITLE']} for new_news in produce_result]
                # jtbc_create_news = [ {'news_id': new_news['NEWS_ID'], 'news_name': new_news['TITLE']} for new_news in produce_result]
                jtbc_produce = [{'news_id': new_news['NEWS_ID']} for new_news in produce_result if new_news.get('SOURCE_CODE') == '11']
                jam_produce = [{'news_id': new_news['NEWS_ID']} for new_news in produce_result if new_news.get('SOURCE_CODE') == '17']

                elk_data['jtbc'] = {
                    'jtbc_produce': len(jtbc_produce),
                    'jam_produce': len(jam_produce),
                    'produce': len(produce_result),

                    'jtbc_produce_news':jtbc_produce,
                    'jam_produce_news':jam_produce,

                    'pc_view': jtbc_pc_view,
                    'mobile_view': jtbc_mobile_view,
                    'app_view': jtbc_app_view,

                    'view_total':jtbc_view_total,
                    'reaction_total':jtbc_reaction_total,
                    'comment_total': jtbc_comment_total,

                    # 'news':[ dict(v, **{'news_id':k}) for k, v in jtbc_map.items()]
                    'news': [ {'news_id':k} for k, v in jtbc_map.items()]
                }
            # naver 조회수, 반응수, 댓글수
            if naver_map:
                # dev망에서 일부 기사에 대해서 pc_view: None인 경우가 존재
                naver_data, naver_view_total, naver_reaction_total, naver_comment_total  = \
                    self._make_open_platform_data(map_data=naver_map)
                elk_data['naver'] = naver_data
            # kakao 조회수, 반응수, 댓글수
            if kakao_map:
                kakao_data, kakao_view_total, kakao_reaction_total, kakao_comment_total = \
                    self._make_open_platform_data(map_data=kakao_map)
                elk_data['kakao'] = kakao_data


            # 조회수 합계(jtbc+naver+kakao)
            elk_data['view_total'] = jtbc_view_total + naver_view_total +kakao_view_total
            # 반응수 합계(jtbc+naver+kakao)
            elk_data['reaction_total'] = jtbc_reaction_total + naver_reaction_total + kakao_reaction_total
            # 댓글 수 합계(jtbc+naver+kakao)
            elk_data['comment_total'] = jtbc_comment_total + naver_comment_total + kakao_comment_total

            reg_date = the_date.replace(minute=0, second=0, microsecond=0).strftime(
                '%Y-%m-%dT%H:00:00+09:00')
            elk_data['reg_date'] = reg_date

            # _id: NCBH_{reg_date} (NewsCreatedByHourly)
            elk_doc = [
                {
                    '_op_type': "update",
                    '_id' : the_date.strftime('%Y%m%dT%H'),
                    "doc_as_upsert": True,
                    "doc": elk_data
                }
            ]

            # index_name이 앞에 'origin'이 붙고, 뒤에 YYYY.MM이 붙어져서 나옴
            #   예) origin-daily-summary-2021.10
            template_name, template, index_name, alias_name, aliases = \
                self.sg.get_template(the_date=the_date, template_name='hourly-summary-template',
                                     index_name=constVar.HOURLY_SUMMARY)
            self.make_index_and_template(index_name=index_name,
                                          template_name=template_name,
                                          template=template,
                                          alias_name=alias_name,
                                          aliases=aliases)
            bulk(self.es_client, elk_doc, index=index_name, refresh="wait_for")
            self.logger.info(f"upsert to {index_name} index by reg_date({reg_date})")

        except Exception:
            # self.logger.error(f"Error: {traceback.format_exc()}")
            raise


    def aggrDailyNewSummary(self, the_date: datetime = None):
        elk_data = {}
        jtbc_view_total = naver_view_total = kakao_view_total = 0
        jtbc_reaction_total = naver_reaction_total = kakao_reaction_total = 0
        jtbc_comment_total = naver_comment_total = kakao_comment_total = 0

        if the_date is None:
            raise ValueError(f'the_data is None')
        try:
            self.logger.info(f"the_date = {the_date}")

            # 이전 1시간 기준으로 JTBC CMS에서 기사 생산건수 가져오기
            # create_news_result
            #       NEWS_ID                                  TITLE NEWS_SECTION SECTION_NAME SOURCE_CODE SOURCE_NAME    SERVICE_DT                    REPORTER_INFO                  MOD_DT
            # 0  NB12027418  남욱 "대장동 사업비용만 600억 써…돈 준 내역 있다"        30           사회           11          JTBC        202110190758  363,박병현,40,내셔널부                 NaT
            # 1  NB12027421  남욱 "'그분' 이재명은 아냐"…기획 입국 의혹 제기도         30           사회           11          JTBC        202110190755  110,정종문,4,사회부                   2021-10-19 07:57:03.453
            # 2  NB12027422  '이재명 국감' 대장동 충돌…"초과이익환수, 지침 따른 것"      10           정치           11          JTBC        202110190752  461,고승혁,1,정치부                   NaT
            produce_result = self.detailNewsInfo.get_vi_elk_news_basic_by_service_dt(end_time=the_date, period='D')

            jtbc_ret, jtbc_map = self.jtbcData.getDataListDaily(the_date=the_date)
            naver_ret, naver_map = self.naverData.getDataListDaily(the_date=the_date)
            kakao_ret, kakao_map = self.kakaoData.getDataListDaily(the_date=the_date)

            # jtbc 조회수, 반응수, 댓글수
            if jtbc_map:
                jtbc_pc_view = sum(v.get('pc_view') for k, v in jtbc_map.items())
                jtbc_mobile_view = sum(v.get('mobile_view') for k, v in jtbc_map.items())
                jtbc_app_view = sum(v.get('app_view') for k, v in jtbc_map.items())
                jtbc_view_total = jtbc_pc_view + jtbc_mobile_view + jtbc_app_view
                jtbc_reaction_total = sum(v.get('reaction') for k, v in jtbc_map.items())
                jtbc_comment_total = sum(v.get('comment') for k, v in jtbc_map.items())
                # jtbc_create_news = [{'news_id': new_news['NEWS_ID'], 'news_name': new_news['TITLE']} for new_news in
                #                     produce_result]
                jtbc_produce = [{'news_id': new_news['NEWS_ID']} for new_news in produce_result if
                                new_news.get('SOURCE_CODE') == '11']
                jam_produce = [{'news_id': new_news['NEWS_ID']} for new_news in produce_result if
                               new_news.get('SOURCE_CODE') == '17']

                elk_data['jtbc'] = {
                    'jtbc_produce': len(jtbc_produce),
                    'jam_produce': len(jam_produce),
                    'produce': len(produce_result),

                    'jtbc_produce_news':jtbc_produce,
                    'jam_produce_news':jam_produce,

                    'pc_view': jtbc_pc_view,
                    'mobile_view': jtbc_mobile_view,
                    'app_view': jtbc_app_view,

                    'view_total': jtbc_view_total,
                    'reaction_total': jtbc_reaction_total,
                    'comment_total': jtbc_comment_total,
                    # 'news': [dict(v, **{'news_id': k}) for k, v in jtbc_map.items()]
                    'news': [{'news_id': k} for k, v in jtbc_map.items()]
                }

            # naver 조회수, 반응수, 댓글수
            if naver_map:
                # dev망에서 일부 기사에 대해서 pc_view: None인 경우가 존재
                naver_data, naver_view_total, naver_reaction_total, naver_comment_total = \
                    self._make_open_platform_data(map_data=naver_map)
                elk_data['naver'] = naver_data
            # kakao 조회수, 반응수, 댓글수
            if kakao_map:
                kakao_data, kakao_view_total, kakao_reaction_total, kakao_comment_total = \
                    self._make_open_platform_data(map_data=kakao_map)
                elk_data['kakao'] = kakao_data

            # 조회수 합계(jtbc+naver+kakao)
            elk_data['view_total'] = jtbc_view_total + naver_view_total + kakao_view_total
            # 반응수 합계(jtbc+naver+kakao)
            elk_data['reaction_total'] = jtbc_reaction_total + naver_reaction_total + kakao_reaction_total
            # 댓글 수 합계(jtbc+naver+kakao)
            elk_data['comment_total'] = jtbc_comment_total + naver_comment_total + kakao_comment_total

            # 집계기준은 당일에 대한 데이터는 00:00:00+09:00으로 설정함
            reg_date = the_date.replace(hour=0, minute=0, second=0, microsecond=0).strftime('%Y-%m-%dT00:00:00+09:00')
            elk_data['reg_date'] = reg_date
            elk_data['update_date'] = datetime.now().strftime('%Y-%m-%dT%H:%m:%d+09:00')

            elk_doc = [
                {
                    '_op_type': "update",
                    '_id': the_date.strftime('%Y%m%d'),
                    "doc_as_upsert": True,
                    "doc": elk_data
                }
            ]

            # index_name이 앞에 'origin'이 붙고, 뒤에 YYYY이 붙어져서 나옴
            #   예) origin-daily-summary-2021
            template_name, template, index_name, alias_name, aliases = \
                self.sg.get_template(the_date=the_date, template_name='daily-summary-template',
                                     index_name=constVar.DAILY_SUMMARY)
            self.make_index_and_template(index_name=index_name,
                                          template_name=template_name,
                                          template=template,
                                          alias_name=alias_name,
                                          aliases=aliases)
            bulk(self.es_client, elk_doc, index=index_name, refresh="wait_for")
            self.logger.info(f"upsert to {index_name} index by reg_date({reg_date})")

        except Exception:
            raise



    def _make_open_platform_data(self, map_data:dict)-> (dict, int, int, int):
        """
        hourly-summary-stat 중 kakao와 naver에 해당하는 dictionary 를 생성
        :param map_data:
        :return:
            (dict, int, int,int)
        """
        try:
            pc_view = sum([v.get('pc_view') for k, v in map_data.items() if isinstance(v.get('pc_view'), int)])
            mobile_view = sum(
                [v.get('mobile_view') for k, v in map_data.items() if isinstance(v.get('mobile_view'), int)])
            view_total = sum([v.get('view') for k, v in map_data.items() if isinstance(v.get('view'), int)])
            reaction_total = sum(
                [v.get('reaction') for k, v in map_data.items() if isinstance(v.get('reaction'), int)])
            comment_total = sum(
                [v.get('comment') for k, v in map_data.items() if isinstance(v.get('comment'), int)])
            data = {
                f'pc_view': pc_view,
                f'mobile_view': mobile_view,

                f'view_total': view_total,
                f'reaction_total': reaction_total,
                f'comment_total': comment_total,
                # 'news': [dict(v, **{'news_id': k}) for k, v in map_data.items()]
                'news': [{'news_id': k} for k, v in map_data.items()]
            }
        except Exception:
            raise

        view_total = view_total if view_total else 0
        reaction_total = reaction_total if reaction_total else 0
        comment_total = comment_total if comment_total else 0

        return data, view_total, reaction_total, comment_total





