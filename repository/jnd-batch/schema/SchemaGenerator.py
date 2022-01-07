from util.JtbcLogger import JtbcLogger
import json
import os
import traceback
from datetime import datetime
from pathlib import Path
from base.constVar import *


class SchemaGenerator:
    ###################################################################################################
    #   index 관리 체계 (참고: alias 맵핑은 우선 수동으로 하기)
    #   - index 생성 규칙          : origin-hourly-summary-YYYY.MM (예: original-hourly-summary-2021.05)
    #   - alias 생성 규칙          : hourly-summary-YYYY.MM
    #   - index template 생성 규칙 : hourly-summary-template
    #                              (내부에 original-hourly-summary-*로 걸기)
    #   - (kibana) index pattern : hourly-summary-*
    #
    #  - 만약, doc 추가 작업하려면 hourly-summary-YYYY.MM.dd 이렇게 하기로...
    ##################################################################################################
    def __init__(self, obj:object, level:str='dev'):
        self._level = level
        self._templates = None

        if os.path.isdir('/box/jnd-batch'):
            # prod
            path = '/box/jnd-batch'
        else:
            # dev
            path = Path(__file__).parent.parent

        logger_factory = JtbcLogger(path=os.path.join(path, *["logs"]), file_name=type(self).__name__)
        self.logger = logger_factory.logger

        if obj is None:
            self.logger.warning(ValueError('obj is None'))
            raise ValueError('obj is None')

        self.logger.info(f"loading index_patterns from {obj.__class__.__name__}")

        self._obj = obj
        self._shard = ELASTIC_DEV_SHARD if self._level == 'dev' else ELASTIC_PROD_SHARD

        self._schema_path = os.path.join(path, *['schema_json', f"{self._obj.__class__.__name__}"])

        self._templates = None

        self._load_templates()
        if self.templates is None:
            self.logger.info(f"{obj.__class__.__name__} does not have index_patterns.")
        else:
            self.logger.info(f"loaded {obj.__class__.__name__}'s index_patterns")


    @property
    def templates(self):
        # 실제 등록된 templates의 key name을 리턴
        return self._templates

    def get_templates_name(self) -> str:
        return list(self.templates.keys()) if self.templates else None

    def get_template(self, the_date:datetime, template_name:str, index_name:str=None) -> (str, dict, str, str, dict):
        """
        template_name으로 생성할 index_pattern의 파일명(확장자 제외)을 넣으면,
        해당 index_pattern에 따르는 template_name과 index_pattern, 그리고 index명이 리턴됨

        주의) index명 규칙은 origin-{지표명}-YYYY.MM으로 정함
             참고: https://pri-confluence.joongang.co.kr/pages/viewpage.action?pageId=27549616
        :param the_date: datetime (index의 YYYY.MM을 생성하기 위해 필요한 인자)
        :param template_name: hourly-summary-template (json의 파일 명으로 확장자 제외)
        :param index_name: 'origin'이 붙어 있는 index_name이 있을 경우, 이를 통해 index_name을 생성
        :return:
            template_name(str), template(json), index_name(str), alias_name(str), aliases(dict)
        """
        template = {}
        if index_name:
            index_name = f"{index_name}-{the_date.strftime('%Y')}" if 'origin-' in index_name else f"origin-{index_name}-{the_date.strftime('%Y')}"
        elif template_name:
            index_name = f"origin-{template_name.replace('-template', '')}-{the_date.strftime('%Y')}"
        else:
            raise ValueError("index_name or template_name is none")

        if template_name:
            template = self.templates.get(template_name) if self.templates else None

        # setting aliases
        aliase = {
                    index_name : {
                        "aliases": {
                            f"{template_name.replace('-template', '')}-{the_date.strftime('%Y')}" : {
                                "is_write_index": True
                            }
                        }
                    }
        }
        alias_name = f"{template_name.replace('-template', '')}-{the_date.strftime('%Y')}"

        return template_name, template, index_name, alias_name, aliase


    def _load_templates(self)->None:
        """
        Class에 맞춰서 templates를 load하고, 이때 BUILD_LEVEL에 맞춰서
            - index_patterns의 template/settings/number_of_shards를 수정
        :return:
        """
        import glob
        from pathlib import Path

        templates = {}

        json_files = glob.glob(f"{self._schema_path}/*.json")
        try:
            for file in json_files:
                with open(file) as json_file:
                    self.logger.info(f"load index_pattern file path = {file}")
                    json_data = json.load(json_file)
                    filename = Path(file).stem

                    self.logger.info(f"load index_pattern : {filename} ")


                    template = json_data.get('template')
                    if template:

                        # update settings
                        settings = template.get('settings')
                        settings.update({'number_of_shards': self._shard})
                        template.update({'settings':settings})
                        self.logger.info(f"\tupdate {settings} in template.settings")

                    json_data.update({'template':template})

                templates.update({filename: json_data})
        except Exception:
            # self.logger.error(f"Error: {traceback.format_exc()}")
            raise

        self._templates = templates









if __name__ == "__main__":
    from util.helper import Helper

    ########################################################
    # for test
    ########################################################
    my_helper = Helper()

    logger = JtbcLogger().logger

    from main.module.TVProInfo import TVProInfo
    tv_pro = TVProInfo(helper=my_helper, level=my_helper.build_level)
    logger.info("\n"*2)

    logger.info("="*100)
    sg = SchemaGenerator(obj=tv_pro, level=my_helper.build_level)
    # template_name2, template2, index_name22, alias_name22, _ = sg.get_template(the_date=datetime.now(),
    #                                                              template_name='horly-summary-template',
    #                                                              index_name=HOURLY_SUMMARY)
    # logger.info(f"index_template name = {template_name2}, index_name={index_name22}, alias_name={alias_name22}")

    template_name, template, index_name, alias_name, _ = sg.get_template(the_date=datetime.now(),
                                                             template_name='mapping-tv-news-program-template')
    logger.info(f"index_template name = {template_name}, index_name={index_name}, alias_name={alias_name}")
    # logger.info(template)


    # sg2 = SchemaGenerator(obj=None, level=BUILD_LEVEL)
