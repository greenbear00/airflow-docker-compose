
from abc import *
import os
from util.JtbcLogger import JtbcLogger
from util.helper import Parser

from elasticsearch.helpers import bulk



class ElasticGenerator(metaclass=ABCMeta):
    def __init__(self, path, es_client):
        logger_factory = JtbcLogger(path=os.path.join(path, "logs"), file_name=type(self).__name__)
        self.logger = logger_factory.logger
        self.es_client = es_client
        self.p = Parser()

    def make_index_and_template(self, index_name:str, template_name:str, template:dict,
                                alias_name:str, aliases:dict):
        """

        :param index_name:  실제 생성시킬 index 명
        :param template_name: 실제 index에 매핑될 index_pattern의 이름
        :param template: 생성시킬 index_pattern에 대한 json
        :param alias_name:
        :param aliases:
        :return:
        """
        try:
            if not self.es_client.indices.exists(index=index_name):
                # index_template와 index 생성

                self._create_index_pattern(template_name=template_name, template=template)
                self.logger.info(f"create index: {index_name}")
                self.es_client.indices.create(index=index_name)

                # alias 생성
                # (index 최초 생성시킬때 index_pattern과 index, alias를 최초로 한번 생성시킨다.)
                # 이유는 년(YYYY)이 넘어가면서 index를 새로 생길때, 같이 alias도 생긴다.
                self.create_alias(index_name=index_name, alias_name=alias_name)

            else:
                # index_template 생성
                self.logger.info(f"create index_pattern: {template_name}")
                self._create_index_pattern(template_name=template_name, template=template)

            # # alias 생성
            # self.create_alias(index_name=index_name, alias_name=alias_name)
        except Exception:
            raise

    def delete_index_template(self, index_template_name:str):
        try:
            if self.es_client.indices.exists_index_template(index_template_name):
                self.es_client.indices.delete_index_template(index_template_name)
                self.logger.info(f"deleted index template : {index_template_name}")
        except Exception:
            raise

    def delete_index(self, index_name):
        try:
            if self.es_client.indices.exists(index=index_name):
                self.es_client.indices.delete(index=index_name)
                self.logger.info(f"deleted index : {index_name}")
        except Exception:
            raise

    def delete_alias(self, index_name, alias_name):
        try:
            if self.es_client.indices.exists_alias(alias_name):
                actions = {
                    "actions": [
                        {
                            "remove": {
                                "index": index_name,
                                "alias": alias_name,
                            }
                        }
                    ]
                }
                self.es_client.indices.put_alias(index=index_name, name=alias_name,
                                                 body=actions)
                self.logger.info(f"remove alias {alias_name} from {index_name} index")
                self.logger.info(f"{alias_name} alias actions {actions}")
                return True
        except Exception:
            raise
        return False

    def create_alias(self, index_name:str, alias_name:str)->bool:
        # appended new alias
        self.logger.info(f"AUTO_APPLY_ALIAS= {self.p.auto_apply_alias}")
        try:

            if self.p.auto_apply_alias:
                if not self.es_client.indices.exists_alias(alias_name):
                    # self.es_client.indices.put_alias(index=index_name, name=new_alias, body={"is_write_index": True})
                    actions = {
                                "actions": [
                                     {
                                         "add": {
                                            "index": index_name,
                                            "alias": alias_name,
                                            "is_write_index": True
                                         }
                                     }
                                 ]
                             }
                    self.es_client.indices.put_alias(index=index_name, name=alias_name,
                                                     body=actions)
                    self.logger.info(f"append new alias {alias_name} to {index_name} index")
                    self.logger.info(f"{alias_name} alias actions {actions}")
                    return True
                else:
                    actions = {
                        "actions": [
                            {
                                "add": {
                                    "index": index_name,
                                    "alias": alias_name,
                                    "is_write_index": True
                                }
                            }
                        ]
                    }
                    self.es_client.indices.update_aliases(
                                                     body=actions)
                    self.logger.info(f"append new alias {alias_name} to {index_name} index")
                    self.logger.info(f"{alias_name} alias actions {actions}")
                    return True
        except Exception:
            raise

        return False

    def get_alias_name(self, index_name):
        """
        # TODO 나중에 re(regex) 추가
        :param index_name: origin-hourly-summary-2021
        :return:
            hourly-summary-2021
        """

        alias_name = index_name.replace('origin-', '') if 'origin-' in index_name else index_name

        return alias_name

    def get_indexnames_from_alias(self, alias_name:str):
        """
        get origin-hourly-summary-2021/_alias
        {
          "origin-hourly-summary-2021" : {
            "aliases" : {
              "hourly-summary-2021" : { }
            }
          }
        }

        :param alias_name:  hourly-summary-2021
        :return:
            [origin-hourly-summary-2021]
        """
        try:
            if self.es_client.indices.exists_alias(name=alias_name):
                return [index_name for index_name in list(self.es_client.indices.get_alias(name=alias_name).keys())]

            # {
            #   "origin-hourly-summary-2021" : {
            #     "aliases" : {
            #       "hourly-summary-2021" : { }
            #     }
            #   }
            # }
        except Exception:
            raise
        return []

    def _create_index_pattern(self, template_name, template):
        try:
            if template:
                if not self.es_client.indices.exists_index_template(template_name):
                    # index_template 생성
                    self.logger.info(f"create index_template: {template_name}")

                    # put_index_template에서 create옵션이 default로 False가 되어 있어야 자동으로 index_pattern update
                    self.es_client.indices.put_index_template(name=template_name, body=template)
                else:
                    self.logger.warning(f"index_pattern= {template_name} already exists.")
            else:
                self.logger.warning(f"{template_name}'s template is None (index_template was not created.)")
            # else:
            #     # appended new alias
            #     new_alias = index_name.replace('origin-', '')
            #     if not self.es_client.indices.exists_alias(new_alias):
            #
            #         self.es_client.indices.put_alias(index=index_name, name=new_alias)
            #         self.logger.info(f"append new alias {new_alias} to {index_name}")
            #         # ori_index_templates = self.es_client.indices.get_index_template(template_name)
            #         # # ori_index_template = ori_index_templates.get('index_templates')[0]
            #         # #
            #         # #
            #         # # # ori_template_name_def = ori_index_template.get('name')
            #         # #
            #         # # # index_template:
            #         # # #   - index_patterns
            #         # # #   - template
            #         # # #       - settings, mappings, aliases
            #         # # #   - composed_of
            #         # # ori_index_template_def = ori_index_template.get('index_template')
            #         # # # ori_def_index_patterns = ori_index_template_def.get('index_patterns')
            #         # # ori_def_index_template = ori_index_template_def.get('template')
            #         # # # ori_def_index_composed_of = ori_index_template_def.get('composed_of')
            #         # #
            #         # # # update aliases
            #         # # ori_aliases = ori_def_index_template.get('aliases')
            #         # # ori_aliases.update(aliases)
            #         # # ori_def_index_template.update({'aliases': ori_aliases})
            #         # # ori_index_template_def.update({'template': ori_def_index_template})
            #         # # ori_index_template_def.pop('composed_of')
            #         # # self.logger.info(f"update '{index_name.replace('origin-', '')}' aliases to {template_name}")
            #         # # self.es_client.indices.put_index_template(name=template_name, body=ori_index_template_def)
        except Exception:
            raise

    def do_elasitc_write(self, index_name:str, template_name:str, template:dict,
                         alias_name:str, aliases:dict, elk_doc:list):
        try:
            self.make_index_and_template(index_name=index_name,
                                         template_name=template_name,
                                         template=template,
                                         alias_name=alias_name,
                                         aliases=aliases)
            bulk(self.es_client, elk_doc, index=index_name, refresh="wait_for")
        except (ValueError, Exception):
            raise


