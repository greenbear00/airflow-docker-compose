import unittest
from unittest import TestCase, main
from main.module.TimebasedSummary import TimebasedSummary
from util.helper import Helper
from datetime import datetime
from base import constVar


class TestElasticGenerator(TestCase):

    def setUp(self) -> None:
        self.level = "prod"
        my_helper = Helper(self.level)
        self.timebased_summary = TimebasedSummary(helper=my_helper, level = self.level)

        self.the_date = datetime.now()
        template_name, template, self.index_name, self.alias_name, aliases = \
            self.timebased_summary.sg.get_template(the_date=self.the_date, template_name='hourly-summary-template',
                                                   index_name=constVar.HOURLY_SUMMARY)

    def test_hello(self):
        self.assertEqual("test", "test")


    def test_index(self):
        # delete test
        # get test/_search
        # get test/_mapping
        # get _cat/indices/*test*
        # get _alias/*test*

        index_name = "test-2021"
        index_body= {
          "settings": {
            "number_of_shards": 1
          },
          "mappings": {
            "properties" : {
              "comment_total" : {
                "type" : "long"
              },
              "view_total" : {
                "type" : "long"
              }
            }
          }
        }
        alias_name = "test"

        print(self.timebased_summary.es_client.indices.exists(index=index_name))
        if not self.timebased_summary.es_client.indices.exists(index=index_name):
            self.timebased_summary.es_client.indices.create(index=index_name, body=index_body)
            if not self.timebased_summary.es_client.indices.exists_alias(alias_name):
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
                self.timebased_summary.es_client.indices.put_alias(index=index_name, name=alias_name,
                                                 body=actions)


    @unittest.skip("later")
    def test_alias_remove(self):
        self.assertEqual(self.timebased_summary.delete_alias(index_name=self.index_name,
                                                             alias_name=self.index_name.replace('origin-',''))
                         , True,
                         "alias does not remove. (already removed alias)")

    def test_index_name(self):

        print(f"index_name = {self.index_name}")
        self.assertEqual(f"origin-hourly-summary-{self.the_date.strftime('%Y')}", self.index_name,
                         f"index_name: {self.index_name} != origin-hourly-summary-{self.the_date.strftime('%Y')}")


    def test_alias_from_index_name(self):
        alias_name = self.timebased_summary.get_alias_name(index_name=self.index_name)
        print(f"{self.index_name}'s alias_name = {alias_name}")
        self.assertEqual(f"hourly-summary-{self.the_date.strftime('%Y')}", alias_name,
                         f"alias_name: {alias_name} != hourly-summary-{self.the_date.strftime('%Y')}")

    def test_indexnames_from_alias(self):
        alias_name = self.timebased_summary.get_alias_name(index_name=self.index_name)
        indexnames = self.timebased_summary.get_indexnames_from_alias(alias_name=alias_name)
        print(f"index_names of {alias_name} alias = {indexnames}")
        self.assertEqual(["origin-hourly-summary-2021"], indexnames)


if __name__ == '__main__':
    TestElasticGenerator()