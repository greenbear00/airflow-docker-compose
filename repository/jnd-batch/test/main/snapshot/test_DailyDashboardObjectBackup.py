from unittest import TestCase
import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from main.snapshot.DailyDashboardObjectBackup import DailyDashboardObjectBackup
from util.helper import Helper
from datetime import datetime
from util.JtbcLogger import JtbcLogger
from pathlib import Path
import os


class TestDailyDashboardObjectBackup(TestCase):

	def __init__(self, *args, **kwargs):
		path = Path(__file__).parent.parent.parent.parent
		logger_factory = JtbcLogger(path=os.path.join(path, "logs"), file_name=self.__class__.__name__)
		logger = logger_factory.logger

		helper = Helper()
		self.helper = helper

		# 백업대상 대시보드 ID 지정
		dashboard_list = list()
		dashboard_list.append({"name": "[기획팀] JTBC 플랫폼 콘텐트 사용 통계", "id": "6a842460-37c6-11ec-bad4-83d15d02a802"})
		dashboard_list.append({"name": "[News]실시간 현황", "id": "73decde0-04c6-11ec-b88b-1dd008f44c55"})
		dashboard_list.append({"name": "[News]실시간 현황(모바일)", "id": "a6590dc0-1ff7-11ec-aec0-4532b2a3a9aa"})
		dashboard_list.append({"name": "[NEWS]플랫폼별 현황", "id": "a5b078e0-41fd-11ec-bad4-83d15d02a802"})
		dashboard_list.append({"name": "[News]기사조회수(Top200)", "id": "a16cf430-3e0a-11ec-bad4-83d15d02a802"})
		dashboard_list.append({"name": "[News] 부서별 생산수&조회수", "id": "4e332e60-3793-11ec-bad4-83d15d02a802"})
		dashboard_list.append({"name": "[News] 기자별 생산수&조회수", "id": "0eb3ef50-4138-11ec-bad4-83d15d02a802"})
		dashboard_list.append({"name": "대선 특집 페이지 콘텐트 유입 성과", "id": "911110d0-3167-11ec-b2d9-6fb9c12d3e6d"})
		dashboard_list.append({"name": "[News]기사 상세", "id": "8d3ec6c0-27dd-11ec-b2d9-6fb9c12d3e6d"})
		dashboard_list.append({"name": "[기획팀] JTBC 뉴스 플랫폼 콘텐트 사용 통계", "id": "89083410-5663-11ec-848c-b371817548f1"})
		self.object_backup = DailyDashboardObjectBackup(helper=helper, dashboard_list=dashboard_list)

		super(TestDailyDashboardObjectBackup, self).__init__(*args, **kwargs)

	def test_backup_dashboard_object(self):
		self.object_backup.get_dashboard_object()

	def test_backup_tag_object(self):
		self.object_backup.get_tag_object()

	def test_json(self):
		header = dict()
		header['Content-Type'] = 'application/json'
		header['kbn-xsrf'] = True
		res = self.helper.get_es().transport.perform_request(
			method='POST',
			url='/api/saved_objects/_export',
			headers=header,
			body={
				"objects": [
					{
						"type": "dashboard",
						"id": "4e332e60-3793-11ec-bad4-83d15d02a802"
					}
				],
				"includeReferencesDeep": True
			}
		)
		print(res)

	def test_curl(self):
		import requests
		from requests.structures import CaseInsensitiveDict
		import json
		import jsonpath
		url = "https://jnd-dev.jtbc.co.kr:5601/api/saved_objects/_export"

		headers = CaseInsensitiveDict()
		headers["Content-Type"] = "application/json"
		headers["kbn-xsrf"] = 'true'
		data = {
				"objects": [
					{
						"type": "dashboard",
						"id": "4e332e60-3793-11ec-bad4-83d15d02a802"
					}
				],
				"includeReferencesDeep": True
		}

		resp = requests.post(url, headers=headers, json=data, auth=('elastic', 'ZS6DCqI5cjR1HyUnNIFM'))
		print(resp.text)
		response_json = json.loads(resp.text)
		name = jsonpath.jsonpath(response_json, 'name')
		print(name)

		print(resp.status_code)
		print(resp.content)


# https://github.com/pycurl/pycurl/blob/master/examples/quickstart/form_post.py
'''
curl -X POST https://jnd-dev.jtbc.co.kr:5601/api/saved_objects/_export \
-u elastic:ZS6DCqI5cjR1HyUnNIFM \
-H 'Content-Type: application/json' \
-H 'kbn-xsrf: true' \
-d '{
  "objects": [
	{
	  "type": "dashboard",
	  "id": "4e332e60-3793-11ec-bad4-83d15d02a802"
	}
  ],
  "includeReferencesDeep": true
}
'
		
'''