import requests
from requests.structures import CaseInsensitiveDict
from _datetime import datetime
from util.helper import Helper, Parser


class DailyDashboardObjectBackup:
	def __init__(self, helper: Helper, dashboard_list: list):
		self.es_client = helper.get_es()
		self.build_conf = Parser()
		self.helper = helper
		self._dashboard_ids = dashboard_list
		self._jsonbody = list()

	def __del__(self):
		pass

	@property
	def tag_ids(self):
		return self._tag_ids

	@property
	def visualization_ids(self):
		return self._visualization_ids

	@property
	def dashboard_ids(self):
		return self._dashboard_ids

	def tag_object(self):
		# backup tag object
		pass

	def jsonbody(self):
		return self._jsonbody

	def search_object(self, object_type: str, condition: dict = None):
		try:
			body = {
				"query": {
					"match": {
						"type": object_type
					}
				}
			}
			if condition is not None:
				body.update(condition)

			response = self.es_client.search(
				index=".kibana",
				body=body
			)
			return response
		except Exception:
			raise

	# noinspection PyMethodMayBeStatic
	def get_dashboard_object(self):
		try:
			headers = CaseInsensitiveDict()
			headers["Content-Type"] = "application/json"
			headers["kbn-xsrf"] = 'true'

			for item in self.dashboard_ids:
				data = {
					"objects": [
						{
							"type": "dashboard",
							"id": item['id']
						}
					],
					"includeReferencesDeep": True
				}

				url = self.helper.config['kibana-api']['url']
				request_url = f"{url}/api/saved_objects/_export"
				kibana_username = self.helper.config['kibana-api']['username']
				kibana_password = self.helper.config['kibana-api']['password']

				resp = requests.post(request_url, headers=headers, json=data, auth=(kibana_username, kibana_password))
				self.write_to_ndjson(file_name=item['name'], text=resp.text)
		except Exception:
			raise

		'''
		visualization_body = {
			"attributes": {
				"description": "#JTBC, #News, #KH",
				"kibanaSavedObjectMeta": {
					"searchSourceJSON": "{\"query\":{\"query\":\"\",\"language\":\"kuery\"},\"filter\":[]}"
				},
				"title": "[NEWS]JTBC_조회수&비율",
				"uiStateJSON": "{}",
				"version": 1,
				"visState": "{\"title\":\"[NEWS]JTBC_조회수&비율\",\"type\":\"metrics\",\"aggs\":[],\"params\":{\"time_range_mode\":\"entire_time_range\",\"id\":\"61ca57f0-469d-11e7-af02-69e470af7417\",\"type\":\"markdown\",\"series\":[{\"id\":\"61ca57f1-469d-11e7-af02-69e470af7417\",\"color\":\"#68BC00\",\"split_mode\":\"everything\",\"palette\":{\"type\":\"palette\",\"name\":\"default\"},\"metrics\":[{\"id\":\"61ca57f2-469d-11e7-af02-69e470af7417\",\"type\":\"sum\",\"field\":\"jtbc.view\"}],\"separate_axis\":0,\"axis_position\":\"right\",\"formatter\":\"number\",\"chart_type\":\"line\",\"line_width\":1,\"point_size\":1,\"fill\":0.5,\"stacked\":\"none\",\"label\":\"JTBC_VIEW\",\"var_name\":\"\"},{\"id\":\"86a972f0-1cd0-11ec-997b-d7235a156630\",\"color\":\"#68BC00\",\"split_mode\":\"everything\",\"palette\":{\"type\":\"palette\",\"name\":\"default\"},\"metrics\":[{\"id\":\"86a972f1-1cd0-11ec-997b-d7235a156630\",\"type\":\"sum\",\"field\":\"jtbc.view\"},{\"id\":\"9fa5a0d0-1cd0-11ec-997b-d7235a156630\",\"type\":\"sum\",\"field\":\"view_total\"},{\"id\":\"c84eb760-1cd0-11ec-997b-d7235a156630\",\"type\":\"calculation\",\"variables\":[{\"id\":\"cdbb9240-1cd0-11ec-997b-d7235a156630\",\"name\":\"A\",\"field\":\"86a972f1-1cd0-11ec-997b-d7235a156630\"},{\"id\":\"d0679f70-1cd0-11ec-997b-d7235a156630\",\"name\":\"B\",\"field\":\"9fa5a0d0-1cd0-11ec-997b-d7235a156630\"}],\"script\":\"params.A / params.B\"}],\"separate_axis\":0,\"axis_position\":\"right\",\"formatter\":\"0.0%\",\"chart_type\":\"line\",\"line_width\":1,\"point_size\":1,\"fill\":0.5,\"stacked\":\"none\",\"label\":\"JTBC_RATE\",\"var_name\":\"\",\"value_template\":\"\"}],\"time_field\":\"\",\"use_kibana_indexes\":true,\"interval\":\"\",\"axis_position\":\"left\",\"axis_formatter\":\"number\",\"axis_scale\":\"normal\",\"show_legend\":1,\"show_grid\":1,\"tooltip_mode\":\"show_all\",\"background_color_rules\":[{\"id\":\"cd99f5f0-1ccf-11ec-997b-d7235a156630\"}],\"gauge_color_rules\":[{\"id\":\"ce9c4ac0-1ccf-11ec-997b-d7235a156630\"}],\"gauge_width\":10,\"gauge_inner_width\":10,\"gauge_style\":\"half\",\"isModelInvalid\":false,\"bar_color_rules\":[{\"id\":\"de81cfa0-1ccf-11ec-997b-d7235a156630\"}],\"background_color\":\"rgba(25,113,244,0.25)\",\"markdown\":\"**{{ jtbc_view.last.formatted }}**\\n({{ jtbc_rate.last.formatted }})\\n\\\\\\nJTBC\",\"markdown_less\":\"text-align:center;\\nFont-size:35px;\\ncolor:black;\",\"markdown_css\":\"#markdown-61ca57f0-469d-11e7-af02-69e470af7417{text-align:center;Font-size:35px;color:black}\",\"markdown_vertical_align\":\"middle\",\"index_pattern_ref_name\":\"metrics_0_index_pattern\"}}"
			},
			"coreMigrationVersion": "7.13.2",
			"id": "2990b7b0-1cd3-11ec-aec0-4532b2a3a9aa",
			"migrationVersion": {
				"visualization": "7.13.1"
			},
			"references": [
				{
					"id": "988ca1f0-0479-11ec-b88b-1dd008f44c55",
					"name": "metrics_0_index_pattern",
					"type": "index_pattern"
				},
				{
					"id": "ee51ac50-27d5-11ec-b2d9-6fb9c12d3e6d",
					"name": "tag-ee51ac50-27d5-11ec-b2d9-6fb9c12d3e6d",
					"type": "tag"
				}
			],
			"type": "visualization",
			"updated_at": "2021-12-01T02:06:37.374Z",
			"version": "WzM4MTUyNywxMF0="
		}
		'''

	def get_tag_object(self):
		# first: tag 정보를 먼저 백업한다.
		tag_repsonse = self.search_object('tag')
		body_list = list()
		for item in tag_repsonse['hits']['hits']:
			_source = item['_source']
			body = {
				"attributes": _source['tag'],
				"coreMigrationVersion": _source['coreMigrationVersion'],
				"id": str(item["_id"]).replace("tag:", ""),
				"references": [],
				"type": "tag",
				"updated_at": _source["updated_at"]
			}
			body_list.append(body)
		return body_list

	# noinspection PyMethodMayBeStatic
	def write_to_ndjson(self, file_name, text):
		# dev, prod별 디렉토리 지정(키바나 오브젝트는 당일날짜 백업)
		try:
			path = f"{self.build_conf.backup_location}/test"
			if self.build_conf.build_level != 'dev' or self.build_conf.build_level == 'prod':
				path = f"{self.build_conf.backup_location}/kibana/{datetime.today().strftime('%Y.%m.%d')}/{file_name}.ndjson"
			with open(path, 'w') as f:
				f.write(text)
		except Exception:
			raise

	# noinspection PyMethodMayBeStatic
	def write_to_local_ndjson(self, file_name, text):
		try:
			path = f'/Users/a200323007/jtbcrepo/jtbcnewsanalyze/test/{file_name}.ndjson'
			with open(path, 'w') as f:
				f.write(text)
		except Exception:
			raise

