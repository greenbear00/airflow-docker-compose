from util.helper import Helper
from _datetime import datetime
from calendar import monthrange
from util.helper import Parser
import base.constVar
import os
from pathlib import Path
from util.JtbcLogger import JtbcLogger
import elasticsearch.exceptions

class MonthlyDashboardDataSnapshot:
	def __init__(self, helper: Helper, target_month: datetime):
		self.es_client = helper.get_es()
		self.build_conf = Parser()
		self._target_month = target_month
		self._reindex_list = list()

		self._repository_name = f"test_monthly_dashboard_{self.target_month.strftime('%Y.%m')}"
		self._repository_location = f"{self.build_conf.snap_shot_location}/test/{self.repository_name}"
		if self.build_conf.build_level != 'dev' or self.build_conf.build_level == 'prod':
			self._repository_name = f"monthly_dashboard_{self.target_month.strftime('%Y.%m')}"
			self._repository_location = f"{self.build_conf.snap_shot_location}/{self.repository_name}"

		if os.path.isdir('/box/jnd-batch'):
			# prod
			path = '/box/jnd-batch'
		else:
			# dev
			path = Path(__file__).parent.parent.parent

		logger_factory = JtbcLogger(path=os.path.join(path, "logs"), file_name=type(self).__name__)
		self.logger = logger_factory.logger

		self.es_client = helper.get_es()

	def __del__(self):
		pass

	@property
	def target_month(self):
		return self._target_month

	@property
	def repository_name(self):
		return self._repository_name

	@property
	def repository_location(self):
		return self._repository_location

	@property
	def reindex_list(self):
		return self._reindex_list

	def createRepository(self):
		isExists = True
		try:
			self.es_client.snapshot.verify_repository(
				repository=self.repository_name
			)
		except elasticsearch.exceptions.NotFoundError:
			isExists = False

		try:
			# To do made repository by the month
			if isExists is False:
				self.es_client.snapshot.create_repository(
					repository=self.repository_name,
					body={
						"type": "fs",
						"settings": {
							"compress": True,
							"location": self.repository_location
						}
					}
				)
				self.logger.info(f"successfully created repository({self.repository_name})")
			else:
				self.logger.info(f"skipped creating repository(exists, {self.repository_name})")

			return True
		except Exception:
			raise

	def createReindex(self):
		self.logger.info("started reindex task.")

		try:
			self.logger.info("started step2 task")
			indicies = list()
			indicies.append(base.constVar.DAILY_NEWS_SUMMARY)
			indicies.append(base.constVar.HOURLY_NEWS_SUMMARY)
			indicies.append(base.constVar.DAILY_PRODUCE_REPORTER)
			indicies.append(base.constVar.DAILY_PRODUCE_DEPART)
			indicies.append(base.constVar.DAILY_PRODUCE_SECTION)
			indicies.append(base.constVar.DAILY_PLATFORM_SUMMARY)

			indicies.append(base.constVar.HOURLY_SUMMARY)
			indicies.append(base.constVar.DAILY_SUMMARY)
			indicies.append(base.constVar.USER_MONTHLY_SUMMARY)
			indicies.append(base.constVar.USER_DAILY_SUMMARY)
			indicies.append(base.constVar.ELECTION_USER_DAILY_SUMMARY)
			indicies.append(base.constVar.ELECTION_DAILY_NEWS_SUMMARY)

			start_time = f"{self.target_month.strftime('%Y-%m')}-01T00:00:00+09:00"
			end_time = f"{self.target_month.strftime('%Y-%m')}-{monthrange(self.target_month.year, self.target_month.month)[1]}T23:59:59+09:00"

			for idx, val in enumerate(indicies):
				source_name = val

				# daily-news-summary, hourly-news-summary 이외의 인덱스는 index_name-YYYY로 관리
				if val not in (base.constVar.DAILY_NEWS_SUMMARY, base.constVar.HOURLY_NEWS_SUMMARY):
					source_name += datetime.today().strftime('%Y')

				dest_name = f"reindex_{val}-{self.target_month.strftime('%Y.%m')}"

				body = {
					"source": {
						"index": source_name,
						"query": {
							"range": {
								"reg_date": {
									"gte": start_time,
									"lte": end_time
								}
							}
						}
					},
					"dest": {
						"index": dest_name
					}
				}

				self.logger.info(f"reindex name is {dest_name}(condition)")
				self.logger.info(body)

				if self.es_client.indices.exists(index=dest_name):
					self.es_client.indices.delete(index=dest_name)
					self.logger.info(f"existing index name {dest_name} has been deleted.")

				self.es_client.reindex(
					body=body,
					wait_for_completion=True
				)
				self.reindex_list.append(dest_name)
				self.logger.info(f"created reindex:{dest_name}.")

			self.logger.info("successfully reindex task")
			return True
		except Exception:
			raise

	def createSnapshot(self):
		try:
			# process snapshot from reindex list
			indices = ','.join(self.reindex_list)
			print(indices)
			# 데이터가 없는 경우 reindex 생성되지 않아서 해당 인덱스가 있는지 체크
			# if not self.es_client.indices.exists(index=index_name):
			# 	continue

			body = {
				"indices": indices,
				"ignore_unavailable": True,
				"include_global_state": False,
				"metadata": {
					"taken_by": "lee.hoyeop",
					"taken_because": "monthly dashboard's data snapshot"
				}
			}

			self.logger.info(body)
			snapshot_name = f"dashboard_{self.target_month.strftime('%Y.%m')}"
			self.es_client.snapshot.create(
				repository=self.repository_name,
				snapshot=snapshot_name,
				body=body,
				wait_for_completion=True
			)
			self.logger.info(f"created snapshot name is {snapshot_name}")
		except Exception:
			raise

	# noinspection PyMethodMayBeStatic
	def cleanupReindex(self):
		try:
			# delete index from reindex indicies
			for val, idx in enumerate(self.reindex_list):
				if self.es_client.indices.exists(index=idx):
					self.es_client.indices.delete(index=idx)
					self.logger.info(f"reindex name {idx} has been deleted.")
		except Exception:
			raise
