from util.helper import Helper
from _datetime import datetime, timedelta
from calendar import monthrange
from util.helper import Parser
import base.constVar
import os
from pathlib import Path
from util.JtbcLogger import JtbcLogger
import elasticsearch.exceptions


class DailyRawDataSnapshot:
	def __init__(self, helper: Helper, target_date: datetime):
		self.es_client = helper.get_es()
		self.build_conf = Parser()
		self._target_date = target_date

		self._repository_name = f"test_daily_raw_{target_date.strftime('%Y.%m.%d')}"
		self._repository_location = f"{self.build_conf.snap_shot_location}/test/{self.repository_name}"
		if self.build_conf.build_level != 'dev' or self.build_conf.build_level == 'prod':
			self._repository_name = f"daily_raw_{target_date.strftime('%Y.%m.%d')}"
			self._repository_location = f"{self.build_conf.snap_shot_location}/{self.repository_name}"

		self._jtbc_snapshot_name = f"jtbc_{target_date.strftime('%Y.%m.%d')}"
		self._naver_snapshot_name = f"naver_{target_date.strftime('%Y.%m.%d')}"
		self._kakao_snapshot_name = f"kakao_{target_date.strftime('%Y.%m.%d')}"
		self._reindex_jtbc_indicies = list()
		self._reindex_naver_indicies = list()
		self._reindex_kakao_indicies = list()

		if os.path.isdir('/box/jnd-batch'):
			# prod
			path = '/box/jnd-batch'
		else:
			# dev
			path = Path(__file__).parent.parent.parent

		logger_factory = JtbcLogger(path=os.path.join(path, "logs"), file_name=type(self).__name__)
		self.logger = logger_factory.logger

	def __del__(self):
		pass

	@property
	def target_date(self):
		return self._target_date

	@property
	def reindex_jtbc_indicies(self):
		return self._reindex_jtbc_indicies

	@property
	def reindex_naver_indicies(self):
		return self._reindex_naver_indicies

	@property
	def reindex_kakao_indicies(self):
		return self._reindex_kakao_indicies

	@property
	def repository_name(self):
		return self._repository_name

	@property
	def repository_location(self):
		return self._repository_location

	@property
	def jtbc_snapshot_name(self):
		return self._jtbc_snapshot_name

	@property
	def naver_snapshot_name(self):
		return self._naver_snapshot_name

	@property
	def kakao_snapshot_name(self):
		return self._kakao_snapshot_name

	def createRepository(self):
		self.logger.info("started create repository task.")

		isExists = True
		try:
			self.es_client.snapshot.verify_repository(
				repository=self.repository_name
			)
		except elasticsearch.exceptions.NotFoundError:
			isExists = False

		try:
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
			# jtbc는 원본 그대로 스냅샷 처리
			reindex_name = f"jtbcnews-raw-{self.target_date.strftime('%Y.%m.%d')}"
			self.reindex_jtbc_indicies.append(reindex_name)

			start_time = f"{self.target_date.strftime('%Y-%m-%d')}T00:00:00+09:00"
			end_time = f"{self.target_date.strftime('%Y-%m-%d')}T23:59:59+09:00"

			# reindex naver rawdata by the daily
			reindex_name = f"reindex_{base.constVar.NAVER_NEWS_HOUR_STAT}-{self.target_date.strftime('%Y%m%d')}"
			body = {
				"source": {
					"index": base.constVar.NAVER_NEWS_HOUR_STAT,
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
					"index": reindex_name
				}
			}

			self.logger.info(f"reindex name is {reindex_name}(condition)")
			self.logger.info(body)
			self.es_client.reindex(
				body=body,
				wait_for_completion=True
			)
			self.reindex_naver_indicies.append(reindex_name)

			reindex_name = f"reindex_{base.constVar.NAVER_NEWS_DAY_STAT}-{self.target_date.strftime('%Y%m%d')}"
			body = {
				"source": {
					"index": base.constVar.NAVER_NEWS_DAY_STAT,
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
					"index": reindex_name
				}
			}

			self.logger.info(f"reindex name is {reindex_name}(condition)")
			self.logger.info(body)
			self.es_client.reindex(
				body=body,
				wait_for_completion=True
			)
			self.reindex_naver_indicies.append(reindex_name)

			# reindex kakao rawdata by the month
			reindex_name = f"reindex_{base.constVar.DAUM_NEWS_HOUR_STAT}-{self.target_date.strftime('%Y%m%d')}"
			body = {
				"source": {
					"index": base.constVar.DAUM_NEWS_HOUR_STAT,
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
					"index": reindex_name
				}
			}

			self.logger.info(f"reindex name is {reindex_name}(condition)")
			self.logger.info(body)
			self.es_client.reindex(
				body=body,
				wait_for_completion=True
			)
			self.reindex_kakao_indicies.append(reindex_name)

			reindex_name = f"reindex_{base.constVar.DAUM_NEWS_HOUR_STAT}-{self.target_date.strftime('%Y%m')}"
			body = {
				"source": {
					"index": base.constVar.DAUM_NEWS_DAY_STAT,
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
					"index": f"reindex_{base.constVar.DAUM_NEWS_DAY_STAT}-{self.target_date.strftime('%Y.%m')}"
				}
			}
			self.logger.info(f"reindex name is {reindex_name}(condition)")
			self.logger.info(body)
			self.es_client.reindex(
				body=body,
				wait_for_completion=True
			)
			self.reindex_kakao_indicies.append(reindex_name)
			self.logger.info("successfully reindex task")

			return True
		except Exception:
			raise

	def createSnapshot(self):
		try:
			# snapshot jtbc-rawdata
			body = {
				"indices": ','.join(self.reindex_jtbc_indicies),
				"ignore_unavailable": True,
				"include_global_state": False,
				"metadata": {
					"taken_by": "lee.hoyeop",
					"taken_because": "jtbc-raw data monthly backup"
				}
			}

			self.logger.info(body)
			self.es_client.snapshot.create(
				repository=self.repository_name,
				snapshot=self.jtbc_snapshot_name,
				body=body,
				wait_for_completion=True
			)
			self.logger.info(f"created snapshot jtbc-rawdata({self.jtbc_snapshot_name})")

			body = {
				"indices": ','.join(self.reindex_naver_indicies),
				"ignore_unavailable": True,
				"include_global_state": False,
				"metadata": {
					"taken_by": "lee.hoyeop",
					"taken_because": "naver-raw data monthly backup"
				}
			}

			self.logger.info(body)
			self.es_client.snapshot.create(
				repository=self.repository_name,
				snapshot=self.naver_snapshot_name,
				body=body,
				wait_for_completion=True
			)
			self.logger.info(f"created snapshot naver-rawdata{self.naver_snapshot_name}")

			body = {
				"indices": ','.join(self.reindex_kakao_indicies),
				"ignore_unavailable": True,
				"include_global_state": False,
				"metadata": {
					"taken_by": "lee.hoyeop",
					"taken_because": "snapshot by monthly backup"
				}
			}

			self.logger.info(body)
			self.es_client.snapshot.create(
				repository=self.repository_name,
				snapshot=self.kakao_snapshot_name,
				body=body,
				wait_for_completion=True
			)
			self.logger.info(f"created snapshot kakao-rawdata{self.kakao_snapshot_name}")

			return True
		except Exception:
			raise

	# noinspection PyMethodMayBeStatic
	def cleanupReindex(self):
		try:
			# delete index from reindex indicies
			all_reindex_list = self.reindex_jtbc_indicies + self.reindex_naver_indicies + self.reindex_kakao_indicies
			for val, idx in enumerate(all_reindex_list):
				if self.es_client.indices.exists(index=idx):
					self.es_client.indices.delete(index=idx)
					self.logger.info(f"reindex name {idx} has been deleted.")
		except Exception:
			raise
