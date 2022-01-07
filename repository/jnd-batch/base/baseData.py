import abc
import datetime


class BaseData(metaclass=abc.ABCMeta):

	@abc.abstractmethod
	def getDataListHourly(self, the_date: datetime = None):
		# 시간별 조회/공감/댓글 데이터 셋
		raise NotImplemented

	@abc.abstractmethod
	def getDataListDaily(self, the_date: datetime = None):
		# 조회/공감/댓글 데이터 일별 집계
		raise NotImplemented

	@abc.abstractmethod
	def aggrNewsDataById(self, the_date: datetime = None):
		# 뉴스 ID 단위 데이터 셋
		raise NotImplemented


