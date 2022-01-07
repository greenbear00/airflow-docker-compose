from unittest import TestCase
import os
import sys
from main.snapshot.DailyRawDataSnapshot import DailyRawDataSnapshot
from util.helper import Helper
from datetime import datetime
from util.JtbcLogger import JtbcLogger
from pathlib import Path
import os
import pprint


class TestDailyRawDataSnapshot(TestCase):

	def __init__(self, *args, **kwargs):
		path = Path(__file__).parent.parent.parent.parent
		logger_factory = JtbcLogger(path=os.path.join(path, "logs"), file_name=self.__class__.__name__)
		logger = logger_factory.logger

		helper = Helper()
		self.daily_snapshot = DailyRawDataSnapshot(helper=helper, target_date=datetime(2021, 10, 1))

		super(TestDailyRawDataSnapshot, self).__init__(*args, **kwargs)

	def test_Step1Task(self):
		self.daily_snapshot.createRepository()

	def test_Step2TaskandStep3Task(self):
		self.daily_snapshot.createReindex()
		self.daily_snapshot.createSnapshot()

	def test_Step3Task(self):
		pass

	def test_StepFinalTask(self):
		pass

	def test_testTask(self):
		the_date = datetime(datetime.today().year, datetime.today().month-1, 1)
		print(the_date)
