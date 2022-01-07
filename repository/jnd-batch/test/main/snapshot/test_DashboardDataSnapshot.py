from unittest import TestCase
import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
<<<<<<< HEAD
from main.snapshot.MonthlyDashboardDataSnapshot import MonthlyDashboardDataSnapshot
=======
from main.snapshot.MonthlyDashboardSnapshot import MonthlyDashboardSnapshot
>>>>>>> b20cec9 (raw snapshot(monthly->daily),dashboard snapshot(3months->monthly))
from util.helper import Helper
import pprint
from datetime import datetime
from util.JtbcLogger import JtbcLogger
from pathlib import Path
import os


class TestDashboardDataSnapshot(TestCase):

	def __init__(self, *args, **kwargs):
		path = Path(__file__).parent.parent.parent.parent
		logger_factory = JtbcLogger(path=os.path.join(path, "logs"), file_name=self.__class__.__name__)
		logger = logger_factory.logger

		helper = Helper()
<<<<<<< HEAD
		self.dashboard_snapshot = MonthlyDashboardDataSnapshot(helper=helper, target_month=datetime(2021, 10, 1))
=======
		self.dashboard_snapshot = MonthlyDashboardSnapshot(helper=helper, target_month=datetime(2021, 10, 1))
>>>>>>> b20cec9 (raw snapshot(monthly->daily),dashboard snapshot(3months->monthly))

		super(TestDashboardDataSnapshot, self).__init__(*args, **kwargs)

	def test_Step1Task(self):
		self.dashboard_snapshot.createRepository()

	def test_Step2Task(self):
		self.dashboard_snapshot.createReindex()
		self.dashboard_snapshot.createSnapshot()

	def test_Step3Task(self):
		# self.dashboard_snapshot.Step3Task()
		pass

	def test_StepFinalTask(self):
		n = input('몇 개 출력?')
		w = input('몇 개 마다 줄바꿈')
		print(w)
		for i in range(n):
			print('#')

	def test_testTask(self):
		the_date = datetime(datetime.today().year, datetime.today().month-1, 1)
		print(the_date)
