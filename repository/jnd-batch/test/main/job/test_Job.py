from main.job.Job import Job
from datetime import datetime
import unittest



class TestJob(unittest.TestCase):

	@Job(job_name="test_job")
	def test_run(self, logger, my_helper, target_date=datetime.now()):
		print(f"hello target_date={target_date}")

		return True