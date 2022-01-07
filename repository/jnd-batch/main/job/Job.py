from functools import wraps
from pathlib import Path
from util.JtbcLogger import JtbcLogger
import os
import traceback
from util.helper import Helper


class Job:
	def __init__(self, job_name:str=None):
		self.job_name = job_name
		if os.path.isdir('/box/jnd-batch'):
			# prod
			path = '/box/jnd-batch'
		else:
			# dev
			path = Path(__file__).parent.parent.parent # data-batch

		if job_name is None:
			logger_factory = JtbcLogger(path=os.path.join(path, "logs"), file_name=type(self).__name__)
		else:
			logger_factory = JtbcLogger(path=os.path.join(path, "logs"), file_name=job_name)
		self.logger = logger_factory.logger
		self.my_helper = Helper()

	def __call__(self, func):
		@wraps(func)
		def decorator(*args, **kwargs):
			flag = True
			try:
				self.logger.info("=" * 50)
				self.logger.info(f"[JOB] {self.job_name} ")
				# self.logger.info(f"before {func.__name__}")
				# flag = func(*args, **kwargs, logger=self.logger, my_helper=self.my_helper)
				flag = func(*args, **kwargs, logger=self.logger, my_helper=self.my_helper)
				# self.logger.info(f"job result = {flag}")
				# self.logger.info(f"after {func.__name__}")
			except (ValueError, Exception):
				# exception 발생에 따른 error 로그 출력 및 slack으로 error 메시지 전송 마지막으로 return Flase로 job fail 처리
				self.logger.error(f"[{self.my_helper.build_level.upper()}] ERROR: {traceback.format_exc()}")
				self.logger.error(f"[{self.my_helper.build_level.upper()}] job({self.job_name}) was not working. "
							 f"(target_date = {kwargs.get('target_date').strftime('%Y-%m-%d %H:%M:%S')})\n\n\n")

				self.my_helper.get_slack().post_message(f"[{self.my_helper.build_level.upper()}] job({self.job_name}) ERROR "
												   f"(target_date = {kwargs.get('target_date').strftime('%Y-%m-%d %H:%M:%S')})\n"
												   f"{traceback.format_exc()}")
				flag=False
			return flag

		return decorator


# @Job(job_name="ddd")
# def func_2(a, b):
# 	return (a + b)


# if __name__ == "__main__":
# 	# func_1()
# 	func_2(4, 5)
