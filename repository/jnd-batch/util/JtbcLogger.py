import os
import logging
from logging import handlers
from util.Singleton import Singleton
from pathlib import Path

class JtbcLogger(metaclass=Singleton):

    # # log_path = os.path.join(path, 'logs', '{}_{}.log'.format(file, datetime.today().strftime('%Y%m%d')))
    #         # log path: '/Users/jmac/project/jtbc_news_data/data-batch/main/logs/NewsSummary_20211018.log'
    def __init__(self, path:str=None, file_name:str=None):
        if path is None:
            path = os.path.join(Path(__file__).parent.parent, "logs")
            print(path)
            file_name = self.__class__.__name__
        os.makedirs(path, exist_ok=True)

        self._formatter = logging.Formatter(
            '%(asctime)s [%(levelname)s] - %(filename)s:%(funcName)s - line:%(lineno)d - %(message)s')

        self._logger = logging.getLogger()
        self._logger.setLevel(logging.INFO)

        self._file_handler = logging.handlers.TimedRotatingFileHandler(
            filename=os.path.join(path, file_name),
            when="midnight",
            interval=1,
            encoding='utf-8',
            backupCount=3
        )
        self._stream_handler = logging.StreamHandler()

        self._file_handler.suffix = "%Y%m%d.log"

        self._file_handler.setFormatter(self._formatter)
        self._stream_handler.setFormatter(self._formatter)

        self._logger.addHandler(self._file_handler)
        self._logger.addHandler(self._stream_handler)

        self._logger.info(f"log path : {os.path.join(path, file_name)}")


    @property
    def logger(self):
        return self._logger



if __name__ == "__main__":

    logger_factory = JtbcLogger(file_name="NewsSummary")
    logger = logger_factory.logger
    logger.info(f"logger's id = {id(logger_factory)}")

    path = Path(__file__).parent.parent
    logger_factory2 = JtbcLogger(path=os.path.join(path, "logs"), file_name="NewsSummary")
    logger2 = logger_factory2.logger
    logger2.info(f"logger2's id = {id(logger_factory2)}")


