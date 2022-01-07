import json
from configparser import ConfigParser

from elasticsearch import Elasticsearch
import os
import pymssql
from util.JtbcLogger import JtbcLogger
from pathlib import Path

from util.Singleton import Singleton
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from urllib.error import URLError


class Slack(metaclass=Singleton):

    # proxy
    # https://www.py4u.net/discuss/238241
    # https://slack.dev/node-slack-sdk/web-api#proxy-requests-with-a-custom-agent
    def __init__(self, token:str, channel_name:str, enable:bool, proxy:str=None):
        self.token = token
        if proxy:
            self.client = WebClient(token=token, proxy=proxy, timeout=60)
        else:
            self.client = WebClient(token=token)
        self.channel_name = channel_name
        self.enable = enable
        self._conversations_li = None
        if os.path.isdir('/box/jnd-batch'):
            path = '/box/jnd-batch'
        else:
            # data-batch
            path = Path(__file__).parent.parent.parent
        logger_factory = JtbcLogger(path=os.path.join(path, "logs"), file_name=type(self).__name__)
        self.logger = logger_factory.logger
        self.logger.info(f"Append Slack Client (channel_name = {self.channel_name}, enable={self.enable})")

    def _get_conversations_list(self):
        self._conversations_li = self.client.conversations_list(types="public_channel, private_channel")

    def get_all_channel_info(self) -> list:
        channels = []
        if self._conversations_li is None:
            self._get_conversations_list()

        for c in self._conversations_li.get('channels'):
            self.logger.info(c)
        channels = self._conversations_li.get('channels')
        return channels

    def get_channel_info(self, channel_name=None) -> dict:
        if channel_name is None:
            channel_name = self.channel_name
        self.logger.info(f"\n\n========== channel info {':' + channel_name if channel_name else ': ALL'} ==========")
        channel = {}
        if self._conversations_li is None:
            self._get_conversations_list()
        if channel_name:
            channel = list(filter(lambda x: x.get('name') == channel_name, self._conversations_li.get('channels')))
            channel = channel[0] if channel else {}
            self.logger.info(channel)
        else:
            for c in self._conversations_li.get('channels'):
                self.logger.info(c)
            channel = {}
        return channel

    def get_channel_history(self, channel, limit: int = None):
        history = self.client.conversations_history(channel=channel.get('id'))
        # print(dir(history))
        messages = history.get('messages')
        # for m in messages:
        #     print(m)
        self.print_history(channel.get('name'), messages, limit=limit)
        return messages

    def print_history(self, channel_name, history, limit: int = None):
        if limit is not None:
            history = history[:limit]

        self.logger.info(
            f"\n\n========== channel({channel_name}) history {'(ALL)' if limit is None else '(last ' + str(limit) + ')'} ==========")
        i = 0
        for m in history:
            self.logger.info(f"[{i} {'(bot)' if m.get('bot_id') else '(not bot)'}]\t {m}")
            i += 1

    def get_bots_message_ts_from_history(self, channel, history, index: int):
        self.logger.info(f"\n\n========== delete message type check from {channel.get('name')} channel ==========")
        self.logger.info(f"[{index} {'(bot)' if history[index].get('bot_id') else '(not bot)'}] {history[index]}")
        return history[index].get('ts') if history[index].get('bot_id') else None

    def delete_message(self, channel, ts):
        if self.enable:
            if ts:
                channel_id = channel.get('id')
                self.logger.info(
                    f"\n\n========== delete message from {channel.get('name')} channel, ts={ts} ==========")
                res = self.client.chat_delete(channel=channel_id, ts=ts)
                self.logger.info(str(res))

    def post_message(self, msg, channel_name: str = None):
        if self.enable:
            # if isinstance(channel_name, dict):
            # 	channel = channel_name.get('name')
            if channel_name is None:
                channel_name = self.channel_name
            try:
                res = self.client.chat_postMessage(channel=channel_name, text=msg)
            except SlackApiError as e:
                self.logger.error(e.response['error'])
            except (URLError, Exception) as e:
                self.logger.error(e)


class Parser(metaclass=Singleton):
    @property
    def auto_apply_alias(self):
        return self._auto_apply_alias

    @property
    def snap_shot_location(self):
        return self._snap_shot_location

    @property
    def backup_location(self):
        return self._backup_location

    @property
    def build_level(self):
        return self._build_level

    @property
    def slack_token(self):
        return self._slack_token

    @property
    def query_split_size(self):
        return self._query_split_size

    @property
    def slack_channel(self):
        return self._slack_channel

    @property
    def slack_enable(self):
        return self._slack_enable

    @property
    def proxy(self):
        return self._proxy

    def __init__(self):
        if os.path.isdir('/box/jnd-batch'):
            path = '/box/jnd-batch'
        else:
            path = Path(__file__).parent.parent  # data-batch

        logger_factory = JtbcLogger(path=os.path.join(path, "logs"), file_name=type(self).__name__)
        self.logger = logger_factory.logger

        self._build_level = None
        self._auto_apply_alias = False
        self._slack_token = None
        self._slack_channel = None
        self._slack_enable = False
        self._snap_shot_location = None
        self._query_split_size = 10
        self._backup_location = None

        self._parser = ConfigParser()
        if os.path.isfile(os.path.join(path, *['conf', 'build.ini'])):
            self._parser.read(os.path.join(path, *['conf', 'build.ini']))
            self.logger.info(f"load build conf : {os.path.join(path, *['conf', 'build.ini'])}")

            self._load_build_conf()
            self._load_elastic_conf()
            self._load_slack()

    def _load_build_conf(self):
        if self._parser.sections():
            self._build_level = self._parser.get('build', 'BUILD_LEVEL') if self._parser.has_option('build',
                                                                                                    'BUILD_LEVEL') else None
            self.logger.info(f"BUILD_LEVEL = {self.build_level} (build.ini)")

    def _load_elastic_conf(self):
        if self._parser.sections():
            self._auto_apply_alias = self._parser.getboolean('elastic', 'AUTO_APPLY_ALIAS') if self._parser.has_option(
                'elastic',
                'AUTO_APPLY_ALIAS') else False
            self.logger.info(f"AUTO_APPLY_ALIAS = {self.auto_apply_alias} (build.ini)")
            self._snap_shot_location = self._parser.get('elastic', 'SNAP_SHOT_LOCATION') \
                if self._parser.has_option('elastic', 'SNAP_SHOT_LOCATION') else False
            self.logger.info(f"SNAP_SHOT_LOCATION = {self.snap_shot_location} (build.ini)")
            self._backup_location = self._parser.get('elastic', 'BACK_UP_LOCATION') \
                if self._parser.has_option('elastic', 'BACK_UP_LOCATION') else False
            self.logger.info(f"BACK_UP_LOCATION = {self.backup_location} (build.ini)")

    #         QUERY_SPLIT_SIZE
            self._query_split_size = self._parser.getint('elastic', 'QUERY_SPLIT_SIZE') if self._parser.has_option(
                'elastic',
                'QUERY_SPLIT_SIZE') else 10

    def _load_slack(self):
        if self._parser.sections():
            self._slack_token = self._parser.get('slack', 'token') if self._parser.has_option(
                'slack',
                'token') else None
            self.logger.info(f"SLACK TOKEN = {self.slack_token} (build.ini)")

            self._slack_channel = self._parser.get('slack', 'channel') if self._parser.has_option(
                'slack',
                'channel') else None
            self.logger.info(f"SLACK CHANNEL = {self.slack_channel} (build.ini)")

            self._slack_enable = self._parser.getboolean("slack", "enable") if self._parser.has_option("slack",
                                                                                                       "enable") else False
            self.logger.info(f"SLACK ENABLE = {self.slack_enable} (build.ini)")

            if self.build_level == "dev":
                self._proxy = None
            else:
                self._proxy = self._parser.get('proxy', 'proxy') if self._parser.has_option("proxy", "proxy") else None
            self.logger.info(f"PROXY = {self.proxy}")


class Helper:
    @property
    def build_level(self):
        return self._build_level

    @property
    def query_split_size(self):
        return self._query_split_size

    @property
    def config(self):
        return self._config

    def __init__(self, env: str = None):
        if os.path.isdir('/box/jnd-batch'):
            # prod
            path = '/box/jnd-batch'
        else:
            # dev
            path = Path(__file__).parent.parent
        # if env == "dev":
        # 	self.config_file = os.path.join(self.root_path, "conf", "dev-config.json")
        # elif env == "prod":
        # 	self.config_file = os.path.join(self.root_path, "conf", "prod-config.json")
        # elif env == "qa":
        # 	self.config_file = os.path.join(self.root_path, "conf", "qa-config.json")

        self._parser = Parser()
        if env is not None:
            self._build_level = env
        else:
            self._build_level = self._parser.build_level

        self._query_split_size = self._parser.query_split_size

        conf_file_name = "dev-config.json" if self._parser.build_level == "dev" else "prod-config.json" if self._parser.build_level == "prod" else "qa-config.json"
        self.config_file = os.path.join(path, *['conf', conf_file_name])

        logger_factory = JtbcLogger(path=os.path.join(path, "logs"), file_name=type(self).__name__)
        self.logger = logger_factory.logger
        self.logger.info(f"load {self._parser.build_level} conf_file : {self.config_file}")

        with open(self.config_file, 'r') as f:
            self._config = json.load(f)

    def get_slack(self):
        slack = Slack(token=self._parser.slack_token, channel_name=self._parser.slack_channel,
                      enable=self._parser.slack_enable, proxy=self._parser.proxy)
        return slack

    def get_es(self):
        username = self.config['elastic']['username']
        password = self.config['elastic']['password']
        es = Elasticsearch(
            self.config['elastic']['hosts'],
            http_auth=(username, password),
            timeout=7800, max_retries=1, retry_on_timeout=True)
        return es

    def get_news_db(self, is_autocommit=False):
        key_name = "jtbc-news-db"
        server = self.config[key_name]['server']
        user = self.config[key_name]['user']
        password = self.config[key_name]['password']
        database = self.config[key_name]['database']
        conn = pymssql.connect(server=server, port=1433, user=user, password=password, database=database,
                               autocommit=is_autocommit, tds_version="7.0")
        return conn

    def get_tv_db(self, is_autocommit=False):
        key_name = "jtbc-tv-db"
        server = self.config[key_name]['server']
        user = self.config[key_name]['user']
        password = self.config[key_name]['password']
        database = self.config[key_name]['database']
        conn = pymssql.connect(server=server, port=1433, user=user, password=password, database=database,
                               autocommit=is_autocommit, tds_version="7.0")
        return conn


if __name__ == "__main__":
    p = Parser()
    from datetime import datetime

    # error: DB-Lib error message 20002, severity 9: Adaptive Server connection failed
    # -> solution: os에 tsd(Tabular Data Stream) 설치
    # -> 참고: https://docs.microsoft.com/ko-kr/sql/connect/python/pymssql/step-1-configure-development-environment-for-pymssql-python-development?view=sql-server-ver15
    # h = Helper(p.build_level)
    h = Helper()
    h.get_news_db()
    h.get_tv_db()
    print(h.query_split_size, type(h.query_split_size))
    # s = h.get_slack()
    #
    # # slack에 bot 메시지 보내기
    # s.post_message(channel_name=p.slack_channel, msg=f"[{h.build_level.upper()}] oh, yes! {datetime.now()}")
    #
    # # slack에 최근 bot 메시지 삭제
    # # channel = s.get_channel_info(channel_name=p.slack_channel)
    # channel = s.get_channel_info()
    # history = s.get_channel_history(channel=channel, limit=3)
    # ts = s.get_bots_message_ts_from_history(channel=channel, history=history, index=0)
    # s.delete_message(channel=channel, ts=ts)
