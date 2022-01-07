import unittest
import rootpath
import os
import subprocess
from deepdiff import DeepDiff
import json
import pprint

class RequirementsChecker(unittest.TestCase):

    def setUp(self) -> None:
        self.path = rootpath.detect()

        self.ori_rootpath = self._get_path(self.path, "requirements_old.txt")

    def _get_path(self, path, file_name):
        if os.path.isfile(os.path.join(path, file_name)):
            return os.path.join(path, file_name)
        else:
            return None

    def test_path_cheker(self):
        self.assertEqual(self.path, '/Users/jmac/project/jtbc_news_data/data-batch', 'path does not equal')

    def _make_req_info(self, file_path:str):

        with open(file_path) as f:
            # ori_data = eval(f.read().replace('\n', ''))
            data = f.readlines()

        req_info = {}
        for ori in data:
            p = ori.replace('\n', '').split("==")
            # print(p[0], p[1])
            req_info[p[0]] = p[1]
        return req_info

    def test_load_freez_requirements(self):
        new_filename = "requirements_new.txt"
        if self.ori_rootpath:
            subprocess.call(f"pip3 freeze > {self.path}/{new_filename}", shell=True)

            ori_req_info = self._make_req_info(self.ori_rootpath)
            # pprint.pp(ori_req_info)

            new_req_info = self._make_req_info(self._get_path(self.path, new_filename))
            diff = DeepDiff(ori_req_info, new_req_info)

            li = list(diff.keys())
            for l in li:
                print(l,"="*100)
                diff_data = diff.get(l)
                print(diff_data)






if __name__ == '__main__':
    RequirementsChecker()
