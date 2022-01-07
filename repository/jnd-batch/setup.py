from setuptools import setup, find_packages
from datetime import datetime

dd = find_packages()

# 패키지 name은 jtbcnews로 하되, 실제 배포는 패키지 방식이 아닌 crontab에서 PYTHONPATH를 지정하는 방식으로 배포
setup(
    name='jtbcnews',
    version=datetime.now().strftime('%Y.%m.%d'),
    description='jtbc news packages',
    author='lee.hoyeop, woo.sujeong',
    setup_requires=['wheel', 'twine', 'setuptools'],
    author_email='lee.hoyeop@joongang.co.kr, woo.sujeong@joongang.co.kr',
    # packages=find_packages(include=[ 'jtbcnews','jtbcnews.*'], exclude=['test', 'logs', 'jtbcnews.main.logs']),
    packages=find_packages(exclude=['test', 'conf', 'schema_json','logs']),
    package_data={'':['*.json']},
    python_requires='>=3',
    include_package_data=True
)
