# data-batch

## JTBC News 데이터 시스템 배치

### 1. 개요
엘라스틱 기반 데이터 배치 어플리케이션

### 2. 개발환경
- SDK: Python 3.7
- Library: requirements.txt
- Configuration: pyenv, pyenv-virtualenv

### 3. install 방법
#### 3.1 bamboo 
1. bamboo에서 dev/prod에 맞춰서 빌드 수행
2. 각 dev/prod에서 script에서 job들이 정상적으로 동작하는지를 확인
   1. prod는 script내에서 build_level=prod로 자동 변경 적용
3. script가 정상적으로 작동을 확인하면
   1. job 수행을 고려하여 매시 30분에 
   2. /box/jnd-batch 를 우선 압축 또는 임의의 폴더로 백업 수행
   3. /box/jnd-batch-tmp 를 /box/jnd-batch로 변경
   4. crontab에 job 등록

#### 3.2 (참고) PYTHONPATH를 crontab에 적용
1. local에서 수행
```
# 프로젝트 디렉토리 내에서 작업 수행
tar --exclude='.idea' --exclude='.git' --exclude='.gitignore' --exclude='build' --exclude='dist' --exclude='jtbcnews.egg-info' --exclude='data-batch.jar' --exclude='venv' -zcvf data-batch.tar .

# 압축파일 내용 확인
tar tvf data-batch.tar
```
2. 실제 배포 서버(dev or prod)에서 수행
```
# 파일 압축 풀기
$ tar -xvf data-batch.tar

# requirements.txt는 조인스의 김형주 차장님께 말씀드리고 설치 요청하기 (상용버전에서는 pip으로 인스톨 안하기)
# $ pip install -r requirement.txt
```

3. 서버에 맞춰서 conf/build.ini 내용 수정
```
[build]
# build leve (dev or prod)
BUILD_LEVEL = dev

[elastic]
# 자동 alias 적용 옵션 (True or False)
AUTO_APPLY_ALIAS = False
```

5. 실제 배포 서버에서 crontab 적용 (스크립트 방식이 더 나음)
```
* * * * * PYTHONPATH=/box/jnd-data /usr/bin/python3 /box/jnd-data/main/job/TimebasedRunner.py
```

#### 3.3 (참고) 스크립트 방식
```
####### [job_runner.sh 파일 생성 (참고 script 폴더)] #######
$ mkdir script
$ vi script/job_runner.sh 
#!/bin/bash
export PYTHONPATH=/box/jnd-data/

# 만약 내부에서 따로 정의한 venv를 사용할 경우, 아래와 같이 가상환경을 load 수행
# source ~~~/bin/activate
/usr/bin/python3 /box/jnd-batch/main/job/TimebasedRunner.py > /dev/null 2>&1

####### [job_runner.sh 파일 실행권한 적용 및 실행] #######
$ chmod u+x script/job_runner.sh
$ ./script/job_runner.sh

####### [crontab 등록] #######
$ crontab -e
* * * * * /box/jnd-batch/job_runner.sh
```

참고: 패키지 방식이 아니기에 path 찾는 방식은 pathlib이 아닌 rootpath로 해도 무방 (현재는 pathlib으로 지정됨)