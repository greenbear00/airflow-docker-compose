# airflow docker-compose
- postgresql -> mysql로 변경
- docker-compose 및 docker swarm기반 docker-compose 추가 

## 1. docker-compose version check
```bash
$ docker-compose -version
docker-compose version 1.29.2, build 5becea4c
```

## 2. airflow docker-compose 다운로드
```bash
# quick start: yaml 다운로드 (2022.06.08 stable version 2.3.2)
$ curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.3.2/docker-compose.yaml'

# 해당 머신의 memory check (4gb 이상이어야 함)
$ docker run --rm "debian:buster-slim" bash -c 'numfmt --to iec $(echo $(($(getconf _PHYS_PAGES) * $(getconf PAGE_SIZE))))'
```

## 3. base 이미지인 airflow 2.3.2 버전 수정
### 3.1 Dockerfile
apache/airflow:2.3.2를 기본으로 하며, docker-compose에 기존 postgres 대신 **mysql로 변경**
그와 관련된 패키지 install 포함

```
$ mkdir .dags .logs .plugins
$ echo -e "AIRFLOW_UID=$(id -u)" > .env

$ docker pull apache/airflow:2.3.2

# mysql로 이미지 변동으로 인한 pymysql 패키지 설치
$ vi Dockerfile
FROM apache/airflow:2.3.2

RUN /usr/local/bin/python -m pip install --upgrade pip  
RUN pip3 install pymysql
RUN pip3 install plyvel 

########################################################################
# root 권한으로 ps, vi 설치 
# - apply alias ll
########################################################################
USER root

RUN apt-get update && apt-get install -y procps vim
RUN echo "alias ll='ls --color=auto -alF'" >> ~/.bashrc
RUN source ~/.bashrc
# # airflow에 해당하는 script 추가 및 권한 적용
# WORKDIR /opt/airflow/script
# ADD airflow/* /opt/airflow/script
# RUN chown airflow:root *.sh
# RUN chmod u+x *.sh

########################################################################
# by airflow user
# - install jupyter
# - apply alias ll
########################################################################
USER airflow
RUN pip3 install jupyter
RUN echo "alias ll='ls --color=auto -alF'" >> ~/.bashrc
RUN source ~/.bashrc
WORKDIR /opt/airflow/script


########################################################################
# default user
# - apply alias ll
########################################################################
USER ${AIRFLOW_UID}
RUN echo "alias ll='ls --color=auto -alF'" >> ~/.bashrc
RUN source ~/.bashrc
WORKDIR /opt/airflow



ENTRYPOINT ["/usr/bin/dumb-init", "--", "/entrypoint"]
CMD []
```

### 3.2 build
```
# 이미지 build
docker build -f Dockerfile_local -t airflow:2.3.2-01 .

# 이미지 체크
docker run -it -u root -it --name airflow airflow:2.3.2-01 bash
docker exec -u [root|airflow|default] -it airflow:2.3.2-01 bash
```


## 4-1. docker-compose with mssql
### 4-1.1 docker-compose.yaml
```
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# Basic Airflow cluster configuration for CeleryExecutor with Redis and PostgreSQL.
#
# WARNING: This configuration is for local development. Do not use it in a production deployment.
#
# This configuration supports basic configuration using environment variables or an .env file
# The following variables are supported:
#
# AIRFLOW_IMAGE_NAME           - Docker image name used to run Airflow.
#                                Default: apache/airflow:2.2.2
# AIRFLOW_UID                  - User ID in Airflow containers
#                                Default: 50000
# Those configurations are useful mostly in case of standalone testing/running Airflow in test/try-out mode
#
# _AIRFLOW_WWW_USER_USERNAME   - Username for the administrator account (if requested).
#                                Default: airflow
# _AIRFLOW_WWW_USER_PASSWORD   - Password for the administrator account (if requested).
#                                Default: airflow
# _PIP_ADDITIONAL_REQUIREMENTS - Additional PIP requirements to add when starting all containers.
#                                Default: ''
#
# Feel free to modify this file to suit your needs.
---
version: '3'
x-airflow-common:
  &airflow-common
  # In order to add custom dependencies or upgrade provider packages you can use your extended image.
  # Comment the image line, place your Dockerfile in the directory where you placed the docker-compose.yaml
  # and uncomment the "build" line below, Then run `docker-compose build` to build the images.
  # image: ${AIRFLOW_IMAGE_NAME:-apache/airflow:2.3.2}
  image: ${AIRFLOW_IMAGE_NAME:-airflow:2.3.2-01}
  # build: .
  environment:
    &airflow-common-env
    AIRFLOW__CORE__EXECUTOR: CeleryExecutor
    # AIRFLOW__CORE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
    # AIRFLOW__CELERY__RESULT_BACKEND: db+postgresql://airflow:airflow@postgres/airflow
    AIRFLOW__CORE__SQL_ALCHEMY_CONN: mysql+pymysql://airflow:airflow@mysql:3306/airflow
    AIRFLOW__CELERY__RESULT_BACKEND: db+mysql://airflow:airflow@mysql:3306/airflow
    AIRFLOW__CELERY__BROKER_URL: redis://:@redis:6379/0
    AIRFLOW__CORE__FERNET_KEY: ''
    AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'true'
    AIRFLOW__WEBSERVER__EXPOSE_CONFIG: 'true'
    AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
    AIRFLOW__API__AUTH_BACKEND: 'airflow.api.auth.backend.basic_auth'
    _PIP_ADDITIONAL_REQUIREMENTS: ${_PIP_ADDITIONAL_REQUIREMENTS:-}
  volumes:
    - ./dags:/opt/airflow/dags
    - ./logs:/opt/airflow/logs
    # - airflowlogs:/usr/local/airflow/logs
    - ./plugins:/opt/airflow/plugins
    # - ${PWD}/airflow.cfg:/opt/airflow/airflow.cfg
    - ./repository:/opt/airflow/repository
  command:
    - pip3 install -r /opt/airflow/repository/requirements.txt
  user: "${AIRFLOW_UID:-50000}:0"
  depends_on:
    &airflow-common-depends-on
    redis:
      condition: service_healthy
    # postgres:
    #   condition: service_healthy
    mysql:
      condition: service_healthy

services:
  # postgres:
  #   image: postgres:13
  #   environment:
  #     POSTGRES_USER: airflow
  #     POSTGRES_PASSWORD: airflow
  #     POSTGRES_DB: airflow
  #   volumes:
  #     - postgres-db-volume:/var/lib/postgresql/data
  #   healthcheck:
  #     test: ["CMD", "pg_isready", "-U", "airflow"]
  #     interval: 5s
  #     retries: 5
  #   restart: always
  mysql:
    container_name: mysql
    image: mysql:8.0.27
    environment:
      MYSQL_ROOT_PASSWORD: root
      MYSQL_USER: airflow
      MYSQL_PASSWORD: airflow
      MYSQL_DATABASE: airflow
    volumes:
      - mysql-db-volume:/var/lib/mysql
    command: # 명령어 실행
      - --character-set-server=utf8mb4
      - --collation-server=utf8mb4_unicode_ci
      - --default-authentication-plugin=mysql_native_password
      - --lower-case-table-names=1
    healthcheck:
            test: ["CMD", "mysqladmin" ,"ping", "-h", "localhost"]
            timeout: 20s
            retries: 10

  redis:
    container_name: redis
    image: redis:6.2.6
    expose:
      - 6379
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 30s
      retries: 50
    restart: always

  airflow-webserver:
    <<: *airflow-common
    container_name: airflow-webserver
    command: webserver
    ports:
      - 8080:8080
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:8080/health"]
      interval: 10s
      timeout: 10s
      retries: 5
    restart: always
    depends_on:
      <<: *airflow-common-depends-on
      airflow-init:
        condition: service_completed_successfully

  airflow-scheduler:
    <<: *airflow-common
    container_name: airflow-scheduler
    command: scheduler
    healthcheck:
      test: ["CMD-SHELL", 'airflow jobs check --job-type SchedulerJob --hostname "$${HOSTNAME}"']
      interval: 10s
      timeout: 10s
      retries: 5
    restart: always
    depends_on:
      <<: *airflow-common-depends-on
      airflow-init:
        condition: service_completed_successfully

  airflow-worker:
    <<: *airflow-common
    container_name: airflow-worker
    command: celery worker
    healthcheck:
      test:
        - "CMD-SHELL"
        - 'celery --app airflow.executors.celery_executor.app inspect ping -d "celery@$${HOSTNAME}"'
      interval: 10s
      timeout: 10s
      retries: 5
    environment:
      <<: *airflow-common-env
      # Required to handle warm shutdown of the celery workers properly
      # See https://airflow.apache.org/docs/docker-stack/entrypoint.html#signal-propagation
      DUMB_INIT_SETSID: "0"
    restart: always
    #ports:
    #  - 50001:8793
    depends_on:
      <<: *airflow-common-depends-on
      airflow-init:
        condition: service_completed_successfully

  airflow-triggerer:
    <<: *airflow-common
    container_name: airflow-triggerer
    command: triggerer
    healthcheck:
      test: ["CMD-SHELL", 'airflow jobs check --job-type TriggererJob --hostname "$${HOSTNAME}"']
      interval: 10s
      timeout: 10s
      retries: 5
    restart: always
    depends_on:
      <<: *airflow-common-depends-on
      airflow-init:
        condition: service_completed_successfully

  airflow-init:
    <<: *airflow-common
    entrypoint: /bin/bash
    # yamllint disable rule:line-length
    command:
      - -c
      - |
        function ver() {
          printf "%04d%04d%04d%04d" $${1//./ }
        }
        airflow_version=$$(gosu airflow airflow version)
        airflow_version_comparable=$$(ver $${airflow_version})
        min_airflow_version=2.2.0
        min_airflow_version_comparable=$$(ver $${min_airflow_version})
        if (( airflow_version_comparable < min_airflow_version_comparable )); then
          echo
          echo -e "\033[1;31mERROR!!!: Too old Airflow version $${airflow_version}!\e[0m"
          echo "The minimum Airflow version supported: $${min_airflow_version}. Only use this or higher!"
          echo
          exit 1
        fi
        if [[ -z "${AIRFLOW_UID}" ]]; then
          echo
          echo -e "\033[1;33mWARNING!!!: AIRFLOW_UID not set!\e[0m"
          echo "If you are on Linux, you SHOULD follow the instructions below to set "
          echo "AIRFLOW_UID environment variable, otherwise files will be owned by root."
          echo "For other operating systems you can get rid of the warning with manually created .env file:"
          echo "    See: https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html#setting-the-right-airflow-user"
          echo
        fi
        one_meg=1048576
        mem_available=$$(($$(getconf _PHYS_PAGES) * $$(getconf PAGE_SIZE) / one_meg))
        cpus_available=$$(grep -cE 'cpu[0-9]+' /proc/stat)
        disk_available=$$(df / | tail -1 | awk '{print $$4}')
        warning_resources="false"
        if (( mem_available < 4000 )) ; then
          echo
          echo -e "\033[1;33mWARNING!!!: Not enough memory available for Docker.\e[0m"
          echo "At least 4GB of memory required. You have $$(numfmt --to iec $$((mem_available * one_meg)))"
          echo
          warning_resources="true"
        fi
        if (( cpus_available < 2 )); then
          echo
          echo -e "\033[1;33mWARNING!!!: Not enough CPUS available for Docker.\e[0m"
          echo "At least 2 CPUs recommended. You have $${cpus_available}"
          echo
          warning_resources="true"
        fi
        if (( disk_available < one_meg * 10 )); then
          echo
          echo -e "\033[1;33mWARNING!!!: Not enough Disk space available for Docker.\e[0m"
          echo "At least 10 GBs recommended. You have $$(numfmt --to iec $$((disk_available * 1024 )))"
          echo
          warning_resources="true"
        fi
        if [[ $${warning_resources} == "true" ]]; then
          echo
          echo -e "\033[1;33mWARNING!!!: You have not enough resources to run Airflow (see above)!\e[0m"
          echo "Please follow the instructions to increase amount of resources available:"
          echo "   https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html#before-you-begin"
          echo
        fi
        mkdir -p /sources/logs /sources/dags /sources/plugins
        chown -R "${AIRFLOW_UID}:0" /sources/{logs,dags,plugins}
        exec /entrypoint airflow version
    # yamllint enable rule:line-length
    environment:
      <<: *airflow-common-env
      _AIRFLOW_DB_UPGRADE: 'true'
      _AIRFLOW_WWW_USER_CREATE: 'true'
      _AIRFLOW_WWW_USER_USERNAME: ${_AIRFLOW_WWW_USER_USERNAME:-airflow}
      _AIRFLOW_WWW_USER_PASSWORD: ${_AIRFLOW_WWW_USER_PASSWORD:-airflow}
    user: "0:0"
    volumes:
      - .:/sources

  airflow-cli:
    <<: *airflow-common
    profiles:
      - debug
    environment:
      <<: *airflow-common-env
      CONNECTION_CHECK_MAX_COUNT: "0"
    # Workaround for entrypoint issue. See: https://github.com/apache/airflow/issues/16252
    command:
      - bash
      - -c
      - airflow

  flower:
    <<: *airflow-common
    container_name: flower
    command: celery flower
    ports:
      - 5555:5555
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:5555/"]
      interval: 10s
      timeout: 10s
      retries: 5
    restart: always
    depends_on:
      <<: *airflow-common-depends-on
      airflow-init:
        condition: service_completed_successfully

volumes:
  mysql-db-volume:
  # airflowlogs:

```

### 4-1.2 docker-compose up

airflow-init을 수행 (따로 airflow-init을 안하고 바로 docker-compose up으로 해도 됨)
```
# 초기 셋팅 (repository는 다양한 소스 repository를 의미)
$ mkdir dags logs plugins repository
# 만약에 실행중에 permission error가 난다면 mount한 부분에 권한 적용
#$ chmod -R 777 dags/
#$ chmod -R 777 logs/
#$ chmod -R 777 plugins/
#$ chmod -R 777 repository/

# 버전이 올라가면서 airflow_GID는 사라짐 (오류 나와도 무시하면 됨)
#$ echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
$ echo -e "AIRFLOW_UID=$(id -u)" > .env

$ docker-compose up airflow-init
airflow-cluster-test-airflow-init-1  | [2021-12-27 04:18:06,822] {manager.py:214} INFO - Added user airflow
airflow-cluster-test-airflow-init-1  | User "airflow" created with role "Admin"
airflow-cluster-test-airflow-init-1  | 2.2.2
airflow-cluster-test-airflow-init-1 exited with code 0

```

airflow run
```
$ docker-compose up -d

# 만약 worker를 여러개 띄우고싶다면
# docker-compose up -d --scale airflow-worker=3

# 상태확인
$ docker-compose ps
NAME                             COMMAND                  SERVICE             STATUS              PORTS
airflow-cluster_airflow-init_1   "/bin/bash -c 'funct…"   airflow-init        exited (0)
airflow-scheduler                "/usr/bin/dumb-init …"   airflow-scheduler   running (healthy)   8080/tcp
airflow-triggerer                "/usr/bin/dumb-init …"   airflow-triggerer   running (healthy)   8080/tcp
airflow-webserver                "/usr/bin/dumb-init …"   airflow-webserver   running (healthy)   0.0.0.0:8080->8080/tcp
airflow-worker1                  "/usr/bin/dumb-init …"   airflow-worker1     running (healthy)   0.0.0.0:50001->8793/tcp
airflow-worker2                  "/usr/bin/dumb-init …"   airflow-worker2     running (healthy)   0.0.0.0:50002->8793/tcp
flower                           "/usr/bin/dumb-init …"   flower              running (healthy)   0.0.0.0:5555->5555/tcp
mysql                            "docker-entrypoint.s…"   mysql               running (healthy)   33060/tcp
redis                            "docker-entrypoint.s…"   redis               running (healthy)   6379/tcp


# 이때 dag에 dag를 작성하면서 repository에 있는 소스를 실행했을때 파일 생성 등의 소스가 permission error가 발생하면
# host쪽 mount directory의 사용자와 그룹 정보를 봐야함
# 보면 docker-compose로 마운트한 dags, logs, plugins은 greenbear:root로 되어 있기에 해당 부분 권한을 변경하면 정상 동작함
[greenbear airflow-cluster-test]$ ls -l
total 84
# drwxrwxrwx 4 greenbear root        128 Jun  9 10:11 dags
# -rw-rw-r-- 1 greenbear greenbear 10180 Jun  8 13:46 docker-compose_original.yaml
# -rw-rw-r-- 1 greenbear greenbear 19864 Jun  8 16:50 docker-compose_swarm_for_local.yaml
# -rw-rw-r-- 1 greenbear greenbear 20314 Jun  8 16:50 docker-compose.yaml
# -rw-rw-r-- 1 greenbear greenbear  1691 Jun  9 11:13 Dockerfile
# -rw-rw-r-- 1 greenbear greenbear  2043 Jun  8 15:10 Dockerfile_proxy
# drwxrwxr-x 2 greenbear greenbear    30 Jun  8 13:30 img
# drwxrwxrwx 6 greenbear root        117 Jun  9 10:11 logs
# drwxrwxrwx 2 greenbear root          6 Jun  8 14:02 plugins
# -rw-rw-r-- 1 greenbear greenbear 20724 Jun  9 11:13 readme.md
# drwxrwxr-x 3 greenbear root         23 Jun  8 13:30 repository
```


## 4-2. docker-compose with swarm
swarm 버전으로 airflow를 docker-compose해서 올리는 방법
### 4-2.1. private docker registry for swarm
swarm 버전으로 airflow를 올리기 위해서는 사설 docker registry가 존재해야 함.
일반적으로 nexus를 많이 쓰나, 여기에서는 docker registry를 그대로 사용
```
$ docker image pull registry

# 기본 pass 맞추기 
$ mkdir docker-registry
$ cd docker-registry
$ mkddir data

# 띄우기
$ docker run -d -p 5000:5000 --restart=always --name registry -v ${pwd}/data:/var/lib/registry registry

# 실제 private registry에 이미지 올려보기
$ docker pull centos:8
$ vi Dockerfile
FROM docker.io/centos:8
#FROM cetons:8

CMD echo "hello, os"

# 빌드 및 private registry에 이미지 올리기
$ docker build -t localhost:5000/centos:8-01 .
$ docker push localhost:5000/centos:8-01 

# 확인
$ docker images localhost:5000/centos:8-01
$ curl -X GET http://localhost:5000/v2/_catalog
{"repositories": ["centos"]}

```
### 4-2.2 airflow 준비 
private repository에 이미지 업로드
```
$ docker build -f Dockerfile_local -t airflow:2.3.2-01 .
$ docker image tag airflow:2.3.2-01 localhost:5000/airflow:2.3.2-01
$ docker push localhost:5000/airflow:2.3.2-01
```

### 4-2.3 airflow docker-compose 파일 수정
해당 버전에 image만 private docker repository로 변경해서 push 후, deploy할경우 아래와 같은 에러가 발생.
따라서 docker-compose 파일을 수정해줘야 함.

실제 sawrm버전 파일: docker-compose_swarm_for_local.yaml

- services.airflow-scheduler.depends_on must be a list

### 4-2.3 배포 및 확인
```
$ sudo docker-compose -f docker-compose_swarm_for_local.yaml push
$ sudo docker stack deploy -c docker-compose_swarm_for_local.yaml airflow
$ sudo docker stack services airflow
ID            NAME               MODE        REPLICAS  IMAGE
05oafi4sjoc9  airflow_init       replicated  1/1       localhost:5000/airflow:2.3.2-01
7hrjh2ilq7zt  airflow_scheduler  replicated  1/1       localhost:5000/airflow:2.3.2-01
ecelkpik4ole  airflow_mysql      replicated  1/1       localhost:5000/mysql:8.0.29
k1uv6s0gk2rv  airflow_worker     replicated  1/1       localhost:5000/airflow:2.3.2-01
ocbow9unufro  airflow_triggerer  replicated  1/1       localhost:5000/airflow:2.3.2-01
pqwsjn90ko2r  airflow_redis      replicated  1/1       localhost:5000/redis:6.2.6
qu5zoi0lmasl  airflow_flower     replicated  1/1       localhost:5000/airflow:2.3.2-01
t4x17unaiyxg  airflow_webserver  replicated  1/1       localhost:5000/airflow:2.3.2-01
tsh4i4j66gr1  airflow_jupyter    replicated  1/1       localhost:5000/airflow:2.3.2-01

# 중간에 worker만 늘리는 법
$ sudo docker service scale airflow_worker=3
ID            NAME               MODE        REPLICAS  IMAGE
05oafi4sjoc9  airflow_init       replicated  1/1       localhost:5000/airflow:2.3.2-01
7hrjh2ilq7zt  airflow_scheduler  replicated  1/1       localhost:5000/airflow:2.3.2-01
ecelkpik4ole  airflow_mysql      replicated  1/1       localhost:5000/mysql:8.0.29
k1uv6s0gk2rv  airflow_worker     replicated  3/3       localhost:5000/airflow:2.3.2-01
ocbow9unufro  airflow_triggerer  replicated  1/1       localhost:5000/airflow:2.3.2-01
pqwsjn90ko2r  airflow_redis      replicated  1/1       localhost:5000/redis:6.2.6
qu5zoi0lmasl  airflow_flower     replicated  1/1       localhost:5000/airflow:2.3.2-01
t4x17unaiyxg  airflow_webserver  replicated  1/1       localhost:5000/airflow:2.3.2-01
tsh4i4j66gr1  airflow_jupyter    replicated  1/1       localhost:5000/airflow:2.3.2-01

# 내리는 법
$ sudo docker stack rm airflow
```



## 5. airflow 확인하기
http://localhost:8080 으로 접속하면 airflow로 접속되며,
기본 ID/PW는 위에 설정한 ID/PW를 따른다.

초기에 셋팅해놓은 MappingRunner가 잘뜨고, 실행이 되면 문제 없음.
또한 dags내에 작성해둔 airflow_test2.py (실제 DAG 이름은 MappingRunner)에 자신이 작성한 파이선 코드를 맵핑하고 싶다면
repository내에 소스코드를 작성해서 추가하면 됨

그리고 개인이 작성한 파이선 코드에 설치된 requirements.txt는 클러스터로 설치되기 때문에
worker에도 같이 설치하는게 좋음
```
$ docker exec -u airflow airflow-worker1 pip3 install -r /opt/airflow/repository/requirements.txt
$ docker exec -u airflow airflow-worker2 pip3 install -r /opt/airflow/repository/requirements.txt

```

실제 로컬에서 구동한 airflow
![airflow](img/airflow_8080.png)


## 6. jupyter
실제 http://localhost:8888 로 접속하고 token에 'airflow'를 입력하면 접속 가능

확인방법:
```
$ docker exec -u airflow airflow-jupyter bash
$ jupyter notebook list
