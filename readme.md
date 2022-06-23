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

### 3.1 build
```
# 이미지 build
docker build -f Dockerfile_local --progress=tty -t airflow:2.3.2-01 .

# 이미지 체크
docker run -it -u root -it --name airflow airflow:2.3.2-01 bash
docker exec -u [root|airflow|default] -it airflow:2.3.2-01 bash
```


## 4-1. docker-compose with mssql

### 4-1.1 docker-compose up

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

### 4-1.2 airflow run
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
```


## 7. 서비스 실행에 따른 패키지 install

### 7.1 docker container에서 직접 설치

만약 worker를 다중으로 둔다면, 서비스에 필요한 패키지는 모든 노드에 맞춰서 각각 필요 패키지를 설치해야 함.

```
# 워커 names 확인하기
$ sudo docker ps
CONTAINER ID        IMAGE                                                                                            COMMAND                  CREATED             STATUS                    PORTS                    NAMES
d4dc88ab6588        localhost:5000/airflow@sha256:a6b61c7cff04b88f7c4abbd7dbea4563994484f2913019ccf7dcf250271faa39   "/bin/bash -c 'fun..."   12 seconds ago      Up 6 seconds              8080/tcp                 airflow_init.1.ldfx8u4iaopksuy5c60ae7ehx
0e5f19f4eccc        localhost:5000/airflow@sha256:a6b61c7cff04b88f7c4abbd7dbea4563994484f2913019ccf7dcf250271faa39   "/usr/bin/dumb-ini..."   36 minutes ago      Up 36 minutes (healthy)   8080/tcp                 airflow_worker.3.43r85o6makgq8p4i2kaviwk6k
2d6e45fd71e6        localhost:5000/airflow@sha256:a6b61c7cff04b88f7c4abbd7dbea4563994484f2913019ccf7dcf250271faa39   "/usr/bin/dumb-ini..."   36 minutes ago      Up 36 minutes (healthy)   8080/tcp                 airflow_worker.2.i133ct8xprbcm4yhu597ivi22
93fa49f54f02        localhost:5000/airflow@sha256:a6b61c7cff04b88f7c4abbd7dbea4563994484f2913019ccf7dcf250271faa39   "/usr/bin/dumb-ini..."   20 hours ago        Up 20 hours (healthy)     8080/tcp                 airflow_flower.1.rns44d5aikcgyojuzoo4izgd3
4a3edfba14d9        localhost:5000/airflow@sha256:a6b61c7cff04b88f7c4abbd7dbea4563994484f2913019ccf7dcf250271faa39   "/usr/bin/dumb-ini..."   20 hours ago        Up 20 hours (healthy)     8080/tcp                 airflow_scheduler.1.a280ux2xc4bozrlewvkpydwa0
0c0842fc7a25        localhost:5000/redis@sha256:563888f63149e3959860264a1202ef9a644f44ed6c24d5c7392f9e2262bd3553     "docker-entrypoint..."   20 hours ago        Up 20 hours (healthy)     6379/tcp                 airflow_redis.1.lttppjvuz3rco1a2xd4iomfmy
1bae36367ad2        localhost:5000/airflow@sha256:a6b61c7cff04b88f7c4abbd7dbea4563994484f2913019ccf7dcf250271faa39   "/usr/bin/dumb-ini..."   20 hours ago        Up 20 hours               8080/tcp                 airflow_jupyter.1.4026izt3qjlop6reazhfi7asz
43c4ff9e954d        localhost:5000/airflow@sha256:a6b61c7cff04b88f7c4abbd7dbea4563994484f2913019ccf7dcf250271faa39   "/usr/bin/dumb-ini..."   20 hours ago        Up 20 hours (healthy)     8080/tcp                 airflow_triggerer.1.ia2veslnmrx9nswhu9jf76xbv
6cc43bac80c2        localhost:5000/airflow@sha256:a6b61c7cff04b88f7c4abbd7dbea4563994484f2913019ccf7dcf250271faa39   "/usr/bin/dumb-ini..."   20 hours ago        Up 20 hours (healthy)     8080/tcp                 airflow_webserver.1.4cqoz1n66mxnvsx1muzlotgyr
babecac65fca        localhost:5000/airflow@sha256:a6b61c7cff04b88f7c4abbd7dbea4563994484f2913019ccf7dcf250271faa39   "/usr/bin/dumb-ini..."   20 hours ago        Up 20 hours (healthy)     8080/tcp                 airflow_worker.1.tdjxpkjvvp5tpyzezcbsfw623
edfff3913036        localhost:5000/mysql@sha256:0c0beeac7ca1937d60f54e1fb0c4a5c0b0ffee2aae37488fbc9f5ea301425551     "docker-entrypoint..."   20 hours ago        Up 20 hours (healthy)     3306/tcp, 33060/tcp      airflow_mysql.1.vh67eigpb1r9tc4ftyqlqv6xn
3b8e943ae841        registry                                                                                         "/entrypoint.sh /e..."   43 hours ago        Up 43 hours               0.0.0.0:5000->5000/tcp   registry


# 특정 worker에 직접 들어가서 패키지 설치 (이걸 worker 모두에게 해야 함)
$ sudo docker exec -it --user airflow airflow_worker.2.i133ct8xprbcm4yhu597ivi22 bash
$ python -m pip install --user rootpath 

```

### 7.2 python site-packages를 맵핑

또는 호스트 터미널에서 아래와 같이 하면, airflow 컨테이너에 따로 패키지 설치 없이 바로 사용 가능함.
관련하여 추가한 사항
- Dockerfile 내용 추가
```
COPY ./packages.pth /home/airflow/.local/lib/python3.7/site-packages
RUN sudo chmod -R o+rwx /home/airflow/.local/lib/python3.7/site-packages
```
- ./packages 디렉토리 생성
   - /opt/airflow/packages
- packages.pth 파일 추가

```
$ pip3 install --target=./packages python-dateutil
```

## 8. worker에 sudo 권한

worker에서 bashoperator로 sudo 권한이 필요한 작업을 해야 할 경우
worker 컨테이너에서 아래 명령어를 수행해야 함
`
usermod -e "" default
`

다만, 이를 위해 airflow entrypoint 파일을 수정함.
worker가 default user로 셋팅될때, 바로 sudo 권한 적용