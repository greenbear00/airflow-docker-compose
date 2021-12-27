# airflow docker-compose

## 1. docker-compose version check
```bash
$ docker-compose -version
docker-compose version 1.29.2, build 5becea4c
```

## 2. airflow docker-compose 다운로드
```bash
# quick start: yaml 다운로드
$ curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.2.2/docker-compose.yaml'

# 해당 머신의 memory check (4gb 이상이어야 함)
$ docker run --rm "debian:buster-slim" bash -c 'numfmt --to iec $(echo $(($(getconf _PHYS_PAGES) * $(getconf PAGE_SIZE))))'
```

## 3. 