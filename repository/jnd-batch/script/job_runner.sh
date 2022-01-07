#!/bin/bash

############################################################
# 1. dev or prod 서버에서 임시폴더에 검증 작업 수행
# mv data-batch.tar data-batch-20211101.tar
# mkdir /box/jnd-batch-tmp
# tar -xvf data-batch.tar /box/jnd-batch-tmp

# 2. 해당 스크립트를 돌려서 정상적으로 돌아가는지 확인인 (main/job에 있는 것들을 모두 수행)
# cd /box/jnd-batch-tmp
# chmod u+x /box/jnd-batch-tmp/script/job_runner.sh
# ./script/job_runner.sh

# 3. crontab에 /box/temp/jnd-batch 디렉토리의 .py 파일이 정상적으로 수행되고, 로그(/box/jnd-batch/logs)가 쌓이는지 확인
# * * * * * PYTHONPATH=/box/jnd-batch-tmp /usr/bin/python3 /box/jnd-batch-tmp/main/job/TimebasedRunner.py

# 4. 실제 적용 (매시 30분경에 수행)
# mv /box/jnd-batch /box/jnd-batch-original
# mv /box/jnd-batch-tmp /box/jnd-batch

# 5. crontab 적용
############################################################

#export PYTHONPATH=$PYTHONPATH:/box/jnd-batch-tmp
#
#echo "job test: TimebasedRunner"
##/usr/bin/python3 /box/jnd-batch-tmp/main/job/NewsSummary.py
##/usr/bin/python3 /box/jnd-batch-tmp/main/job/ProduceNewsSummary.py
#/usr/bin/python3 /box/jnd-batch-tmp/main/job/TimebasedRunner.py
#echo -e "job test: TimebasedRunner done\n\n"

path=$(pwd)
echo -e "\n\ncheck current path=$path"
export PYTHONPATH=$path:$PYTHONPATH
echo "check PYTHONPATH=$PYTHONPATH"


echo "job start"
#source $path/venv/bin/activate
#echo -e "$path/main/job/TimebasedRunner.py -v"
#python3 $path/main/job/TimebasedRunner.py -v

#echo -e "$path/main/job/NewsSummary.py -v"
#python3 $path/main/job/NewsSummary.py -v

#echo -e "$path/main/job/ProduceNewsSummary.py -v"
#python3 $path/main/job/ProduceNewsSummary.py -v

#echo -e "$path/main/job/PlatformSummaryJob.py -v"
#python3 $path/main/job/PlatformSummaryJob.py -v

#deactivate

function run_unittest()
{
  filename=$1
  echo -e "\n\n$path/test/$filename -v"
  python3 $path/test/$filename -v
}

function check_fun_exit()
{
  filename=$1
  result=$2
#  echo "filename $filename"
#  echo "result $result"
  if [ $result -eq 0 ]
  then
    echo "$filename unittest is normal exit ($result)"
  else
      echo "$filename is abnormal exit ($result)"
      exit 1
  fi
}


run_unittest main/job/test_MappingJTBCTVProRunner.py
check_fun_exit main/job/test_MappingJTBCTVProRunner.py $?

run_unittest main/job/test_TimebasedRunner.py
check_fun_exit main/job/test_TimebasedRunner.py $?
run_unittest main/job/test_PlatformNewsSummary.py
check_fun_exit main/job/test_PlatformNewsSummary.py $?
run_unittest main/job/test_UserSummaryRunner.py
check_fun_exit main/job/test_UserSummaryRunner.py $?
run_unittest main/job/test_ElectionSummaryRunner.py
check_fun_exit main/job/test_ElectionSummaryRunner.py $?

run_unittest main/job/test_NewsSummary.py
check_fun_exit main/job/test_NewsSummary.py $?

run_unittest main/job/test_ProduceNewsSummary.py
check_fun_exit main/job/test_ProduceNewsSummary.py $?
