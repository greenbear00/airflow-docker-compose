#!/bin/bash

############################################################
# [dev or prod] unittest 수행

# cd /box/jnd-batch-tmp
# chmod u+x /box/jnd-batch-tmp/script/test_runner.sh
# ./script/test_runner.sh
# 주의) test폴더 밑에 있는 unittest들은 상위 디렉토리(즉, 루트 디렉토리 /box/temp/jnd-batch에서 수행)
############################################################

path=$(pwd)
echo -e "\n\ncheck current path=$path"
export PYTHONPATH=$path:$path/test:$PYTHONPATH
echo "check PYTHONPATH=$PYTHONPATH"


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


#echo "unittest"
##source $path/venv/bin/activate
#echo -e "$path/test/main/job/test_TimebasedRunnerTest.py -v"
#python3 $path/test/main/job/test_TimebasedRunnerTest.py -v
#echo -e "\n\n$path/test/test_helper.py -v"
#python3 $path/test/test_helper.py -v
#deactivate
run_unittest test_helper.py
check_fun_exit test_helper.py $?