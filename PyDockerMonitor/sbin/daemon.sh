#!/usr/bin/env bash

sbin="`dirname "$0"`"
sbin="`cd "$sbin";pwd`"

if [ $# -lt 2 ]; then
 echo "error when start slave, not enough parameter"
 exit 1
fi


. "$sbin/rmdocker-config.sh"


pid_slave=/tmp/rmdocker-slave.pid
pid_master=/tmp/rmdocker-master.pid
pid_name=/tmp/rmdocker-name.pid


if [ $1 = "master" ]; then
 pyexecute=pyro4NameServer.py
 pid=$pid_master

elif [ $1 = "slave" ]; then
 pyexecute=containerManager.py
 pid=$pid_slave

else

 echo "unknown parameters"
 exit 1

fi

case $2  in

 (start)
 
  
  if [ $1 = "master" ]; then 
  nohup python3 -m Pyro4.naming -n $RMDOCKER_NAME_SERVER > myout1.file 2>&1& #< /dev/null& 
  newpid=$!
  echo $newpid > $pid_name
  echo "startting nameserver as process $newpid"  
  
  fi

  echo "python3 $RMDOCKER_BIN/$pyexecute" 
  nohup python3 $RMDOCKER_BIN/$pyexecute > myout.file 2>&1& #< /dev/null& 
  newpid=$!
  echo $newpid > $pid
  echo "startting $pyexecute as proces $newpid"  
  ;;

 (stop)

    if [ -f $pid ]; then
     TARGET_ID="$(cat "$pid")"
     echo "stopping $pyexecute"
     kill "$TARGET_ID" && rm -f "$pid"

  if [ $1 = "master" ]; then 
    if [ -f $pid_name ];then
     TARGET_ID="$(cat "$pid_name")"
     echo "stopping name server" 
     kill "$TARGET_ID" && rm -f "$pid_name"

    fi  
  fi



    else
     echo "no $pyexecute to stop"
   fi
  ;;

 (*)
    echo "unkonw commands"
    exit 1
   ;;
esac
  
 
