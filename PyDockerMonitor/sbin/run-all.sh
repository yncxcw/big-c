#!/usr/bin/env bash


#start all RMDocker daemons.
#start the master on this node.
#start a worker on each node specified in conf/slaves

sbin="`dirname "$0"`"
sbin="`cd "$sbin";pwd`"


##Load the configuration
. "$sbin/rmdocker-config.sh"


usage="Usage run-all.sh (start|stop)"

if [ $# -lt 1 ]; then
  echo $usage
  exit 1

fi

case $1 in
   (start)
 ##start Master
 "$sbin"/daemon.sh master start
 sleep 5
 ##start slaves
 "$sbin"/slaves.sh start
;;
   (stop)
 ##start Master
"$sbin"/daemon.sh master stop
 ##start slaves
"$sbin"/slaves.sh stop
;;
 (*)
 echo $usage
 exit 1
;;
esac
