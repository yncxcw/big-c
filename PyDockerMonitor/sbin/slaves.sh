#!/usr/bin/env bash

##run a shell command on all slave hosts


sbin="`dirname "$0"`"
sbin="`cd "$sbin";pwd`"

if [ $# -lt 1 ];then
   
   exit 1

fi

. "$sbin/rmdocker-config.sh"

RMDOCKER_SLAVES="$RMDOCKER_SBIN/slaves"


if [ -f "$RMDOCKER_SLAVES" ]; then
   HOSTLIST=`cat "$RMDOCKER_SLAVES"`
else
   HOSTLIST="localhost"
fi



if [ "$RMDOCKER_SSH_OPTS"="" ]; then
   RMDOCKER_SSH_OPTS="-o StrictHostKeyChecking=no -t"
fi 



for slave in `echo "$HOSTLIST"|sed  "s/#.*$//;/^$/d"`; do

  echo "login on slave $slave"
  ssh $RMDOCKER_SSH_OPTS "$slave" cd $RMDOCKER_SBIN" ; $sbin/execute-root.sh" slave $1 2>&1 | sed "s/^/$slave: /" &
  
  if [ "$RMDOCKER_SLAVE_SLEEP" != "" ]; then
    sleep $RMDOCKER_SLAVE_SLEEP
  fi
done

wait
