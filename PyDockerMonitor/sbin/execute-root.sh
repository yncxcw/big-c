#!/usr/bin/env bash

sbin="`dirname "$0"`"
sbin="`cd "$sbin";pwd`"

. "$sbin/rmdocker-config.sh"

expect -c "
    spawn su root;
    expect {
         *assword* {send \"$RMDOCKER_PASSWORD\r\"}
    };
    expect # {send  \"$RMDOCKER_SBIN/daemon.sh $1 $2\r\"};
    expect # {send  \"exit\r\"};
    expect eof;
"

