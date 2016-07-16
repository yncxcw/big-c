#!/usr/bin/env bash

this="`dirname "$0"`"
this="`cd "$this"/../;pwd`"

export RMDOCKER_NAME_SERVER=host6
export RMDOCKER_PASSWORD=password
export RMDOCKER_SLAVE_SLEEP=2
export RMDOCKER_SLAVES="slaves"
export RMDOCKER_PREFIX="$this"
export RMDOCKER_HOME="$RMDOCKER_PREFIX"
export RMDOCKER_CONF="$RMDOCKER_PREFIX/conf"
export RMDOCKER_SBIN="$RMDOCKER_PREFIX/sbin"
export RMDOCKER_BIN="$RMDOCKER_HOME"

