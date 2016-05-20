#!/usr/bin/expect

set password 1
spawn su root
expect "Password"
send "$password\r"
expect "#"
send "sh ./a.sh\r"
expect "#"
