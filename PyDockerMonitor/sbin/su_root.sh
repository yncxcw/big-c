#!/usr/bin/expect

set password password
spawn su root
expect "Password"
send "$password\r"
expect "#"
send "sh ./a.sh\r"
expect "#"
