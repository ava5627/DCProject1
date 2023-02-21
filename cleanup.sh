#!/bin/bash

# Change this to your netid
netid=ash170000

# Directory where the config file is located on the running system
CONFIGLOCAL=./config.txt

n=0

cat $CONFIGLOCAL | sed -e "s/#.*//" | sed -e "/^\s*$/d" |
(
    read i
    echo $i
    while [[ $n -lt $(echo $i | sed "s/\s.*//") ]]
    do
        read line
        host=$( echo "$line" | awk '{ print $2 }' )
        ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no $netid@"$host" killall Project1 &
        n=$(( n + 1 ))
    done
)
killall xterm
echo "Cleanup complete"