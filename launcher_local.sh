CONFIG_LOCAL=./config_local.txt
BINARY_DIR=./cmake-build-debug
PROGRAM=Project1

cat $CONFIG_LOCAL | sed -e "s/#.*//" | sed -e "/^\s*$/d" |
(
  read i
  echo $i
  while [[ $n -lt $i ]]
  do
    read line
    p=$( echo $line | awk '{ print $1 }' )
    host=$( echo $line | awk '{ print $2 }' )
    kitty --hold -e $BINARY_DIR/$PROGRAM $p $CONFIG_LOCAL &
    n=$(( n + 1 ))
  done
)
