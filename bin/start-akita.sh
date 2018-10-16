#!/bin/sh

case "`uname`" in
    Linux)
		bin_absolute_path=$(readlink -f $(dirname $0))
		;;
	*)
		bin_absolute_path=`cd $(dirname $0); pwd`
		;;
esac
akita_home=${bin_absolute_path}/..

export AKITA_HOME=$akita_home

# Java
if [ -z "$JAVA" ] ; then
  JAVA=$(which java)
fi

# PID
if [ -f $akita_home/bin/canal.pid ] ; then
	echo "ERROR: Stop server first." 2>&2
    exit 1
fi

# Start Server
mkdir -p $akita_home/log
$JAVA \
  -Xmx512m \
  -Xms256m \
  -XX:+PrintGC \
  -XX:+PrintGCDetails \
  -XX:+PrintGCTimeStamps \
  -XX:+PrintGCDateStamps \
  -Xloggc:$akita_home/log/gc.log \
  -Dcom.sun.management.jmxremote.port=11123 \
  -Dcom.sun.management.jmxremote.authenticate=false \
  -Dcom.sun.management.jmxremote.ssl=false \
  -jar akita.jar -c $akita_home/conf/akita.properties -s $1 &
echo $! > $akita_home/bin/canal.pid