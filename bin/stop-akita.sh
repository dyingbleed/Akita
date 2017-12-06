#!/bin/sh

pidfile=`dirname $0`/../bin/canal.pid
if [ ! -f "$pidfile" ] ; then
	echo "服务已停止"
	exit
fi

pid=`cat $pidfile`
echo -e "`hostname`: 正在停止 Akita $pid ... "
kill $pid

if [ -f "$pidfile" ] ; then
	rm $pidfile
	exit
fi