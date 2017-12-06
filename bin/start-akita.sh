#!/bin/sh

# Akita Home
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

# 检查 pid 文件
if [ -f $akita_home/bin/canal.pid ] ; then
	echo "错误：服务已启动，请先停止之前的服务" 2>&2
    exit 1
fi

# 启动服务
mkdir -p $akita_home/logs
$JAVA -jar Akita.jar -p $akita_home/conf/akita.properties 1>>$akita_home/logs/akita.log 2>&1 &
echo $! > $akita_home/bin/canal.pid