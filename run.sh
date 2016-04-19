#!/bin/bash
JAVA_OPTS="-Xmx5G -XX:+UseConcMarkSweepGC -XX:+CMSClassUnloadingEnabled -XX:MaxPermSize=5G -Xss2M -Duser.timezone=GMT"

#BIN="java -jar target/daily_hourly_bars_to_db-assembly-1.0-SNAPSHOT.jar"
BIN=./target/universal/stage/bin/daily_hourly_bars_to_db

# java -jar $BIN 132.properties m15
# $BIN 132.properties test 2016-03-06
$BIN 132.properties m15 $(date +'%Y-%m-%d')
