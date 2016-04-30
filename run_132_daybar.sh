#!/bin/bash
JAVA_OPTS="-Xmx8G -XX:+UseConcMarkSweepGC -XX:+CMSClassUnloadingEnabled -XX:MaxPermSize=8G -Xss2M -Duser.timezone=GMT"

#BIN="java -jar target/daily_hourly_bars_to_db-assembly-1.0-SNAPSHOT.jar"
BIN=./target/universal/stage/bin/daily_hourly_bars_to_db

# java -jar $BIN 132.properties m15
# $BIN 132.properties test 2016-03-06


###################################################
# from the h1 bar constructed from HKEx trade tick data, adjust with corporate action, and put in database
###################################################
echo "Start: "$(date)
$BIN 132.properties d1 $(date +'%Y-%m-%d')
echo "End: "$(date)
