#!/bin/bash

#BIN="java -jar target/daily_hourly_bars_to_db-assembly-1.0-SNAPSHOT.jar"
BIN=./target/universal/stage/bin/daily_hourly_bars_to_db

# java -jar $BIN 132.properties m15 corpactadj
$BIN 132.properties test corpactadj



