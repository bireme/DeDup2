#!/bin/bash

if [ -z "$JAVA_HOME_25" ]; then
  JAVA_HOME_25="/home/users/operacao/.cache/coursier/arc/https/github.com/graalvm/graalvm-ce-builds/releases/download/jdk-25.0.1/graalvm-community-jdk-25.0.1_linux-x64_bin.tar.gz/graalvm-community-openjdk-25.0.1+8.1"
fi
export JAVA_HOME=$JAVA_HOME_25

export PATH=$JAVA_HOME/bin:$PATH

cd /home/javaapps/sbt-projects/DeDup2 || exit

if [ -f /bases/fiadmin2/exec/settings/dedup.inc ]; then
  . /bases/fiadmin2/exec/settings/dedup.inc
fi

bin/MySQL2Pipe.sh -mySqlHost=$mysqlserver -mySqlPort=$mysqlport -mySqlUser=$servername -mySqlPassword=$serverpassword -mySqlDbname=$serverdatabase -sqlfs=sqls/LILACS_MNTam.sql,sqls/LILACS_MNTam_ingles.sql -outCsvFile=csv/lilacs_MNTam.csv

cd - || exit
