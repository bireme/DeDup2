#!/bin/bash

if [ -z "$JAVA_HOME_25" ]; then
  JAVA_HOME_25="/home/users/operacao/.cache/coursier/arc/https/github.com/graalvm/graalvm-ce-builds/releases/download/jdk-25.0.1/graalvm-community-jdk-25.0.1_linux-x64_bin.tar.gz/graalvm-community-openjdk-25.0.1+8.1"
fi
export JAVA_HOME=$JAVA_HOME_25

export PATH=$JAVA_HOME/bin:$PATH

cd /home/javaapps/sbt-projects/DeDup2 || exit

. /bases/fiadmin2/exec/settings/dedup.inc

bin/MySQL2Pipe.sh -host=$mysqlserver -port=$mysqlport -user=$servername -pswd=$serverpassword -dbnm=$serverdatabase -sqls=/home/javaapps/sbt-projects/DCDup/sql/LILACS_Sas.sql,/home/javaapps/sbt-projects/DCDup/sql/LILACS_Sas_ingles.sql -pipe=lilacs_Sas.pipe

cd - || exit
