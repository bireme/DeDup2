#!/bin/bash

if [ -z "$JAVA_HOME_25" ]; then
  JAVA_HOME_25="/home/users/operacao/.cache/coursier/arc/https/github.com/graalvm/graalvm-ce-builds/releases/download/jdk-25.0.1/graalvm-community-jdk-25.0.1_linux-x64_bin.tar.gz/graalvm-community-openjdk-25.0.1+8.1"
fi
export JAVA_HOME=$JAVA_HOME_25

export PATH=$JAVA_HOME/bin:$PATH

cd /home/javaapps/sbt-projects/DeDup2/ || exit

. /bases/fiadmin2/exec/settings/dedup.inc

bin/SelfCheckDuplicated.sh 
-mySqlHost=$mysqlserver 
-mySqlPort=$mysqlport
-mySqlUser=$servername
-mySqlPassword=$serverpassword 
-mySqlDbname=$serverdatabase 
-sqlf=sql/DIREV.sql 
-schema=schemas/configDIREV_Three.cfg 
-fieldToIndex= 
-confFile= 
-mongoDatabase=DeDup2 
-mongoCollection=DIREV

cd -|| exit