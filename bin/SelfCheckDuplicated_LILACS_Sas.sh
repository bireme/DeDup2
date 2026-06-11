#!/bin/bash

if [ -z "$JAVA_HOME_25" ]; then
  JAVA_HOME_25="/home/users/operacao/.cache/coursier/arc/https/github.com/graalvm/graalvm-ce-builds/releases/download/jdk-25.0.1/graalvm-community-jdk-25.0.1_linux-x64_bin.tar.gz/graalvm-community-openjdk-25.0.1+8.1"
fi
export JAVA_HOME=$JAVA_HOME_25

export PATH=$JAVA_HOME/bin:$PATH

cd /home/javaapps/sbt-projects/DeDup2/ || exit

bin/GenSASPipe.sh
mv ./lilacs_Sas.pipe selfCheck/LILACS_Sas/ 
bin/SelfCheckDuplicated.sh -pipe=selfCheck/LILACS_Sas/lilacs_Sas.pipe -schema=schemas/configLILACS_Sas_Seven.cfg -outDupFile=selfCheck/LILACS_Sas/dup.pip -outNoDupFile=selfCheck/LILACS_Sas/nodup.pip -pipeEncoding=utf-8

cd -|| exit

