#!/bin/bash

if [ -z "$JAVA_HOME_25" ]; then
  JAVA_HOME_25="/home/users/operacao/.cache/coursier/arc/https/github.com/graalvm/graalvm-ce-builds/releases/download/jdk-25.0.1/graalvm-community-jdk-25.0.1_linux-x64_bin.tar.gz/graalvm-community-openjdk-25.0.1+8.1"
fi
export JAVA_HOME=$JAVA_HOME_25

export PATH=$JAVA_HOME/bin:$PATH

cd /home/javaapps/sbt-projects/DeDup2/ || exit

bin/GenMNTPipe.sh
mv ./lilacs_MNT.pipe selfCheck/LILACS_MNT/ 
bin/SelfCheckDuplicated.sh -pipe=selfCheck/LILACS_MNT/lilacs_MNT.pipe -schema=schemas/configLILACS_MNT_Four.cfg -outDupFile=selfCheck/LILACS_MNT/dup.pip -outNoDupFile=selfCheck/LILACS_MNT/nodup.pip -pipeEncoding=utf-8

cd -|| exit

