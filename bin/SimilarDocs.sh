#!/usr/bin/env bash

if [ -z "$JAVA_HOME_25" ]; then
  JAVA_HOME_25="/home/users/operacao/.cache/coursier/arc/https/github.com/graalvm/graalvm-ce-builds/releases/download/jdk-25.0.1/graalvm-community-jdk-25.0.1_linux-x64_bin.tar.gz/graalvm-community-openjdk-25.0.1+8.1"
fi
PATH=$JAVA_HOME_25/bin:$PATH

cd /home/javaapps/sbt-projects/DeDup2 || exit

sbt 'runMain dd.SimilarDocs $1 $2 $3 $4 $5 $6 $7 $8 $9'

cd - || exit
