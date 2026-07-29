#!/usr/bin/env bash

if [ -z "$JAVA_HOME_25" ]; then
  JAVA_HOME_25="/home/users/operacao/.cache/coursier/arc/https/github.com/graalvm/graalvm-ce-builds/releases/download/jdk-25.0.1/graalvm-community-jdk-25.0.1_linux-x64_bin.tar.gz/graalvm-community-openjdk-25.0.1+8.1"
fi
PATH=$JAVA_HOME_25/bin:$PATH

if [ "$#" -ne "1" ]; then
  echo 'Check for duplicated documents in a database/index.'
  echo
  echo 'usage: SimilarDocs <configFile>'
  echo
  echo '<configFile>:'
  echo '   JSON configuration file containing producer, finder, comparators, and reporters.'
  exit 1
fi

cd /home/javaapps/sbt-projects/DeDup2 || exit

sbt "runMain dd.SimilarDocs $1"

cd - || exit
