#!/bin/bash

if [ -z "$JAVA_HOME_25" ]; then
  JAVA_HOME_25="/home/users/operacao/.cache/coursier/arc/https/github.com/graalvm/graalvm-ce-builds/releases/download/jdk-25.0.1/graalvm-community-jdk-25.0.1_linux-x64_bin.tar.gz/graalvm-community-openjdk-25.0.1+8.1"
fi
export JAVA_HOME=$JAVA_HOME_25

export PATH=$JAVA_HOME/bin:$PATH

if [ "$#" -ne "1" ]
  then
    echo 'Check duplicated documents from a SQL result set using a temporary CSV/Lucene flow.'
    echo
    echo 'usage: SelfCheckDuplicated <configFile>'
    echo
    echo '<configFile>:'
    echo '   JSON configuration file containing producer/mysql or producer/csv, finder/lucene, comparators, and reporters.'
    exit 1
fi

cd /home/javaapps/sbt-projects/DeDup2/ || exit

printf -v quoted_args ' %q' "$@"
sbt "runMain dd.SelfCheckDuplicated$quoted_args"

cd - || exit
