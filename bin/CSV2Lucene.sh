#!/usr/bin/env bash

JAVA_HOME="/home/users/operacao/.cache/coursier/arc/https/github.com/adoptium/temurin21-binaries/releases/download/jdk-21.0.6%252B7/OpenJDK21U-jdk_x64_linux_hotspot_21.0.6_7.tar.gz/jdk-21.0.6+7"
PATH="$JAVA_HOME/bin:$PATH"

cd /home/javaapps/sbt-projects/DeDup2 || exit

# shellcheck disable=SC2016
sbt 'runMain dd.tools.CSV2Lucene $1 $2 $3 $4 $5 $6 $7'

cd - || exit
