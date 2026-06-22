#!/usr/bin/env bash

if [ -z "$JAVA_HOME_25" ]; then
  JAVA_HOME_25="/home/users/operacao/.cache/coursier/arc/https/github.com/graalvm/graalvm-ce-builds/releases/download/jdk-25.0.1/graalvm-community-jdk-25.0.1_linux-x64_bin.tar.gz/graalvm-community-openjdk-25.0.1+8.1"
fi
PATH=$JAVA_HOME_25/bin:$PATH

if [ "$#" -lt "4" ]
  then
    echo 'Command-line utility that converts CSV input into a Lucene index.'
    echo 'usage: CSV2Lucene <options>'
    echo 'options:'
    echo '  -csvFile=<path> Path to the comma separated value file (csv)'
    echo '  -index=<path> Path to the index to be created'
    echo '  -schema=(<pos>=<fieldName>,...,<pos>=<fieldName>|file=<path>) Associate the csv field position (start with 0)'
    echo '    with the Lucene document field name. If the parameter starts with file= then the corresponding schema file path will be used.'
    echo '  -fieldToIndex=<name> Name of the field to be indexed'
    echo '  [-fieldSeparator=<char>] Character indication the field separator. Default value is ",".'
    echo '  [-encoding=<str>] The csv character encoding. Default value is "utf-8"'
    echo '  [--hasHeader] If present indicates the csv file has header.""".stripMargin'
fi

cd /home/javaapps/sbt-projects/DeDup2 || exit

sbt "runMain dd.tools.CSV2Lucene '$1' '$2' '$3' '$4' '$5' '$6' '$7'"

cd - || exit
