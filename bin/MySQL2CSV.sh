#!/bin/bash

if [ -z "$JAVA_HOME_25" ]; then
  JAVA_HOME_25="/home/users/operacao/.cache/coursier/arc/https/github.com/graalvm/graalvm-ce-builds/releases/download/jdk-25.0.1/graalvm-community-jdk-25.0.1_linux-x64_bin.tar.gz/graalvm-community-openjdk-25.0.1+8.1"
fi
export JAVA_HOME=$JAVA_HOME_25

export PATH=$JAVA_HOME/bin:$PATH

if [ "$#" -lt "7" ]
  then
    echo "Export all MySQL result records into a csv file"
    echo "usage: MySQL2CSV <options>"
    echo "options:"
    echo "  -mySqlHost=<host>       MySQL server host address"
    echo "  -mySqlPort=<int>        MySQL server port"
    echo "  -mySqlUser=<str>        MySQL database user"
    echo "  -mySqlPassword=<str>    MySQL database password"
    echo "  -mySqlDbname=<str>      MySQL database name"
    echo "  -sqlfs=<name1>[,...,<nameN>] Comma-separated SQL statement files to execute sequentially."
    echo "	                             Results from each file are appended to the same output CSV during this run."
    echo "  -outCsvFile=<path>      Path to the output CSV file"
    echo "  [-fieldSeparator=<char>] Character indicating the field separator. Default value is ','."
    echo "	[-jsonFieldFile=<path>] Path to a text file with JSON field mappings, one per line, using: <column name>=<json field name>[-><new field name>]"
    echo "	                        For mapped SQL columns, object fields are extracted from the JSON content and emitted with the configured new field names."
    echo "	                        When <new field name> is omitted, the SQL column name is used as the output field name."
    echo "	                        Missing JSON fields are ignored. JSON array values are grouped with '//@//'; arrays of non-objects keep the SQL column name."
    echo "	[-splitDocumentField=<name>] JSON-array field whose occurrences are emitted as separate CSV records."
    echo "	[-sqlEncoding=<str>]    SQL file character encoding. Default is 'utf-8'"
    exit 1
fi

cd /home/javaapps/sbt-projects/DeDup2 || exit

sbt "runMain dd.tools.SQL2CSV $*"

cd - || exit
