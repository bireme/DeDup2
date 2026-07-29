#!/bin/bash

if [ -z "$JAVA_HOME_25" ]; then
  JAVA_HOME_25="/home/users/operacao/.cache/coursier/arc/https/github.com/graalvm/graalvm-ce-builds/releases/download/jdk-25.0.1/graalvm-community-jdk-25.0.1_linux-x64_bin.tar.gz/graalvm-community-openjdk-25.0.1+8.1"
fi
export JAVA_HOME=$JAVA_HOME_25

export PATH=$JAVA_HOME/bin:$PATH

if [ "$#" -lt "7" ]
  then
    echo 'MySQL2Lucene shell takes a list of documents retrieved from a MySQL database and'
    echo 'creates a DeDup index with them. If such index already exists, it will rewritten.'
    echo
    echo 'usage: MySQL2Lucene <options>'
    echo 'options:'
    echo '	-mySqlHost=<host>       MySQL server host address'
    echo '	-mySqlUser=<str>        MySQL database user'
    echo '	-mySqlPassword=<str>    MySQL database password'
    echo '	-mySqlDbname=<str>      MySQL database name'
    echo '	-sqlfs=<name1>[,...,<nameN>] Comma-separated SQL statement files to execute sequentially.'
    echo '	                        Results from each file are appended to the same Lucene index during this run.'
    echo '	-index=<path>           Path to the index to be created'
    echo '	-fieldToIndex=<name>     Name of the field to be indexed'
    echo '	[-mySqlPort=<int>]      MySQL server port. Default is 3306.'
    echo '	[-importFields=(<fieldName>,...,<fieldName>|file=<path>)] Fields that will be written to the Lucene documents.'
    echo '	                         If absent, all fields returned by the SQL statements will be written.'
    echo '	[-jsonFieldFile=<path>] Path to a text file with JSON field mappings, one per line, using: <column name>=<json field name>[-><new field name>]'
    echo '	                        For mapped SQL columns, object fields are extracted from the JSON content and emitted with the configured new field names.'
    echo '	                        When <new field name> is omitted, the SQL column name is used as the output field name.'
    echo '	                        Missing JSON fields are ignored. JSON array values are grouped with '//@//'; arrays of non-objects keep the SQL column name.'
    echo '	[-splitDocumentField=<name>] JSON-array field whose occurrences are emitted as separate Lucene documents.'
    echo '	[-sqlEncoding=<str>]    SQL file character encoding. Default is "utf-8"'
    echo '	[-repetitiveField=<name>[,<name>,...,<name>]] Fields split into multiple documents when repetitiveSep is found.'
    echo '	[-repetitiveSep=<str>]  Separator used by repetitiveField. Default is "//@//".'

    exit 1
fi

cd /home/javaapps/sbt-projects/DeDup2 || exit

printf -v quoted_args ' %q' "$@"
sbt "runMain dd.tools.SQL2Lucene$quoted_args"

if [ "$?" -ne 0 ]; then
  echo 'Pipe file generation error'
  cd -
  exit 1
fi

cd - || exit
