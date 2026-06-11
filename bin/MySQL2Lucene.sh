#!/bin/bash

if [ -z "$JAVA_HOME_25" ]; then
  JAVA_HOME_25="/home/users/operacao/.cache/coursier/arc/https/github.com/graalvm/graalvm-ce-builds/releases/download/jdk-25.0.1/graalvm-community-jdk-25.0.1_linux-x64_bin.tar.gz/graalvm-community-openjdk-25.0.1+8.1"
fi
export JAVA_HOME=$JAVA_HOME_25

export PATH=$JAVA_HOME/bin:$PATH

if [ "$#" -lt "7" ]
  then
    echo 'MySQL2Lucene shell takes a list of documents retrieved from a MySQL database and'
    echo 'creates a local DeDup index with them. If such index already exists, it will rewritten.'
    echo
    echo 'usage: MySQL2Lucene'
    echo '   -host=<MySQL server> - IP or domain of the MySQL server'
    echo '   -user=<MySQL user> - MySQL user name'
    echo '   -pswd=<MySQL password> - MySQL user password'
    echo '   -dbnm=<MySQL database> - MySQL database name'
    echo '   -sqls=<MySQL sql file list> - comma separated sql file names'
    echo '   -index=<indexPath> - NGrams Lucene index path'
    echo '   -schema=<schemaFile> - NGram schema file'
    echo
    echo '   [-port=<MySQL_Port>] - MySQL port'
    echo '   [-sqlEncoding=<sqlEncoding>] - sql file encoding. Default is utf-8'
    echo '   [-jsonField=<tag>[,<tag>,...,<tag>]] - if a column element is a json element, indicates which json elements'
    echo '                                          to retrieve the content. Default are text,_f,_e.'
    echo '   [-repetitiveField=<name>[.<name>,...,<name>]] - the name of the fields that should be broken into a new line'
    echo '                                                when the repetitive separator symbol is found. Default is title.'
    echo '   [-repetitiveSep=<separator>] - repetitive field string separator. Default is //@//'
    echo '   [-jsonLangField=<jsonLangField>] - the json field that store the language indicator. If present it will be'
    echo '                                      used to suffix the id field with the language.'
    echo '   [-idFieldName=<name>] - id field name of the mysql record]. Will be used to prefix the id with the language.'
    echo '                           if the jsonLangField is specified.'
    echo '   [-schemaEncoding=<schemaFileEncoding>] - NGram schema file encoding. Default is utf-8.'
    echo '   [--append] - append documents to an existing Lucene index.'

    exit 1
fi

cd /home/javaapps/sbt-projects/DCDup || exit

NOW=$(date +"%Y%m%d%H%M%S")

java -cp jar/DCDup-assembly-0.1.0.jar org.bireme.dcdup.MySQL2Pipe -pipe=pipeOut_$NOW.txt -jsonField=text,_f,_e,_u -repetitiveField=title $1 $2 $3 $4 $5 $6 $7 $8 $9 ${10} ${11} ${12} ${13} ${14} ${15} ${16}

if [ "$?" -ne 0 ]; then
  echo 'Pipe file generation error'
  exit 1
fi

#java -cp jar/DCDup-assembly-0.1.0.jar org.bireme.dcdup.Pipe2Lucene -pipe=pipeOut_$NOW.txt -pipeEncoding=utf-8 $1 $2 $3 $4 $5 $6 $7 $8 $9 ${10} ${11} ${12} ${13} ${14} ${15} ${16}
/home/users/operacao/.local/share/coursier/bin/sbt "runMain org.bireme.dcdup.Pipe2Lucene -pipe=pipeOut_$NOW.txt -pipeEncoding=utf-8 $1 $2 $3 $4 $5 $6 $7 $8 $9 ${10} ${11} ${12} ${13} ${14} ${15} ${16}"

#rm pipeOut_$NOW.txt

cd - || exit

