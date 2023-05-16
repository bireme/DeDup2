#!/usr/bin/env bash

cd /home/javaapps/sbt-projects/DeDup2 || exit

sbt 'runMain dd.tools.CSV2Lucene $1 $2 $3 $4 $5 $6 $7'

cd - || exit
