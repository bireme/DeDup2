#!/usr/bin/env bash

cd /home/javaapps/sbt-projects/DeDup2 || exit

sbt 'runMain dd.SimilarDocs $1 $2 $3 $4 $5 $6 $7 $8 $9'

cd - || exit
