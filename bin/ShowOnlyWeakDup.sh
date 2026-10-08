#!/usr/bin/env bash

if [ "$#" -ne 3 ]; then
  echo 'Filter weak duplicate pairs already present in the strong duplicate file.'
  echo
  echo 'usage: ShowOnlyWeakDup <strongDupPath> <weakDupPath> <outWeakDupPath>'
  exit 1
fi

cd /home/javaapps/sbt-projects/DeDup2 || exit 1

sbt -batch "runMain dd.tools.ShowOnlyWeakDup \"$1\" \"$2\" \"$3\""
status=$?

cd - || exit 1
exit "$status"
