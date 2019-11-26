#!/bin/bash

if [ $# -lt 1 ]; then
echo "Usage: ./prepare_data (depth of directory)"
echo "Depth of directory : Number of files"
echo "1 : 100"
echo "2 : 10000"
echo "3 : 1000000"
echo "4 : 100000000"
fi

if [ $1 -gt 4 ]; then
echo "Max depth is: 4 "
exit;
fi

depth=$1
#currentdir=/udmtest

echo "dir depth is $depth"

echo "Start generating data"

function createfile() {
  filepath=$1
  echo "file path is $filepath"
  #alluxio fs touch $filepath
}

function createdir () {

  currentdepth=$1
  currentdir=${currentdir}"$2"
  echo "currentdir is $currentdir"

  if [ $currentdepth -lt $depth ]; then
    for i in `seq 1 10`;   
    do
      #alluxio fs mkdir ${currentdepth}"/$i";
      tmpdir=${currentdepth}"/$i"
      echo "mkdir $tmpdir"
      createdir $currentdepth "/$i"
    done

  fi
  
}

createdir 1 /udmtest
