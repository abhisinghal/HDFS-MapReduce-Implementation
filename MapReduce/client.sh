#!/bin/bash

cd $PWD/Client
if [ $# -lt 5 ]
then
	echo "Usage: <mapName> <reducerName> <inputFilein HDFS> <outFile in HDFS> <numReducers>"
	exit
	
fi
echo "Compiling......!!"
javac -classpath $PWD/mapred.jar -d src/*/*.java



if [ $# -eq 5 ]
then
 	mapname=$1
	reducername=$2	
	inputfile=$3
	outputfile=$4
	no_of_reducers=$5
	java -classpath $PWD/mapred.jar:bin Client.client $1 $2 $3 $4 $5

fi
	
