#!/bin/bash

cd $PWD/TaskTracker
echo "Compiling......!!"

javac -classpath $PWD/mapred.jar -d bin src/*/*.java
 


if [ $# -ne 1 ]
then
	echo "Error : enter static ip no. as the argument 1/2/3/4.."
else

	ip=$1
	java -cp $PWD/mapred.jar:bin TaskTracker.TaskTracker_server $ip
	

fi


