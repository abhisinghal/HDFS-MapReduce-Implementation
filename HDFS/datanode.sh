#!/bin/bash

cd $PWD/Datanode
echo "Compiling......!!"

javac -d bin src/*/*.java


if [ $# -ne 1 ]
then
	echo "Error : enter static ip no. as the argument 1/2/3/4.."
else

	ip=$1
	java -cp $PWD/protobuf-java-2.5.0.jar:bin Datanode.Datanode_server $ip

fi


