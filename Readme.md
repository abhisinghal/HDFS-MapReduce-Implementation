#Project - HDFS
=================
Submitted By- Abhishek Singhal (201506572) Megha Agarwal (201506511)
Subject - Distributed Systems
Course - M.Tech (CSIS) - IInd Semester

##Client Interface 
To Run: bash client.sh filename operation. Operation can be : write : For PUT read : For GET list : For LIST
Keep the input files in the client folder only. The output files will be generated while "GET" with the filename as "output_filename" in the same folder.

=====================
##Namenode Interface
To Run: bash Namenode.sh
Namenode Server is statically binded to host 10.0.0.1 and port 5010

======================
##Datanode Interface

To run : bash Datanode.sh ip_no ip_no will be : 1 : For Ip : 10.0.0.1 & Port : 5001 2 : For Ip : 10.0.0.2 & Port : 5002 3 : For Ip : 10.0.0.3 & Port : 5003 4 : For Ip : 10.0.0.4 & Port : 5004
Note : Hearbeat is being sent by all at a gap of 5 sec. To print the Heartbeats recieved - A print statement is to be added.

======================


#Project Part 2- MAPREDUCE
=======================================

#Client Interface 
To Run: bash client.sh filename MapName Reducename inputfile outfile noofreducers

======================
#JobTracker Interface 
To Run: bash jobtracker.sh
JobTracker Server is statically binded to host 10.0.0.1 and port 5020

======================
#TaskTracker Interface 
To run : bash TaskTracker.sh ip_no ip_no will be : 1 : For Ip : 10.0.0.1 & Port : 5021 2 : For Ip : 10.0.0.2 & Port : 5022 3 : For Ip : 10.0.0.3 & Port : 5023 4 : For Ip : 10.0.0.4 & Port : 5024
