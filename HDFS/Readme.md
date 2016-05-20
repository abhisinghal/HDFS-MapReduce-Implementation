***********************************Project - HDFS******************************************
___________________________________________________________________________________________

Submitted By-
Abhishek Singhal (201506572)
Megha Agarwal (201506511)

Subject - Distributed Systems

Course - M.Tech (CSIS) - IInd Semester
___________________________________________________________________________________________
================================== Client Interface =======================================

To Run:
bash client.sh filename operation.
Operation can be :
    write           : For PUT
    read            : For GET
    list            : For LIST

Keep the input files in the client folder only.
The output files will be generated while "GET" with the filename as "output_filename"
in the same folder.

___________________________________________________________________________________________
================================== Namenode Interface =====================================

To Run:
bash Namenode.sh


Namenode Server is statically binded to host 10.0.0.1 and port 5010

___________________________________________________________________________________________
================================== Datanode Interface =====================================

To run :
bash Datanode.sh ip_no
ip_no will be :
    1       : For Ip : 10.0.0.1 & Port : 5001
    2       : For Ip : 10.0.0.2 & Port : 5002
    3       : For Ip : 10.0.0.3 & Port : 5003
    4       : For Ip : 10.0.0.4 & Port : 5004
    
Note : Hearbeat is being sent by all at a gap of 5 sec.
       To print the Heartbeats recieved - A print statement is to be added.
       
____________________________________________________________________________________________
============================================================================================
