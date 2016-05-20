package Datanode;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import com.google.protobuf.InvalidProtocolBufferException;

public class Datanode_server
{
  public static List<Integer> datanode_list = new ArrayList<Integer>();
  
  public static String datanode_ip="10.0.0.";
    
	public static void main(String args[]) throws IOException
	{	   
	   datanode_ip=datanode_ip+args[0]; 
	   System.out.println("Data Node Started at IP: "+ datanode_ip);
	   File f = new File("data_node_config"+args[0]);
	   if (f.exists())
	   {}
	   else {
		   f.createNewFile();
	   }
	   
	   //Setting up the datalist
	   BufferedReader br=null;
       br=new BufferedReader(new FileReader(f));
       String line;
       while((line=br.readLine()) != null)
       {
    	   int temp = Integer.parseInt(line);
    	   datanode_list.add(temp);
       }
       br.close();
	   
	   //Rest of the code
	   System.setProperty("java.rmi.server.hostname", datanode_ip);
	  
	   BlockReport_1 bl=new BlockReport_1();
	   Thread blthread=new Thread(bl);
	   blthread.start();
	   Heartbeat_1 hb=new Heartbeat_1();
	   Thread hbthread=new Thread(hb);
	   hbthread.start();
	   
	    try
	    {  
	     int port=5000+Integer.parseInt(args[0]);
	    IDataNode stub=new IDataNodeRemote();  
	    Registry registry= LocateRegistry.createRegistry(port);
	    registry.rebind("datanode"+args[0], stub);
	    }
	    catch(Exception e){System.out.println(e);} 
	    
	    }
}

class BlockReport_1 implements Runnable{
	@Override
	public void run() {
		// TODO Auto-generated method stub
		 //Creating Block Report Request 
	    while(true)
	    {
	     try {
			Thread.sleep(5000);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	    Hdfs.BlockReportRequest.Builder block_obj = Hdfs.BlockReportRequest.newBuilder();
	    
	    Hdfs.DataNodeLocation.Builder dn_obj = Hdfs.DataNodeLocation.newBuilder();
	    
	    int id=Integer.parseInt(Datanode_server.datanode_ip.substring(7));
	    block_obj.setId(id);
	    
	    for (int i=0; i<Datanode_server.datanode_list.size(); i++)
		{
			int a = Datanode_server.datanode_list.get(i);		
			block_obj.addBlockNumbers(a);
		}	
	    dn_obj.setIp(Datanode_server.datanode_ip);
	    dn_obj.setPort(id+5000);
	    dn_obj.build();
	    
	    block_obj.setLocation(dn_obj);
	    byte[] block_req_buff = new byte[2048];
	    block_req_buff = block_obj.build().toByteArray();
	   Namenode.INameNode stub_namenode=null;
	    try {
			stub_namenode=(Namenode.INameNode)Naming.lookup("rmi://10.0.0.1:5010/namenode");
		} catch (MalformedURLException e) 
		{e.printStackTrace();} 
	    catch (RemoteException e) {
	    	e.printStackTrace();
		} catch (NotBoundException e) {
			e.printStackTrace();
		}  
	    //Calling BlockReport Request - Namenode
	    byte[] block_resp_buff;
	    try {
			block_resp_buff = stub_namenode.blockReport(block_req_buff);
		} catch (RemoteException e) {
			e.printStackTrace();
		}
	    
	    
	    //Retreiving BlockReport Response from Namenode
	    Hdfs.BlockReportResponse block_resp_obj=null;
	    try {
			block_resp_obj = Hdfs.BlockReportResponse.parseFrom(block_req_buff);
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
		}
	    
	    int status = block_resp_obj.getStatus(0);
	  
	    
	}  

	}	
	
}

class Heartbeat_1 implements Runnable{

	//@Override
	public void run() {
		
		 while(true)
		 {
			 
		 try {
			Thread.sleep(5000);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		// TODO Auto-generated method stub
		Hdfs.HeartBeatRequest.Builder id=Hdfs.HeartBeatRequest.newBuilder();
		 int idnode=Integer.parseInt(Datanode_server.datanode_ip.substring(7));
		id.setId(idnode);
		byte[] inp=id.build().toByteArray();  
		 Namenode.INameNode stub_namenode=null;
		    try {
				stub_namenode=(Namenode.INameNode)Naming.lookup("rmi://10.0.0.1:5010/namenode");
			} catch (MalformedURLException e) 
			{e.printStackTrace();} 
		    catch (RemoteException e) {
		    	e.printStackTrace();
			} catch (NotBoundException e) {
				e.printStackTrace();
			}  
		    //Calling BlockReport Request - Namenode
		    byte[] heart_resp=null;
		    try {
				heart_resp = stub_namenode.blockReport(inp);
			} catch (RemoteException e) {
				e.printStackTrace();
			}
		    
		    Hdfs.HeartBeatResponse hbeat_resp_obj=null;
		    try {
				hbeat_resp_obj = Hdfs.HeartBeatResponse.parseFrom(heart_resp);
			} catch (InvalidProtocolBufferException e) {
				e.printStackTrace();
			}
		    
		    int status =hbeat_resp_obj.getStatus();
		    if(status ==1)
		    {
		    	
		    }
		 }
	}

	
	
	
}



