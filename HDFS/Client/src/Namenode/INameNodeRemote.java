package Namenode;
import java.io.BufferedInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.io.*;
import java.rmi.*;
import java.rmi.server.UnicastRemoteObject;

import org.w3c.dom.ls.LSInput;

import Namenode.Hdfs.AssignBlockRequest;
import Namenode.Hdfs.BlockLocationRequest;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

@SuppressWarnings("serial")
public class INameNodeRemote extends UnicastRemoteObject implements INameNode {
	
	INameNodeRemote() throws RemoteException
	{
		super();
	}
	
	/* OpenFileResponse openFile(OpenFileRequest) */
	/* Method to open a file given file name with read-write flag*/
	@SuppressWarnings("resource")
	public synchronized byte[] openFile(byte[] inp) throws RemoteException
	{
    	//Retreiving openfile request: filename and flag - Generating Handle
		Hdfs.OpenFileRequest openfile = null;
		try 
			{
			openfile=  Hdfs.OpenFileRequest.parseFrom(inp);
			} 
		catch (InvalidProtocolBufferException e) {e.printStackTrace();}
		int status = 1;
		String FileName = openfile.getFileName();
		boolean forRead=openfile.getForRead();
		 Hdfs.OpenFileResponse.Builder openfile_resp=Hdfs.OpenFileResponse.newBuilder();
	     openfile_resp.setHandle(namenode_server.handle_counter);
	     openfile_resp.setStatus(status);
	     byte[] open_resp_obj = new byte[1024];
	     namenode_server.hm.put(FileName, namenode_server.handle_counter++);
	    if(forRead==true)
		{
			//Setting Block Nums
			
			List<Integer> blocknos = new ArrayList<Integer>();
		    blocknos=namenode_server.namenode.get(FileName);
		    for(int i=0;i<blocknos.size();i++)
		    {
		      openfile_resp.addBlockNums(blocknos.get(i));	
		    }
		    					
		}
		open_resp_obj = openfile_resp.build().toByteArray();
	    return open_resp_obj;
			
	 }
		
	
	
	/* CloseFileResponse closeFile(CloseFileRequest) */
	
	public synchronized byte[] closeFile(byte[] inp ) throws RemoteException
	{
		int status=1;
		Hdfs.CloseFileRequest close_obj = null;
		try {
			close_obj = Hdfs.CloseFileRequest.parseFrom(inp);
		} catch (InvalidProtocolBufferException e) {
			status=0;
			e.printStackTrace();
		}	
		int handle=close_obj.getHandle();
		String filename="";		
		Set<String> keys = namenode_server.hm.keySet();
		
	    for(String key: keys)
	    	  	{
	    	      		if(namenode_server.hm.get(key)==handle)
	    				{
	    					filename=key;
	    					break;
	    				}
	    		
	    	  	}		
	
	 
		//Writing Name Node Configuration File
		String name_filename = "name_node_config";
		Writer writer_name = null;
		FileOutputStream fos=null;
		try {
			fos = new FileOutputStream(name_filename);
		} catch (FileNotFoundException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		OutputStreamWriter osw_name = new OutputStreamWriter(fos);	
		writer_name = new BufferedWriter(osw_name);
		try 
		{	
			Set<String> file_names = namenode_server.namenode.keySet();
			for (String fileName: file_names)
			{
				
				List<Integer> block_no_list = new ArrayList<Integer>();
				block_no_list = namenode_server.namenode.get(fileName);
				String file_temp = fileName+":";
				String block ="";
				for (int i=0; i<block_no_list.size(); i++)
				{
					String a = block_no_list.get(i).toString();		
					if (i==block_no_list.size()-1)
						block=block+a;
					else block=block+a+",";
				}			
				CharSequence csw_name = file_temp+block+"\n";
				writer_name.append(csw_name);
               
			}
		} 
		
		catch (FileNotFoundException e1) 
		{
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
		
	
		
		//Writing Block Report 
		String block_filename = "block_ip_temp";
		Writer writer = null;
		FileOutputStream block_file_obj=null;
		try {
			block_file_obj = new FileOutputStream(block_filename);
		} catch (FileNotFoundException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		OutputStreamWriter osw = new OutputStreamWriter(block_file_obj);	
		writer = new BufferedWriter(osw);
		
		try 
		{
		
			for (Integer blocknum: namenode_server.block_ip_temp.keySet())
			{
				List<Integer> value_list = new ArrayList<Integer>();
				value_list = namenode_server.block_ip_temp.get(blocknum);
				
				String templine=blocknum.toString()+":"+value_list.get(0).toString()+","+value_list.get(1).toString()+"\n";
				CharSequence csw = templine;
				writer.append(csw);	
			}
		} 
		
		catch (FileNotFoundException e1) 
		{
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e) {
				
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
		
		
		Hdfs.CloseFileResponse.Builder close_resp = Hdfs.CloseFileResponse.newBuilder();
		close_resp.setStatus(status);
		close_resp.build();
		     
		byte[] buffer=new byte[1024];
		buffer = close_resp.build().toByteArray();
				
		try {
			writer.close();
			writer_name.close();
		} catch (IOException e) {
			
			e.printStackTrace();
		}
		
		//Return		
		return buffer;
	}
	
	
	
	
	/* BlockLocationResponse getBlockLocations(BlockLocationRequest) */
	/* Method to get block locations given an array of block numbers */
	public byte[] getBlockLocations(byte[] inp ) throws RemoteException{
		Hdfs.BlockLocationRequest block_req=null;
		try {
			block_req=Hdfs.BlockLocationRequest.parseFrom(inp);
		} catch (InvalidProtocolBufferException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		List<Integer> blocknos = new ArrayList<Integer>();
	    blocknos=block_req.getBlockNumsList();
		
		Hdfs.BlockLocationResponse.Builder blockloc_resp=Hdfs.BlockLocationResponse.newBuilder();
		blockloc_resp.setStatus(1);
		
		for(int i=0;i<blocknos.size();i++)
		{
		   Hdfs.BlockLocations.Builder bloc_loc=Hdfs.BlockLocations.newBuilder();
		   bloc_loc.setBlockNumber(blocknos.get(i));
		   
		   //Setting Data Locations
		   String a="10.0.0.";
		   List<Integer> iplist = new ArrayList<Integer>();
		   iplist=namenode_server.blockreport.get(blocknos.get(i));	  
		   String a1=a+iplist.get(0);
		   String a2=a+iplist.get(1);
		   
		   Hdfs.DataNodeLocation.Builder data_loc1=Hdfs.DataNodeLocation.newBuilder();
		   int port1=5000+iplist.get(0);
		   int port2=5000+iplist.get(1);
		   data_loc1.setIp(a1);
		   data_loc1.setPort(port1);		   
		   bloc_loc.addLocations(data_loc1);
		   
		   Hdfs.DataNodeLocation.Builder data_loc2=Hdfs.DataNodeLocation.newBuilder();
			 
		   data_loc2.setIp(a2);
		   data_loc2.setPort(port2);
		   bloc_loc.addLocations(data_loc2);
		      
		   blockloc_resp.addBlockLocations(bloc_loc);
		}
		
		byte[] buffer=blockloc_resp.build().toByteArray();
		return buffer;
	}
	
	/* AssignBlockResponse assignBlock(AssignBlockRequest) */
	/* Method to assign a block which will return the replicated block locations */
	public synchronized byte[] assignBlock(byte[] inp ) throws RemoteException
	{
		//Retrieving the handle passed in
		//synchronized(namenode_server)
		namenode_server.block_no_counter++;
		
		AssignBlockRequest assign_req=null;
		try {
			assign_req = AssignBlockRequest.parseFrom(inp);
		} catch (InvalidProtocolBufferException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		int handle = assign_req.getHandle();
		
		//Initialising AssignBlockResponse
		Hdfs.AssignBlockResponse.Builder ass_response_object = Hdfs.AssignBlockResponse.newBuilder();
		ass_response_object.setStatus(1);
	    Hdfs.BlockLocations.Builder block_location=Hdfs.BlockLocations.newBuilder();
	    
	    
	    //Random Data Nodes Generation
	    Random x=new Random();	    
	    int dn1,dn2;
	    /*dn1=1;
	    dn2=2;*/
	   dn1=x.nextInt(4)+1;
		dn2=x.nextInt(4)+1;
		while(dn1==dn2)
			dn2=x.nextInt(4)+1;
	
		
		List<Integer> ip_list = new ArrayList<Integer>();
		//Create AssignResponse Object-Assigning DataNodeLocations for BlockNodes
	    for(int i=0;i<2;i++)
	    {
	    	Hdfs.DataNodeLocation.Builder datanode_location=Hdfs.DataNodeLocation.newBuilder();
	    	String ip = null;
	    	int port=0;
	    	if (i==0)
	    	{
	    		ip="10.0.0."+dn1;
	    		port=5000+dn1;
	    	}
	    	else
	    	{
	    		ip="10.0.0."+dn2;
	    		port=5000+dn2;
	    	}
	    	int ip_no=Integer.parseInt(ip.substring(7));
			ip_list.add(ip_no);
	    	
	    	datanode_location.setPort(port);
	    	datanode_location.setIp(ip);
	    	
	    	block_location.addLocations(datanode_location);
	    	
	    }
	
	    block_location.setBlockNumber(namenode_server.block_no_counter);
	    for(String file:namenode_server.hm.keySet())
	    {
	    	
	    	if(namenode_server.hm.get(file)==handle)
	    	{
	            if(namenode_server.namenode.containsKey(file))
	    			 {
	    			 namenode_server.namenode.get(file).add(namenode_server.block_no_counter);
	    			 }
	    		 else 
	    	        {
	    	        	List<Integer> some=new ArrayList<Integer>();
	    	        	some.add(namenode_server.block_no_counter);
	    	        	namenode_server.namenode.put(file, some);
	    	        }
	      	      break;
	    	}
	    }
	  
	 
	    ass_response_object.setNewBlock(block_location);
	    ass_response_object.build();    	//Setting AssignResponseObject
		
		byte[] assign_resp=new byte[2048];
		assign_resp=ass_response_object.build().toByteArray();
		
		namenode_server.block_ip_temp.put(namenode_server.block_no_counter, ip_list);		
		return assign_resp;
	}
	
	
	/* ListFilesResponse list(ListFilesRequest) */
	/* List the file names (no directories needed for current implementation */
	public byte[] list(byte[] inp ) throws RemoteException
	{
		Hdfs.ListFilesRequest list_req=null;
		try {
			 list_req=Hdfs.ListFilesRequest.parseFrom(inp);
		} catch (InvalidProtocolBufferException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Hdfs.ListFilesResponse.Builder list_resp=Hdfs.ListFilesResponse.newBuilder();
	    Set<String> file_name=namenode_server.namenode.keySet();
	    for(String filename:file_name)
	    {
	       list_resp.addFileNames(filename);
	    }
	    byte[] buffer=list_resp.build().toByteArray();
		return buffer;
	}
	
	/*
		Datanode <-> Namenode interaction methods
	*/
	
	/* BlockReportResponse blockReport(BlockReportRequest) */
	/* Get the status for blocks */
	public byte[] blockReport(byte[] inp ) throws RemoteException
	{
		Hdfs.BlockReportRequest block_req_obj=null;
		try {
			block_req_obj = Hdfs.BlockReportRequest.parseFrom(inp);
		} catch (InvalidProtocolBufferException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//Setting the blockreport hash values here	
		int ip=block_req_obj.getId();
		List<Integer> blocknos=new ArrayList<Integer>();
		blocknos=block_req_obj.getBlockNumbersList();
		
		for(int i=0;i<blocknos.size();i++)
		{
			Set<Integer> blockno_set=namenode_server.blockreport.keySet();
			boolean flag=false;
			for(Integer block:blockno_set)
			{
				if(block==blocknos.get(i))
				{
					flag=true;
				}
			}
			
			if(flag==true)
			{
				List<Integer> iplist=new ArrayList<Integer>();
			    iplist=namenode_server.blockreport.get(blocknos.get(i));
			    int size=iplist.size();
			    boolean flag_1=false;
			    for(int j=0;j<size;j++)
			    {
			    	if(iplist.get(j)==ip)
			    	{
			    		flag_1=true;
			    	}
			    }
			    if(flag_1==false)
			    {
			    	iplist.add(ip);
			    }
			    namenode_server.blockreport.put(blocknos.get(i), iplist);
			    
			}
			else if(flag ==false)
			{
				List<Integer> iplist=new ArrayList<Integer>();
				iplist.add(ip);
			    namenode_server.blockreport.put(blocknos.get(i), iplist);
			}
			flag=false;
		}
			
	    Hdfs.BlockReportResponse.Builder block_resp=Hdfs.BlockReportResponse.newBuilder();
		block_resp.addStatus(1);
		byte[] resp=block_resp.build().toByteArray();
		
		return resp;
	}
	
	/* HeartBeatResponse heartBeat(HeartBeatRequest) */
//	/* Heartbeat messages between NameNode and DataNode */
	public byte[] heartBeat(byte[] inp ) throws RemoteException{
		Hdfs.HeartBeatRequest hb1=null;
		try {
			 hb1=Hdfs.HeartBeatRequest.parseFrom(inp);
		} catch (InvalidProtocolBufferException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		int id=hb1.getId();
		String ip="10.0.0."+id;
		Hdfs.HeartBeatResponse.Builder resp=Hdfs.HeartBeatResponse.newBuilder();
		resp.setStatus(1);
		
		byte[] buffer=resp.build().toByteArray();
		return buffer;
	}

}