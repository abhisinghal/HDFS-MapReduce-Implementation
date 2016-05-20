package Client_hdfs;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import Namenode.*;


import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;


public class client {
	
	public static void main(String[] args) throws NotBoundException, IOException
	{
		
	//List<Integer> block_list = new ArrayList<Integer>();
	Namenode.INameNode stub_namenode=(Namenode.INameNode)Naming.lookup("rmi://10.0.0.1:5010/namenode");  
    String list="";
	int len = args.length;
	if (len ==1)
	{
		list = args[0];
		if(list.equals("list"))
	    {
	    	Hdfs.ListFilesRequest.Builder listreq=Hdfs.ListFilesRequest.newBuilder();
	    	byte[]  list_inp=listreq.build().toByteArray();
	    	byte[] list_resp=stub_namenode.list(list_inp);
	    	Hdfs.ListFilesResponse listresp_obj=Hdfs.ListFilesResponse.parseFrom(list_resp);
	    	List<String> filelist=new ArrayList<String>();
	    	filelist=listresp_obj.getFileNamesList();
	    	for(int j=0;j<filelist.size();j++)
	    	{
	    		System.out.println(filelist.get(j));
	    	}
	    }
		return;
		
	}
	else if  (len==2)
	{

		 String filename = args[0];
	    String rw = args[1];
	    Boolean forRead = false;
	    if  (rw.equals("read"))
	    	forRead = true;
	    else forRead = false;  
	 
    //Call Open File Request for Read/Write
    Namenode.Hdfs.OpenFileRequest.Builder openfile = Namenode.Hdfs.OpenFileRequest.newBuilder();
    openfile.setFileName(filename);
    openfile.setForRead(forRead);
    byte[] inp = new byte[1024];
    inp = openfile.build().toByteArray();
    
    //Retreiving Open File Response : File handle and status
    byte[] open_file_resp;
    open_file_resp = stub_namenode.openFile(inp);
    Hdfs.OpenFileResponse openfile_resp_obj=null;
    try 
    	{
	    	openfile_resp_obj=Hdfs.OpenFileResponse.parseFrom(open_file_resp);
    	} 
    catch (InvalidProtocolBufferException e) 
		{
		e.printStackTrace();
		}

    int status=openfile_resp_obj.getStatus();
    int handle=openfile_resp_obj.getHandle();
    //In case we are handling failures
    /* if (status!=1)
     	System.out.println("Error is opening file");*/
     
        if(forRead==true)
    	{
    		//
        	 String output_filename = "output_"+filename;
        	    File f= new File(output_filename);    
        	    if(f.exists())
        	    	{
        	    	f.delete();
        	    	f.createNewFile();
        	    	}
        	//Fetching blocknos to the corresponding file
        	

    	    List<Integer> blocknos = new ArrayList<Integer>();
    	    blocknos=openfile_resp_obj.getBlockNumsList();
    	    
    		Hdfs.BlockLocationRequest.Builder blockloc_req=Hdfs.BlockLocationRequest.newBuilder();
    		for(int i=0;i<blocknos.size();i++)
    		{
    			blockloc_req.addBlockNums(blocknos.get(i));
    		}
    		inp = blockloc_req.build().toByteArray();
    	    
    	    byte[] blockreq_resp;
    	    
    	    //Calling GetBlockLocations - BlockLocationResponse
    	   blockreq_resp = stub_namenode.getBlockLocations(inp);
    	    
    	    Hdfs.BlockLocationResponse blockreq_resp_obj=null;
    	    try 
    	    	{
    		    	blockreq_resp_obj=Hdfs.BlockLocationResponse.parseFrom(blockreq_resp);
    	    	} 
    	    catch (InvalidProtocolBufferException e) 
    			{
    			e.printStackTrace();
    			}
    	    
    	      List<Hdfs.BlockLocations> blockinfo_obj=new ArrayList<Hdfs.BlockLocations>();
    	      blockinfo_obj=blockreq_resp_obj.getBlockLocationsList();
    	     
    	     
    	      FileOutputStream out=null;
    	       try {
				out=new FileOutputStream(f, true);
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	       
    	       for(int i=0;i<blockinfo_obj.size();i++)
    	       {
    	    	   int blockno=blockinfo_obj.get(i).getBlockNumber();
    	    	   String ip=blockinfo_obj.get(i).getLocations(0).getIp();
    	    	   int port=blockinfo_obj.get(i).getLocations(0).getPort();
    	    	   String datanode_no=ip.substring(7);
    	    	   String addr="rmi://"+ip+":"+port+"/datanode"+datanode_no;
    	    	   
    	    	   
    	    	   Hdfs.ReadBlockRequest.Builder readreq=Hdfs.ReadBlockRequest.newBuilder();
    	    	   readreq.setBlockNumber(blockno);
    	    	   byte[] inp1=readreq.build().toByteArray();
    	    	   
    	    	   Datanode.IDataNode stub_datanode_read = null;
   					try {
   						stub_datanode_read = (Datanode.IDataNode)Naming.lookup(addr);
   					} catch (NotBoundException e) {
   					// TODO Auto-generated catch block
   					e.printStackTrace();
   					} 
   				
    	    	   byte[] readblock_resp;
    	    	   
    	    	   readblock_resp=stub_datanode_read.readBlock(inp1);
    	    	   Hdfs.ReadBlockResponse readblocresp_obj=null;
    	    	   try {
    	    		   readblocresp_obj = Hdfs.ReadBlockResponse.parseFrom(readblock_resp);
    	    	   } catch (InvalidProtocolBufferException e) {
					// TODO Auto-generated catch block
    	    		   e.printStackTrace();
    	    	   }
    	    	   byte[] data=readblocresp_obj.getData(0).toByteArray();
    	    	   try {
					out.write(data,0,data.length);
    	    	   } catch (IOException e) {
					
					// TODO Auto-generated catch block
					e.printStackTrace();
    	    	   }
    	    	   
    	    	   
    	       } //Close of for loop
    	       
    	       
    	       try {
				out.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	    
    	}
      
    
    //Writing the file : Calling AssignBlock to namenode and write block to datanode
    if (forRead==false)
	{
    	 File f_read=new File(filename);
         FileInputStream fileinput=null;
    	try {
    		fileinput = new FileInputStream(f_read);
    	} catch (FileNotFoundException e2) {
    		// TODO Auto-generated catch block
    		e2.printStackTrace();
    	}
       
		int sizeofFiles=32*1024*1024;
    //	int sizeofFiles=128*1024;
		byte[] buffer=new byte[sizeofFiles];
		try(BufferedInputStream bis=new BufferedInputStream(fileinput))
		{
			int tmp=0;
			while((tmp=bis.read(buffer))>0)
			  {
									
				//Calling Assign block
				byte[] assg_block=new byte[1024];
				Hdfs.AssignBlockRequest.Builder assign=Hdfs.AssignBlockRequest.newBuilder();
				assign.setHandle(handle);
				assg_block = assign.build().toByteArray();
				
				
				byte[] assign_resp;
				
				assign_resp=stub_namenode.assignBlock(assg_block);
			    					
				//Retrieving data from assign_response
				 Hdfs.AssignBlockResponse assign_resp_obj=Hdfs.AssignBlockResponse.parseFrom(assign_resp); 
				 Hdfs.BlockLocations blockinfo= assign_resp_obj.getNewBlock();
				 int status_from_assign = assign_resp_obj.getStatus();
				 if (status_from_assign!=1)
					 {
					 System.out.println("Error in retrieving Assign BLock Response");
					 
					 }
				 
				int blockno = blockinfo.getBlockNumber();
				
				//Creating WriteBlockRequest					
				Hdfs.WriteBlockRequest.Builder write_block=Hdfs.WriteBlockRequest.newBuilder();
				write_block.setBlockInfo(blockinfo);
				
				
			//	ByteString buf=ByteString.copyFrom(buffer);
				ByteString buf=ByteString.copyFrom(buffer, 0, tmp);
				write_block.addData(buf);
				write_block.build();
				
				
				byte[] write_req = new byte[10000000];
				write_req = write_block.build().toByteArray();
				
				 
				String ip = blockinfo.getLocations(0).getIp();
				int port = blockinfo.getLocations(0).getPort();		
				
				String datanode_no=ip.substring(7);
				String addr="rmi://"+ip+":"+port+"/datanode"+datanode_no;
				
				Datanode.IDataNode stub_datanode = null;
				try {
					stub_datanode = (Datanode.IDataNode)Naming.lookup(addr);
				} catch (NotBoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} 
				
				byte[] write_resp;
				write_resp =  stub_datanode.writeBlock(write_req);
				 
				 //Retreiving Write_Block_Response
				 Hdfs.WriteBlockResponse write_obj = Hdfs.WriteBlockResponse.parseFrom(write_resp);
				 int status1 = write_obj.getStatus();
				 if (status1!=1)
				 {
					 System.out.println("Not able to write to DataNode");
				 }
		
			  }
			bis.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//Creating CloseFile Request
	    Hdfs.CloseFileRequest.Builder close_file_obj = Hdfs.CloseFileRequest.newBuilder();
	    close_file_obj.setHandle(handle);
	    byte[] close_buf = new byte[1024];
	    close_file_obj.build();
	    close_buf = close_file_obj.build().toByteArray();
	    
	    byte[] close_resp;
	    close_resp = stub_namenode.closeFile(close_buf);
	    
	    Hdfs.CloseFileResponse close_obj = null;
	    try {
			close_obj = Hdfs.CloseFileResponse.parseFrom(close_buf);
		} catch (InvalidProtocolBufferException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    
	    int status2 = close_obj.getStatus();
		
	}
    }
	}
	

}