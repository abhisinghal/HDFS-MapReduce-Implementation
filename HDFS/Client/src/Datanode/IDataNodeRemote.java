package Datanode;
import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.MalformedURLException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.*;  
import java.util.ArrayList;
import java.util.List;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

@SuppressWarnings("serial")
public class IDataNodeRemote extends UnicastRemoteObject implements IDataNode
{
	IDataNodeRemote()throws RemoteException
	{  
	super();  
	} 
	
	//Read Block Request/Response
	public byte[] readBlock(byte[] inp) throws RemoteException
	{
		Hdfs.ReadBlockRequest readloc_req=null;
		try {
			 readloc_req=Hdfs.ReadBlockRequest.parseFrom(inp);
		} catch (InvalidProtocolBufferException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		int blockno=readloc_req.getBlockNumber();
		String read_filename="Datanode"+blockno;
		File f1=new File(read_filename);
		FileInputStream f=null;
		try {
			 f=new FileInputStream(f1);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		int size=32*1024*1024;
		byte[] read_buffer= new byte[size];
		BufferedInputStream bis=new BufferedInputStream(f);
		int tmp=0;
		try {
			tmp=bis.read(read_buffer);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		ByteString buf=ByteString.copyFrom(read_buffer,0,tmp);
		Hdfs.ReadBlockResponse.Builder read_resp=Hdfs.ReadBlockResponse.newBuilder();
		read_resp.addData(buf);
		read_resp.setStatus(1);
		try {
			bis.close();
			f.close();
		} catch (IOException e) {
			
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		byte[] b=read_resp.build().toByteArray();
		
		return b;
	
	}
	
	
	//Write BlockRequest Response
	public byte[] writeBlock(byte[] inp) throws RemoteException
	{
		Hdfs.WriteBlockRequest write_block_obj= null;
		try {
			write_block_obj = Hdfs.WriteBlockRequest.parseFrom(inp);
		} catch (InvalidProtocolBufferException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		Hdfs.BlockLocations blockinfo = write_block_obj.getBlockInfo();	
	    byte[] data = write_block_obj.getData(0).toByteArray();
	    
	    //Updating list 
	    int block  = blockinfo.getBlockNumber();
	    Datanode_server.datanode_list.add(block);
		
		Integer blockno = blockinfo.getBlockNumber();
		String ip1 = blockinfo.getLocations(0).getIp();
		int port1 = blockinfo.getLocations(0).getPort();
		String ip2 = blockinfo.getLocations(1).getIp();
		int port2 = blockinfo.getLocations(1).getPort();
		int status =1;
		
		//Write to File
		String data_filename = "Datanode"+blockno;
		try {
			FileOutputStream data_file_obj = new FileOutputStream(data_filename);
			data_file_obj.write(data, 0, data.length);
			data_file_obj.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			status=0;
			e.printStackTrace();
		}
	//	String x="Datanode_server_"+machine_ip;
		String data_ip = Datanode_server.datanode_ip;
		
		String dn_no=data_ip.substring(7);
		
		//Appending to DataNOde configuration File
		String data_filename_config = "data_node_config"+dn_no;
		Writer writer = null;
		FileOutputStream data_file_obj=null;
		try {
			data_file_obj = new FileOutputStream(data_filename_config,true);
		} catch (FileNotFoundException e2) {
		// TODO Auto-generated catch block
		e2.printStackTrace();
		}
		OutputStreamWriter osw = new OutputStreamWriter(data_file_obj);	
		writer = new BufferedWriter(osw);
		String temp = blockno.toString()+"\n";
		
		CharSequence csw = temp;
		try {
			writer.append(csw);
			 writer.close();	
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}	
		
		  
	   if(ip1.equals(data_ip))
	   {
		  String datanode_no=ip2.substring(7);
			String addr="rmi://"+ip2+":"+port2+"/datanode"+datanode_no;
				
    		 
			 Datanode.IDataNode stub = null;
				try {
					
					stub = (Datanode.IDataNode)Naming.lookup(addr);
					stub.writeBlock(inp);
				} catch (NotBoundException e) {
					// TODO Auto-generated catch block
					status=0;
					e.printStackTrace();
				} catch (MalformedURLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} 
	   }
		
	   Hdfs.WriteBlockResponse.Builder write_resp_obj = Hdfs.WriteBlockResponse.newBuilder();
	   write_resp_obj.setStatus(status);   
		byte[] resp_buffer= new byte[1024];
		write_resp_obj.build();
		resp_buffer = write_resp_obj.build().toByteArray();	
		return resp_buffer;
	}
}