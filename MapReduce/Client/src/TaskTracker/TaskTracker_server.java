package TaskTracker;

import java.io.BufferedInputStream;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import Client_hdfs.Hdfs;
import Client_hdfs.Hdfs.BlockLocations;
import JobTracker.IJobTracker;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

public class TaskTracker_server 
{
	public static int tasktrackerip;
	public String word=null;	
	public static int no_of_map_slot_free;
	public static int no_of_reduce_slot_free;
	public static boolean reduceflag=false;
	
	public static HashMap<Integer,Integer> taskcompletedhash = new HashMap<Integer,Integer>();
	public static HashMap<Integer,Integer> reducetaskcompletedhash = new HashMap<Integer,Integer>();
	public static boolean finalflag=false;

	public static void main(String args[])
		{
		
		System.setProperty("java.rmi.server.hostname","10.0.0"+args[0]);
		System.out.println("Task Tracker Server started at 10.0.0."+args[0]);
		tasktrackerip=Integer.parseInt(args[0]);
		try{  
		    ITaskTracker stub_job=new ITaskTrackerRemote(); 
		    int port_task = 5000+20+Integer.parseInt(args[0]);
		    Registry registry=LocateRegistry.createRegistry(port_task);
		    registry.rebind("jobtracker", stub_job);  
		    }catch(Exception e){System.out.println(e);}  

		no_of_map_slot_free=5;
		no_of_reduce_slot_free=5;
		HeartReq_thread heart=new HeartReq_thread();
		Thread hp=new Thread(heart);
		hp.start();
		}	
}

class HeartReq_thread implements Runnable
{
	@SuppressWarnings("unchecked")
	@Override
	public synchronized void run() 
	{
		
	  ExecutorService executor1 = Executors.newFixedThreadPool(5);
	  ExecutorService executor2 = Executors.newFixedThreadPool(5);
	  ArrayList<Future<String>> list = new ArrayList<Future<String>>();
	  ArrayList<Future<String>> list_red = new ArrayList<Future<String>>();
	  
	  boolean flag =true;
	  ArrayList<byte[]> map_statuslist=new ArrayList<byte[]>();
	  ArrayList<byte[]> red_statuslist=new ArrayList<byte[]>();
	  while(true)
	  {
		  try {
			Thread.sleep(3000);
		} catch (InterruptedException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		    MapReduce.HeartBeatRequest.Builder heartreq=MapReduce.HeartBeatRequest.newBuilder();
			heartreq.setTaskTrackerId(TaskTracker_server.tasktrackerip);
			heartreq.setNumMapSlotsFree(TaskTracker_server.no_of_map_slot_free);
			heartreq.setNumReduceSlotsFree(TaskTracker_server.no_of_reduce_slot_free);
			    
		if(flag==true)
		{
			flag=false;
			MapReduce.MapTaskStatus.Builder obj=MapReduce.MapTaskStatus.newBuilder();
			obj.setJobId(0);
			obj.setTaskId(0);
			obj.setMapOutputFile("");
			obj.setTaskCompleted(false);
			byte[] s = obj.build().toByteArray();
			map_statuslist.add(s);
			heartreq.addMapStatus(obj);
			
					
		}
		
		if(TaskTracker_server.reduceflag==false)//We have to write a logic such that if maptasks are completed then this loop should 
	    {
	        if(map_statuslist.size()>1)
	        {
	    	for(int i=1;i<map_statuslist.size();i++)
	    	{
	 		MapReduce.MapTaskStatus mapstatus=null;
	    		try
	    		{
	    			mapstatus = MapReduce.MapTaskStatus.parseFrom(map_statuslist.get(i));} 
	    		catch (InvalidProtocolBufferException e) {
	    			e.printStackTrace();
	    		}
	    		int jobid = mapstatus.getJobId();
	    		int taskid = mapstatus.getTaskId();
	    		boolean tascom = mapstatus.getTaskCompleted();
	    		String outputfle = mapstatus.getMapOutputFile();
	    		MapReduce.MapTaskStatus.Builder mapobj = MapReduce.MapTaskStatus.newBuilder();
	    		mapobj.setJobId(jobid);
	    		mapobj.setTaskId(taskid);
	    		mapobj.setMapOutputFile(outputfle);
	    		mapobj.setTaskCompleted(tascom);   		
	       		heartreq.addMapStatus(mapobj);	
	    	}
	        }
	    }
		
	   
		for(int i=0;i<red_statuslist.size();i++)
		{
			MapReduce.ReduceTaskStatus redstatus=null;
			try {
				 redstatus=MapReduce.ReduceTaskStatus.parseFrom(red_statuslist.get(i));
			} catch (InvalidProtocolBufferException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			heartreq.addReduceStatus(redstatus);
		}
	
		byte[] heartreq_obj = heartreq.build().toByteArray();
		JobTracker.IJobTracker stub_ijob = null;
		try {
			stub_ijob = (JobTracker.IJobTracker)Naming.lookup("rmi://10.0.0.1:5020/jobtracker");
		} catch (MalformedURLException | RemoteException | NotBoundException e1) {
			e1.printStackTrace();
		}	
		
		byte[] heartres_obj=null;	
		try {
			heartres_obj = stub_ijob.heartBeat(heartreq_obj);
		} catch (RemoteException e) {
			
			e.printStackTrace();
		}
	
		
		//Getting the Heartbeat Response
		MapReduce.HeartBeatResponse heartbeat_res = null;
		try {
			heartbeat_res=MapReduce.HeartBeatResponse.parseFrom(heartres_obj);
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
		}	
		int resp_status=heartbeat_res.getStatus();
		int sizeofmaptask=heartbeat_res.getMapTasksList().size();
		int sizeofreducetask=heartbeat_res.getReduceTasksList().size();
		int flagforsizemap=0;
		if(sizeofmaptask>0)
			{
			  
			  flagforsizemap=1;
			 // map_statuslist.clear();
			  for(int i=0;i<sizeofmaptask;i++)
			  {
				  int blockno = heartbeat_res.getMapTasks(i).getInputBlocks(0).getBlockNumber();
				  String ip = heartbeat_res.getMapTasks(i).getInputBlocks(0).getLocations(0).getIp();
				  int port = heartbeat_res.getMapTasks(i).getInputBlocks(0).getLocations(0).getPort();
				  int taskid = heartbeat_res.getMapTasks(i).getTaskId();
				  if(TaskTracker_server.taskcompletedhash.containsKey(taskid))
				  {}
				  else
				  {
					  TaskTracker_server.taskcompletedhash.put(taskid, 0);
				  }
				  int jobid = heartbeat_res.getMapTasks(i).getJobId();
				  String outputfile = "job_"+jobid+"_map_"+taskid;
				  MapReduce.MapTaskStatus.Builder obj=MapReduce.MapTaskStatus.newBuilder();
				  obj.setJobId(jobid);
				  obj.setTaskId(taskid);
				  obj.setMapOutputFile(outputfile);
				  obj.setTaskCompleted(false);
				  byte[] s = obj.build().toByteArray();
			      map_statuslist.add(s);
			      
			      Runnable worker = (Runnable) new MapTask(blockno,ip ,port, taskid, jobid, outputfile); 
				  executor1.execute(worker);
											  
			  }	
			  
			  //FUTURE LIST OF MAP TASK
               }
				 
				 
		else if (sizeofreducetask>0)
			{
			 if(TaskTracker_server.reduceflag==false)
			 {
			   try {
				Thread.sleep(15000);
			 } catch (InterruptedException e) {
			 	// TODO Auto-generated catch block
				e.printStackTrace();
			}
			 }
			  TaskTracker_server.finalflag=true;
				TaskTracker_server.reduceflag = true;
				map_statuslist.clear();
				TaskTracker_server.taskcompletedhash.clear();
				for(int i=0;i<sizeofreducetask;i++)
			    //for(int i=0;i<1;i++)
				{
				  
				  int taskid = heartbeat_res.getReduceTasks(i).getTaskId();
				  if(TaskTracker_server.reducetaskcompletedhash.containsKey(taskid))
				  {}
				  else
				  {
					  TaskTracker_server.reducetaskcompletedhash.put(taskid, 0);
				  }
				  int jobid = heartbeat_res.getReduceTasks(i).getJobId();
				  String outputfile = heartbeat_res.getReduceTasks(i).getOutputFile();
				  List<String> mapfilelist=new ArrayList<String>();
				  mapfilelist=heartbeat_res.getReduceTasks(i).getMapOutputFilesList();
				  
				  MapReduce.ReduceTaskStatus.Builder obj=MapReduce.ReduceTaskStatus.newBuilder();
				  obj.setJobId(jobid);
				  obj.setTaskId(taskid);
				  
				  obj.setTaskCompleted(false);
				  byte[] s = obj.build().toByteArray();
			      red_statuslist.add(s);
			      
			    
				  Runnable worker = (Runnable) new ReduceTask(jobid, taskid ,outputfile, mapfilelist); 
				  executor2.execute(worker);
				 						  
			  }	
			
	}
		if(sizeofreducetask==0  && TaskTracker_server.reduceflag==false)
		{
			  int i=0;	
			  int flag1=0;
			  {
				   flag1=0;
				 
		     //     if (!TaskTracker_server.taskcompletedhash.isEmpty())
		          	{
		             for(Integer taskids:TaskTracker_server.taskcompletedhash.keySet())
		             { 
		            ///	 System.out.println("HI I am inside taskcompleted hask");
		            	 flag1=0;
		            	 int current_tid = taskids;
				         	if(TaskTracker_server.taskcompletedhash.get(current_tid)==2)
				         	{
				         			flag1=1;	
				         	}
				         	if(flag1!=1 && (TaskTracker_server.taskcompletedhash.get(current_tid)==1))
				         	{
				         		TaskTracker_server.taskcompletedhash.put(taskids, 2);//To set the above flag
				         		TaskTracker_server.no_of_map_slot_free+=1;
				         		for (int k=1; k< map_statuslist.size(); k++)
				         	
				          		{
				         		 MapReduce.MapTaskStatus mapstatus=null;
								try {
									mapstatus = MapReduce.MapTaskStatus.parseFrom(map_statuslist.get(k));
									} catch (InvalidProtocolBufferException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
									}
				         		int taskid = mapstatus.getTaskId();
				         		if (taskid==current_tid)
				        	  		{
				         			int jobid = mapstatus.getJobId();
				         			int taskid_cur = mapstatus.getTaskId();
				         			String out_file = mapstatus.getMapOutputFile();
				         			map_statuslist.remove(k);        	
				        		 
				         			MapReduce.MapTaskStatus.Builder obj=MapReduce.MapTaskStatus.newBuilder();
				         			obj.setJobId(jobid);
				         			obj.setTaskId(taskid_cur);
				         			obj.setMapOutputFile(out_file);
				         			obj.setTaskCompleted(true);
				         			byte[] buffer = obj.build().toByteArray();
				         			map_statuslist.add(buffer);     
				         			//TaskTracker_server.taskcompletedhash.put(current_tid, 0);
				        	  		}        	  	        	  
				          		}
				         	}
		             }
		            } 
		          
				 }
		}
		
		if(TaskTracker_server.reduceflag == true )
		{
			
			int i=0;
			int flag1=0;  
			 for(Integer taskids:TaskTracker_server.reducetaskcompletedhash.keySet())
			    {
				flag1=0;
				int current_tid = taskids;		
				if(TaskTracker_server.reducetaskcompletedhash.get(current_tid)==2)
				     flag1=1;		            
				if(flag1!=1 && (TaskTracker_server.reducetaskcompletedhash.get(current_tid)==1))
				      {   
				      TaskTracker_server.reducetaskcompletedhash.put(current_tid,2);
				      TaskTracker_server.no_of_reduce_slot_free+=1;
				      for (int k=0; k< red_statuslist.size(); k++)
				         {
				           MapReduce.ReduceTaskStatus redstatus=null;
				           try {
				        	   redstatus = MapReduce.ReduceTaskStatus.parseFrom(red_statuslist.get(k));
								}
				           catch (InvalidProtocolBufferException e) {
				        	   e.printStackTrace();
							}
				      int taskid = redstatus.getTaskId();
				      if (taskid==current_tid)
				      	{
				    	  
				    	  int jobid = redstatus.getJobId();
				    	  int taskid_cur = redstatus.getTaskId();
				    	  red_statuslist.remove(k);        			        		 
				    	  MapReduce.ReduceTaskStatus.Builder obj=MapReduce.ReduceTaskStatus.newBuilder();
				    	  obj.setJobId(jobid);
				    	  obj.setTaskId(taskid_cur);
				    	  obj.setTaskCompleted(true);
				    	  byte[] buffer = obj.build().toByteArray();
				    	  red_statuslist.add(buffer);    
				    	 
				        
				        }        	  	        	  
			  		}
				 }
			 }		 
			 
			 
			 //If everything is 2			 
			 if ((sizeofreducetask==0) && (TaskTracker_server.finalflag==true))
				{
				TaskTracker_server.reduceflag=false;
				TaskTracker_server.taskcompletedhash.clear();
				TaskTracker_server.reducetaskcompletedhash.clear();
				TaskTracker_server.finalflag=false;
				}
			
		}	
	}
}  
}



class MapTask implements Runnable
  { 
	int taskid;
    int jobid;
	String ip;
	int port;
	int blockno;
	String outputfile;
	
	public static String word=null;	
	MapTask(int blockno, String ip, int port,int taskid,int jobid, String outputfile)
	{
		this.ip = ip;
		this.port = port;
		this.blockno = blockno;
		this.taskid=taskid;
		this.jobid=jobid;
		this.outputfile = outputfile;
		
		
	}
	

	@Override
	public void run() {
		TaskTracker_server.no_of_map_slot_free--;
		//HashMap<Integer,String> taskcomp=new HashMap<Integer,String>();
		
		//Setting the word        
        BufferedReader br=null;
		try {
			br = new BufferedReader(new FileReader("word.txt"));
		} catch (FileNotFoundException e3) {
			// TODO Auto-generated catch block
			e3.printStackTrace();
		}
        try {
			word = br.readLine().toString();
		} catch (IOException e3) {
			// TODO Auto-generated catch block
			e3.printStackTrace();
		}
        
        //Calling Datanode to get the data
        int ipofserver=Integer.parseInt(ip.substring(7));
		String ip1="10.0.0."+ipofserver+":"+port;
		String addr="rmi://"+ip1+"/datanode"+ipofserver;
		Datanode.IDataNode stub_datanode=null;
		try {
			stub_datanode=(Datanode.IDataNode)Naming.lookup(addr);
		} catch (MalformedURLException | RemoteException | NotBoundException e) {
			e.printStackTrace();
		}
		Hdfs.ReadBlockRequest.Builder readobj=Hdfs.ReadBlockRequest.newBuilder();
		readobj.setBlockNumber(blockno);
		byte[] resp=null;
		try {
			resp=stub_datanode.readBlock(readobj.build().toByteArray());
			} catch (RemoteException e) {
			e.printStackTrace();
			}
		Hdfs.ReadBlockResponse readres=null;
		try {
			 readres=Hdfs.ReadBlockResponse.parseFrom(resp);
		} catch (InvalidProtocolBufferException e) 
			{
			// TODO Auto-generated catch block
			e.printStackTrace();
			}
		byte[] data=readres.getData(0).toByteArray();
		
		InputStream is = null;
		BufferedReader bfReader = null;
		is = new ByteArrayInputStream(data);
		bfReader = new BufferedReader(new InputStreamReader(is));
		String temp = null;
		
	//	IMapperClass mapper_obj = new IMapperClass();
		IMapper mapper_obj=null;
		try {
			mapper_obj = (IMapper)Class.forName("TaskTracker.IMapperClass").newInstance();
		} catch (InstantiationException | IllegalAccessException
				| ClassNotFoundException e3) {
			// TODO Auto-generated catch block
			e3.printStackTrace();
		}
		
		//Creating output File
		File out_file = new File(this.outputfile);
		Writer writer_name = null;
		FileOutputStream fos=null;
		try 
			{
			fos = new FileOutputStream(out_file);
			} 
		catch (FileNotFoundException e2) 
			{
			e2.printStackTrace();
			}
		OutputStreamWriter osw_name = new OutputStreamWriter(fos);	
		writer_name = new BufferedWriter(osw_name);
		try {
			while((temp = bfReader.readLine()) != null)
				{
				String valid = mapper_obj.map(temp);
				if(valid!=null)
					{
					CharSequence csw_name = valid+"\n";
					writer_name.append(csw_name);
					}
				
				}
			} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			 
			writer_name.close();
			fos.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
		
		//WRITING ON THE HDFS
				Namenode.INameNode stub_namenode=null;
				try {
					stub_namenode = (Namenode.INameNode)Naming.lookup("rmi://10.0.0.1:5010/namenode");
				} catch (MalformedURLException | RemoteException
						| NotBoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}  
				
				//Call Open File Request for Read/Write
				    Namenode.Hdfs.OpenFileRequest.Builder openfile = Namenode.Hdfs.OpenFileRequest.newBuilder();
				    openfile.setFileName(outputfile);
				    openfile.setForRead(false);
				    byte[] inp =openfile.build().toByteArray();
				  
				    
				    //Retreiving Open File Response : File handle and status
				    byte[] open_file_resp=null;
				    try {
						open_file_resp = stub_namenode.openFile(inp);
					} catch (RemoteException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				    Hdfs.OpenFileResponse openfile_resp_obj=null;
				    try 
				    	{
					    	openfile_resp_obj=Hdfs.OpenFileResponse.parseFrom(open_file_resp);
				    	} 
				    catch (InvalidProtocolBufferException e1) 
						{
						e1.printStackTrace();
						}

				    int status=openfile_resp_obj.getStatus();
				    int handle=openfile_resp_obj.getHandle();
				  //Writing the file : Calling AssignBlock to namenode and write block to datanode
				   File f_read1=new File(outputfile);
				    FileInputStream fileinput1=null;
				    try {
				    		fileinput1 = new FileInputStream(f_read1);
				    	} catch (FileNotFoundException e2) {
				    		// TODO Auto-generated catch block
				    		e2.printStackTrace();
				    	}
				       
						int sizeofFiles=32*1024*1024;
				    //	int sizeofFiles= 128*1024;				    
						byte[] buffer1=new byte[sizeofFiles];
						try(BufferedInputStream bis=new BufferedInputStream(fileinput1))
						{
							int tmp=0;
							while((tmp=bis.read(buffer1))>0)
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
								ByteString buf=ByteString.copyFrom(buffer1, 0, tmp);
								write_block.addData(buf);
								write_block.build();
								
								
								byte[] write_req = new byte[10000000];
								write_req = write_block.build().toByteArray();
								
								 
								String ip = blockinfo.getLocations(0).getIp();
								int port = blockinfo.getLocations(0).getPort();		
								
								int datanode_no=Integer.parseInt(ip.substring(7));
								String addr1="rmi://"+ip+":"+port+"/datanode"+datanode_no;
								
								Datanode.IDataNode stub_datanode1 = null;
								try {
									stub_datanode1 = (Datanode.IDataNode)Naming.lookup(addr1);
								} catch (NotBoundException e1) {
									// TODO Auto-generated catch block
									e1.printStackTrace();
								} 
								
								byte[] write_resp;
								write_resp =  stub_datanode1.writeBlock(write_req);
								 
								 //Retreiving Write_Block_Response
								 Hdfs.WriteBlockResponse write_obj = Hdfs.WriteBlockResponse.parseFrom(write_resp);
								 int status1 = write_obj.getStatus();
								 if (status1!=1)
								 {
									 System.out.println("Not able to write to DataNode");
								 }
						
							  }
							bis.close();
						} 
						
						catch (FileNotFoundException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						} catch (IOException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}
						
						//Creating CloseFile Request
					    Hdfs.CloseFileRequest.Builder close_file_obj = Hdfs.CloseFileRequest.newBuilder();
					    close_file_obj.setHandle(handle);
					    byte[] close_buf = new byte[1024];
					    close_file_obj.build();
					    close_buf = close_file_obj.build().toByteArray();
					    
					    byte[] close_resp;
					    try {
							close_resp = stub_namenode.closeFile(close_buf);
						} catch (RemoteException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					    
					    Hdfs.CloseFileResponse close_obj = null;
					    try {
							close_obj = Hdfs.CloseFileResponse.parseFrom(close_buf);
						} catch (InvalidProtocolBufferException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}
					    
					    
	    TaskTracker_server.taskcompletedhash.put(taskid, 1);
	   
	}
	

}


//REDUCE THREAD
class ReduceTask implements Runnable 
{ 
	int taskid;
    int jobid;
	String outputfile;
	List<String> mapoutputfiles=new ArrayList<String>();
	
	public static String word=null;	
	ReduceTask(int jobid,int taskid, String outputfile,List<String> outputfiles)
	{
		this.taskid=taskid;
		this.jobid=jobid;
		this.outputfile = outputfile;
		this.mapoutputfiles=outputfiles;	
	}
	
	@Override
	public synchronized void run() {
		// TODO Auto-generated method stub
		TaskTracker_server.no_of_reduce_slot_free--;
		HashMap<Integer,String> taskcomp=new HashMap<Integer,String>();
		String output_filename = outputfile;
		List<String> files = new ArrayList<String>();
		for(int y=0; y<mapoutputfiles.size(); y++)
		{
			String file = mapoutputfiles.get(y);
			files.add(file);
		}
		File f= new File(output_filename);    
   	    if(f.exists())
   	    	{
   	    	f.delete();
   	    	try {
				f.createNewFile();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
   	    	}
   	    FileOutputStream fout=null;
		try {
			fout = new FileOutputStream(f,true);
		} catch (FileNotFoundException e5) {
			// TODO Auto-generated catch block
			e5.printStackTrace();
		}
   	   BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fout));
      Namenode.INameNode stub_namenode=null;
	  try {
		stub_namenode = (Namenode.INameNode)Naming.lookup("rmi://10.0.0.1:5010/namenode");
	  } catch (MalformedURLException | RemoteException
			| NotBoundException e2) {
		// TODO Auto-generated catch block
		e2.printStackTrace();
	  }  
		//for (int x=0; x<mapoutputfiles.size(); x++)
	    for(int x=0;x<files.size();x++)
			{
			
		    //Call Open File Request for Read/Write
		    Namenode.Hdfs.OpenFileRequest.Builder openfile = Namenode.Hdfs.OpenFileRequest.newBuilder();
		    openfile.setFileName(files.get(x));
		    openfile.setForRead(true);
		    byte[] inp = openfile.build().toByteArray();
		    
		    //Retreiving Open File Response : File handle and status
		    byte[] open_file_resp=null;
		    try {
				open_file_resp = stub_namenode.openFile(inp);
			} catch (RemoteException e2) {
				e2.printStackTrace();
			}
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
			
			List<Integer> blocknos=new ArrayList<Integer>();
		    blocknos=openfile_resp_obj.getBlockNumsList();
			//Fetching blocknos to the corresponding file
			Namenode.Hdfs.BlockLocationRequest.Builder blockloc_req=Namenode.Hdfs.BlockLocationRequest.newBuilder();
			for(int i=0;i<blocknos.size();i++)
				{
				//  blockloc_req.addBlockNums(op)
				  //int blockno=openfile_resp_obj.getBlockNums(i);
				  blockloc_req.addBlockNums(blocknos.get(i));
				}
			byte[] inp1 = blockloc_req.build().toByteArray();
   	    
			byte[] blockreq_resp=null;
			
			 Namenode.INameNode stub_namenode1=null;
			  try {
				  stub_namenode1 = (Namenode.INameNode)Naming.lookup("rmi://10.0.0.1:5010/namenode");
			  } catch (MalformedURLException | RemoteException
					| NotBoundException e2) {
				// TODO Auto-generated catch block
				e2.printStackTrace();
			  }  
   	    //Calling GetBlockLocations - BlockLocationResponseblockloc_reqopen
			 
   	   try {
		blockreq_resp = stub_namenode1.getBlockLocations(inp1);
	} catch (RemoteException e1) {
		e1.printStackTrace();
	}
   	    
     
   	    Namenode.Hdfs.BlockLocationResponse blockreq_resp_obj=null;
   	    try 
   	    	{
   		    	blockreq_resp_obj=Namenode.Hdfs.BlockLocationResponse.parseFrom(blockreq_resp);
   	    	} 
   	    catch (InvalidProtocolBufferException e) 
   			{
   			e.printStackTrace();
   			}
   	    
   	      List<Namenode.Hdfs.BlockLocations> blockinfo_obj=new ArrayList<Namenode.Hdfs.BlockLocations>();
   	      blockinfo_obj=blockreq_resp_obj.getBlockLocationsList();  	     

   	       
   	       for(int i=0;i<blockinfo_obj.size();i++)
   	       {
   	    	   int blockno=blockinfo_obj.get(i).getBlockNumber();
   	    	   String ip=blockinfo_obj.get(i).getLocations(0).getIp();
   	    	   int port=blockinfo_obj.get(i).getLocations(0).getPort();
   	    	   String datanode_no=ip.substring(7);
   	    	   String addr="rmi://"+ip+":"+port+"/datanode"+datanode_no;
   	    	   
   	    	   
   	    	   Hdfs.ReadBlockRequest.Builder readreq=Hdfs.ReadBlockRequest.newBuilder();
   	    	   readreq.setBlockNumber(blockno);
   	    	   byte[] inp2=readreq.build().toByteArray();
   	    	   
   	    	   Datanode.IDataNode stub_datanode_read = null;
  					try {
  						stub_datanode_read = (Datanode.IDataNode)Naming.lookup(addr);
  					} catch (NotBoundException e) {
  					// TODO Auto-generated catch block
  					e.printStackTrace();
  					} catch (MalformedURLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (RemoteException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} 
  				
   	    	   byte[] readblock_resp=null;
   	    	   
   	    	   try {
				readblock_resp=stub_datanode_read.readBlock(inp2);
			} catch (RemoteException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
   	    	   Hdfs.ReadBlockResponse readblocresp_obj=null;
   	    	   try {
   	    		   readblocresp_obj = Hdfs.ReadBlockResponse.parseFrom(readblock_resp);
   	    	   } catch (InvalidProtocolBufferException e) {
				   e.printStackTrace();
   	    	   }
   	    	   byte[] data=readblocresp_obj.getData(0).toByteArray();
   	    	//   IReducerClass red = new IReducerClass();
   	    	   IReducer red=null;
   	    	   try {
   				red = (IReducer)Class.forName("TaskTracker.IReducerClass").newInstance();
   	    	   } catch (InstantiationException | IllegalAccessException
   					| ClassNotFoundException e3) {
   				// TODO Auto-generated catch block
   				e3.printStackTrace();
   			}
   	    	   InputStream is = null;
   	           BufferedReader bfReader = null;
   	           try {
   	                 is = new ByteArrayInputStream(data);
   	                 bfReader = new BufferedReader(new InputStreamReader(is));
   	                 String temp = null;
   	                 while((temp = bfReader.readLine()) != null)
   	                 {
   	                	 String str = red.reduce(temp);
   	                	 bw.write(str);
   	                	 bw.newLine();               
   	                }
   	           	} 
   	           catch (IOException e) {
   	            e.printStackTrace();
   	           } 
   	           finally 
   	           		{
   	        	   		try{
   	        	   			if(is != null) is.close();
   	        	   		} catch (Exception ex){
   	                 
   	        	   		}   
   	           		} 
   	       		
   	       		}		
			}
		
		try {bw.close();} catch (IOException e) {
			e.printStackTrace();}
		
			
		//WRITING ON THE HDFS
		
		//Call Open File Request for Read/Write
		    Namenode.Hdfs.OpenFileRequest.Builder openfile = Namenode.Hdfs.OpenFileRequest.newBuilder();
		    openfile.setFileName(outputfile);
		    openfile.setForRead(false);
		    byte[] inp = new byte[1024];
		    inp = openfile.build().toByteArray();
		    
		    //Retreiving Open File Response : File handle and status
		    byte[] open_file_resp=null;
		    try {
				open_file_resp = stub_namenode.openFile(inp);
			} catch (RemoteException e3) {
				// TODO Auto-generated catch block
				e3.printStackTrace();
			}
		    Hdfs.OpenFileResponse openfile_resp_obj=null;
		    try 
		    	{
			    	openfile_resp_obj=Hdfs.OpenFileResponse.parseFrom(open_file_resp);
		    	} 
		    catch (InvalidProtocolBufferException e1) 
				{
				e1.printStackTrace();
				}

		    int status=openfile_resp_obj.getStatus();
		    int handle=openfile_resp_obj.getHandle();
		  //Writing the file : Calling AssignBlock to namenode and write block to datanode
		   File f_read=new File(outputfile);
		    FileInputStream fileinput=null;
		    try {
		    		fileinput = new FileInputStream(f_read);
		    	} catch (FileNotFoundException e2) {
		    		// TODO Auto-generated catch block
		    		e2.printStackTrace();
		    	}
		       
				int sizeofFiles=32*1024*1024;
		    //int sizeofFiles = 128*1024;
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
						} catch (NotBoundException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
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
				} 
				
				catch (FileNotFoundException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				
				//Creating CloseFile Request
			    Hdfs.CloseFileRequest.Builder close_file_obj = Hdfs.CloseFileRequest.newBuilder();
			    close_file_obj.setHandle(handle);
			    byte[] close_buf = new byte[1024];
			    close_file_obj.build();
			    close_buf = close_file_obj.build().toByteArray();
			    
			    byte[] close_resp=null;
			    try {
					close_resp = stub_namenode.closeFile(close_buf);
				} catch (RemoteException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			    
			    Hdfs.CloseFileResponse close_obj = null;
			    try {
					close_obj = Hdfs.CloseFileResponse.parseFrom(close_buf);
				} catch (InvalidProtocolBufferException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			    
			    int status2 = close_obj.getStatus();
			    
		
		//Returning
	    TaskTracker_server.reducetaskcompletedhash.put(taskid, 1);



	}
}
