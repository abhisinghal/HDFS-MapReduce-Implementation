package JobTracker;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RMISecurityManager;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import Client_hdfs.Hdfs;

import com.google.protobuf.InvalidProtocolBufferException;

public class JobTracker_server {
	
	public static Queue<test> queue=new LinkedList<test>();
    public static int totaltasks;
    public static int remainingtasks, taskscompleted=0;
    public static int remainingreducetasks, reducetask_completed=0;
    public static ArrayList<String> outputfilelist = new ArrayList<String>();
    public static int task_id=0;
    public static int no_ofreducertasks;
    public static boolean filesubmit=false;
	public static boolean flagforereducequeue=false;
	
    
   public static Queue<maptask_info> queue_mapred=new LinkedList<maptask_info>();
   public static Queue<reducetask_info> queue_reducetask=new LinkedList<reducetask_info>();
   
    //Storing maptaskinfo in the structure from heartbeat request
    ArrayList<byte[]> mapjob_statuslist=new ArrayList<byte[]>();	
	public static void main(String args[])
	{
	System.out.println("Job Tracker Server Started at 10.0.0.1");	 
		
	 System.setProperty("java.rmi.server.hostname","10.0.0.1");
	 try{  
		    IJobTracker stub_job=new IJobTrackerRemote();  
		    Registry registry=LocateRegistry.createRegistry(5020);
		    registry.rebind("jobtracker", stub_job);  
		    }catch(Exception e){System.out.println(e);
		    }
	 Namenode.INameNode stub_namenode=null;
	 try {
		stub_namenode = (Namenode.INameNode)Naming.lookup("rmi://10.0.0.1:5010/namenode");
	
	 } catch ( RemoteException | NotBoundException e1) {
			e1.printStackTrace();
	 } catch (MalformedURLException e) {
		e.printStackTrace();
	}  
	
	 while(true)
	 {
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		
	  if(filesubmit==true)
	  {	  
	  String filename = queue.peek().getinfilename();	 
	  filesubmit=false;
	  
	  //Call Open File Request for Read/Write
	    Hdfs.OpenFileRequest.Builder openfile = Hdfs.OpenFileRequest.newBuilder();
	  
	    openfile.setFileName(filename);
        openfile.setForRead(true);
        byte[] inp = openfile.build().toByteArray();
 
   
        //Retreiving Open File Response : File handle and status
        byte[] open_file_resp=null;
        try {
        	open_file_resp = stub_namenode.openFile(inp);
        } catch (RemoteException e1) {
	    	e1.printStackTrace();
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
        List<Integer> blocknos = new ArrayList<Integer>();
        blocknos=openfile_resp_obj.getBlockNumsList();
       
 
        Hdfs.BlockLocationRequest.Builder blockloc_req=Hdfs.BlockLocationRequest.newBuilder();
        for(int i=0;i<blocknos.size();i++)
        	{
        	blockloc_req.addBlockNums(blocknos.get(i));
        	}
        inp = blockloc_req.build().toByteArray();
 
        byte[] blockreq_resp=null;
        
 //Calling GetBlockLocations - BlockLocationResponse
 try {
	 blockreq_resp = stub_namenode.getBlockLocations(inp);
	    
 } catch (RemoteException e1) {
	  
	e1.printStackTrace();
 }

 Hdfs.BlockLocationResponse blockreq_resp_obj=null;
 try 
 	{
	    	blockreq_resp_obj=Hdfs.BlockLocationResponse.parseFrom(blockreq_resp);
 	} 
 catch (InvalidProtocolBufferException e) 
		{
		e.printStackTrace();
		}
   
 //BLOCKLOCATIONS INFO
 int status_res=blockreq_resp_obj.getStatus();
   List<Hdfs.BlockLocations> blockinfo_obj=new ArrayList<Hdfs.BlockLocations>();
   blockinfo_obj=blockreq_resp_obj.getBlockLocationsList();   //Block locations list
  // Queue<maptask_info> queue_mapred=new LinkedList<maptask_info>();
   
  
   
   for(int i=0;i<blockinfo_obj.size();i++)
 		{
	   task_id +=1;
	   	String map="MapTask";
	   	int blockno=blockinfo_obj.get(i).getBlockNumber();
	   	String ip1=blockinfo_obj.get(i).getLocationsList().get(0).getIp();
	   	int ip_1=Integer.parseInt(ip1.substring(7));
	   	String ip2=blockinfo_obj.get(i).getLocationsList().get(1).getIp();
	   	int ip_2=Integer.parseInt(ip2.substring(7));
	   	int port1=blockinfo_obj.get(i).getLocationsList().get(0).getPort();
	   	int port2=blockinfo_obj.get(i).getLocationsList().get(1).getPort();
	   	maptask_info obj=new maptask_info(queue.element().jobid,task_id, map,blockno, ip_1,ip_2, port1, port2);
	   	queue_mapred.add(obj);
   		}
   //Apply the check for num of reducer tasks
   boolean flag=false;
 
   totaltasks = blockinfo_obj.size();
   remainingtasks=totaltasks;
	}
	  
	 }
	}
}

class test{
	int jobid;
	String infilename;
	String outfilename;
	String mapname;
	String reducername;
	int numredtasks;
	test(int jobid,String a,String b,String c,String d,int e)
	{
		this.jobid=jobid;
		this.infilename=a;
		this.outfilename=b;
		this.mapname=c;
		this.reducername=d;
		this.numredtasks=e;
	}
	
	public String getinfilename()
	{
	  return infilename;	
	}
	public String getoutfilename()
	{
		return outfilename;
	}
	public String getmapname()
	{
		return mapname;
	}
	public String getreducername()
	{
		return reducername;
	}
	public int getnumredtasks()
	{
		return numredtasks;
	}
	
}

class maptask_info{
	int jobid;
	int taskid;
	String mapname;
	int blockno;
	int ip1;
	int ip2;
	int port1;
	int port2;
	public maptask_info(int a,int b, String c,int blockno,int ip1,int ip2,int port1,int port2) {
		this.jobid=a;
		this.taskid=b;
		this.mapname=c;
		this.blockno=blockno;
		this.ip1=ip1;
		this.ip2=ip2;
		this.port1=port1;
		this.port2=port2;
		// TODO Auto-generated constructor stub
	}
}


class reducetask_info{
	int jobid_r;
	int taskid_r;
	String reducer_name;
	String outputfile;
	List<String> mapoutputfiles = new ArrayList<String>();
	public reducetask_info(int jobid_r, int taskid_r, String reducer_name, String outputfile,ArrayList<String> mapoutputfiles )
	{
		this.taskid_r =taskid_r;
		this.jobid_r = jobid_r;
		this.reducer_name = reducer_name;	
		this.outputfile = outputfile;
		this.mapoutputfiles = mapoutputfiles;
	}
}

