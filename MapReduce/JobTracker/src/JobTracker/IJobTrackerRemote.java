package JobTracker;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Random;

import com.google.protobuf.InvalidProtocolBufferException;


public class IJobTrackerRemote extends UnicastRemoteObject implements IJobTracker
{
	public static int totaltimes;
	public static int reducetotaltimes;
	public static int i=0;
	public static int reducerno=1;
	public static HashMap<Integer, Integer> TaskCompletionHash = new HashMap<Integer, Integer>();
	public static HashMap<Integer, Integer> ReduceTaskCompletionHash = new HashMap<Integer, Integer>();
	IJobTrackerRemote() throws RemoteException {
		super();
	}

	/* JobSubmitResponse jobSubmit(JobSubmitRequest) */
	public byte[] jobSubmit(byte[] inp)
	{
		MapReduce.JobSubmitRequest jobreq=null;
		try {
			jobreq=MapReduce.JobSubmitRequest.parseFrom(inp);
		} catch (InvalidProtocolBufferException e) {
			System.out.println("Error in job submit");
			e.printStackTrace();
		}	
		String mapname=jobreq.getMapName();
		String redname=jobreq.getReducerName();
		String inputfile=jobreq.getInputFile();
		String outfile=jobreq.getOutputFile();
		JobTracker_server.no_ofreducertasks=jobreq.getNumReduceTasks();
		MapReduce.JobSubmitResponse.Builder jobres=MapReduce.JobSubmitResponse.newBuilder();
		jobres.setStatus(1);
		Random rand = new Random();
		int randno=rand.nextInt();
		if(randno<0)
			randno=-randno;
		jobres.setJobId(randno);
		byte[] buffer =jobres.build().toByteArray();
		test fileinfo=new test(randno,inputfile,outfile,mapname,redname,JobTracker_server.no_ofreducertasks);
		JobTracker_server.queue.add(fileinfo);
		JobTracker_server.filesubmit=true;
		return buffer;
	}
	

	/* JobStatusResponse getJobStatus(JobStatusRequest) */
	public synchronized byte[] getJobStatus(byte[] inp)
	{ 
		MapReduce.JobStatusRequest req=null;
		try {
			 req=MapReduce.JobStatusRequest.parseFrom(inp);
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
		}
		int jobid=req.getJobId();
		
		MapReduce.JobStatusResponse.Builder jobres=MapReduce.JobStatusResponse.newBuilder();
		jobres.setStatus(1);
		jobres.setTotalMapTasks(JobTracker_server.totaltasks);
		jobres.setNumMapTasksStarted(totaltimes);
		jobres.setTotalReduceTasks(JobTracker_server.no_ofreducertasks);
		jobres.setNumReduceTasksStarted(reducetotaltimes);
	    if(JobTracker_server.reducetask_completed!=0 )
	    	{
	    	if(JobTracker_server.no_ofreducertasks==JobTracker_server.reducetask_completed)
	    		{
	    		jobres.setJobDone(true);
	    		JobTracker_server.queue.clear();
	    		JobTracker_server.totaltasks=0;
	    	    JobTracker_server.remainingtasks=0;
	    	    JobTracker_server.taskscompleted=0;
	    	    JobTracker_server.remainingreducetasks=0;
	    	    JobTracker_server.reducetask_completed=0;
	    	    JobTracker_server.outputfilelist.clear();
	    	    JobTracker_server.task_id=0;
	    	    JobTracker_server.no_ofreducertasks=0;
	    	    JobTracker_server.filesubmit=false;
	    	    JobTracker_server.flagforereducequeue=false;
	    	    
	    	    totaltimes=0;
	    	    reducetotaltimes=0;
	    	    reducerno=1;    	    
	    		TaskCompletionHash.clear();
	    		}
	    	}
	    else
	    	jobres.setJobDone(false);
	    byte[] buffer = jobres.build().toByteArray();
		return buffer;
	}
	
	/* HeartBeatResponse heartBeat(HeartBeatRequest) */
	public synchronized byte[]  heartBeat(byte[] inp)
	{
		MapReduce.HeartBeatRequest hrtbeat_req_ob=null;
		try {
			hrtbeat_req_ob = MapReduce.HeartBeatRequest.parseFrom(inp);
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
		}
		int tasktrackerid = hrtbeat_req_ob.getTaskTrackerId();
		int numMapSlotsFree = hrtbeat_req_ob.getNumMapSlotsFree();
		int numReduceSlotsFree = hrtbeat_req_ob.getNumReduceSlotsFree();
		int mapstatuscount = hrtbeat_req_ob.getMapStatusCount();
		int reducestatuscount = hrtbeat_req_ob.getReduceStatusCount();
		
		for (int i=0; i<mapstatuscount; i++)
			{
		//	System.out.println("Checking map Status");
			int jobId = hrtbeat_req_ob.getMapStatus(i).getJobId();
			int taskId = hrtbeat_req_ob.getMapStatus(i).getTaskId();
			boolean taskCompleted = hrtbeat_req_ob.getMapStatus(i).getTaskCompleted();
			String mapOutputFile = hrtbeat_req_ob.getMapStatus(i).getMapOutputFile();
			int flag=0;
			if (JobTracker_server.totaltasks>JobTracker_server.taskscompleted)
			 {
				flag=0;
				for (int x=0; x<JobTracker_server.outputfilelist.size(); x++)	
				 	{
				    if(JobTracker_server.outputfilelist.get(x).equals(mapOutputFile))
				    		flag=1;
				 	}
			 if(flag==0)
				 {
				 JobTracker_server.outputfilelist.add(mapOutputFile);
				 	
				 } 
			 }
			
			if(TaskCompletionHash.containsKey(taskId))
			{
				if(TaskCompletionHash.get(taskId)==0 && taskCompleted==true)
				{
					TaskCompletionHash.put(taskId, 1);
					JobTracker_server.taskscompleted++;
				}
				
					
			}
			else
			{
			   if(taskCompleted!=true)
				TaskCompletionHash.put(taskId, 0);
			   else
			   {
				   TaskCompletionHash.put(taskId, 1);
				   JobTracker_server.taskscompleted++;
			   }
			}
			
			}
			
		
		if(JobTracker_server.totaltasks>0 &&(JobTracker_server.totaltasks==JobTracker_server.taskscompleted) && JobTracker_server.flagforereducequeue==false)
	    {   
			JobTracker_server.remainingreducetasks = JobTracker_server.no_ofreducertasks;
			JobTracker_server.flagforereducequeue=true;
			 int x=JobTracker_server.no_ofreducertasks;
			 
		     for(int j=0; j<JobTracker_server.no_ofreducertasks; j++)
		   		{
			   	  int jobid = JobTracker_server.queue.element().jobid;
			   	  int taskid = JobTracker_server.task_id+=1;
			   	  String reducername = "ReduceTask";
			   	  int nooffiles = JobTracker_server.outputfilelist.size()/x;
			   	   x=x-1;
			    	String outfilename = JobTracker_server.queue.element().outfilename+"_"+jobid+"_"+reducerno;
			    	reducerno++;
			    	ArrayList<String> mapoutputfiles = new ArrayList<String>();
			   	  for(int j1=0; j1<nooffiles; j1++)
			   		{
			   		  mapoutputfiles.add(JobTracker_server.outputfilelist.get(0));
			   		  JobTracker_server.outputfilelist.remove(0);
			   		}
			   	  int te = reducerno-1;
			   	   reducetask_info obj = new reducetask_info(jobid, taskid, reducername, outfilename, mapoutputfiles);
			       JobTracker_server.queue_reducetask.add(obj);
		   		}
		 }
		
		//NOT GOING IN THIS LOOOPP Prinitng zero always
		for (int i=0; i<reducestatuscount; i++)
			{
			    int jobId = hrtbeat_req_ob.getReduceStatus(i).getJobId();
				int taskId = hrtbeat_req_ob.getReduceStatus(i).getTaskId();
				boolean taskCompleted = hrtbeat_req_ob.getReduceStatus(i).getTaskCompleted();			
				if(ReduceTaskCompletionHash.containsKey(taskId))
				{
					if(ReduceTaskCompletionHash.get(taskId)==0 && taskCompleted==true)
					{
						ReduceTaskCompletionHash.put(taskId, 1);
						JobTracker_server.reducetask_completed++;
					}
				}
				else
				{
				   if(taskCompleted!=true)
					ReduceTaskCompletionHash.put(taskId, 0);
				   else
				   {
					  ReduceTaskCompletionHash.put(taskId, 1);
					   JobTracker_server.reducetask_completed++;
				   }
				}
				
			}
		
		//System.out.println("************Reduce Tasks completed********** : " + JobTracker_server.reducetask_completed);
		
		//Heartbeat Response back to TaskTracker	
		MapReduce.HeartBeatResponse.Builder hrtresp_obj = MapReduce.HeartBeatResponse.newBuilder();
		hrtresp_obj.setStatus(1);   
		
		int times=0;	    
	    if(JobTracker_server.remainingtasks>numMapSlotsFree)
	    {
	       //System.out.println("Remaining Tasks: "+JobTracker_server.remainingtasks+" "+"numofMap "+numMapSlotsFree);
	    	times=numMapSlotsFree;
	    	JobTracker_server.remainingtasks-=numMapSlotsFree;
	    }
	    else
	    { 
	    	//System.out.println("Remaining Tasks: "+JobTracker_server.remainingtasks); 
	    	times=JobTracker_server.remainingtasks;
	    	JobTracker_server.remainingtasks=0;
	    }
	    totaltimes=totaltimes+times;
	    for(int i=0; i<times; i++)
			{ 
	    	 // System.out.println("Times"+i);
	    	    int jobid=JobTracker_server.queue_mapred.element().jobid;
	    	 //  System.out.println("Error");
	    	    int taskid=JobTracker_server.queue_mapred.element().taskid;
	    	    String mapname=JobTracker_server.queue_mapred.element().mapname;
	    	    int blockno=JobTracker_server.queue_mapred.element().blockno;
	    	    int ip_no1=JobTracker_server.queue_mapred.element().ip1;
	    	    int ip_no2=JobTracker_server.queue_mapred.element().ip2;
	    	    String ip1 = "10.0.0."+ip_no1;
	    	    String ip2 = "10.0.0."+ip_no2;
	    	    
	    	    int port1=JobTracker_server.queue_mapred.element().port1;
	    	    int port2=JobTracker_server.queue_mapred.element().port2;
	    	    JobTracker_server.queue_mapred.remove();
			    
				//Setting MapTaskInfo For the heartbeat Response
				MapReduce.MapTaskInfo.Builder mapinfo_obj = MapReduce.MapTaskInfo.newBuilder();
				mapinfo_obj.setJobId(jobid);
				mapinfo_obj.setTaskId(taskid);
				mapinfo_obj.setMapName(mapname);
				MapReduce.BlockLocations.Builder blloc = MapReduce.BlockLocations.newBuilder();
				blloc.setBlockNumber(blockno);
				
				MapReduce.DataNodeLocation.Builder dlloc1 = MapReduce.DataNodeLocation.newBuilder();
				dlloc1.setIp(ip1);
				dlloc1.setPort(port1);
				blloc.addLocations(dlloc1);
				
				MapReduce.DataNodeLocation.Builder dlloc2 = MapReduce.DataNodeLocation.newBuilder();
				dlloc2.setIp(ip2);
				dlloc2.setPort(port2);
				blloc.addLocations(dlloc2);
				
				mapinfo_obj.addInputBlocks(blloc);
				hrtresp_obj.addMapTasks(mapinfo_obj);
				
			}
	    
	  if (JobTracker_server.totaltasks>0 && (JobTracker_server.totaltasks == JobTracker_server.taskscompleted))
	    	{
	    	int times2=0;	    
	    	if(JobTracker_server.remainingreducetasks>numReduceSlotsFree)
	    		{
	    		times2=numReduceSlotsFree;
	    		JobTracker_server.remainingreducetasks-=numReduceSlotsFree;
	    		}
	    	else
	    	{
	    	  times2=JobTracker_server.remainingreducetasks;
	    	  
	    	  JobTracker_server.remainingreducetasks=0;
	    	}
	    	reducetotaltimes=reducetotaltimes+times2;
	    	for(int i=0; i<times2; i++)
	    	{
	    		//System.out.println("Setting the reduce taks for heartresponse");
	    		int jobid=JobTracker_server.queue_reducetask.element().jobid_r;
	    		int taskid=JobTracker_server.queue_reducetask.element().taskid_r;
	    		String reducename=JobTracker_server.queue_reducetask.element().reducer_name;
	    		List<String> mapoutputfiles = JobTracker_server.queue_reducetask.element().mapoutputfiles;
	    		
	    		String outputfile = JobTracker_server.queue_reducetask.element().outputfile;
	    		JobTracker_server.queue_reducetask.remove();
	    		
	    		//Setting ReduceTaskInfo For the heartbeat Response
	    		MapReduce.ReducerTaskInfo.Builder reduceinfo_obj = MapReduce.ReducerTaskInfo.newBuilder();
	    		reduceinfo_obj.setJobId(jobid);
	    		reduceinfo_obj.setTaskId(taskid);
	    		reduceinfo_obj.setOutputFile(outputfile);
	    		
	    		for(int j=0; j<mapoutputfiles.size(); j++)
	    			{
	    			String outfile = mapoutputfiles.get(j);
	    			reduceinfo_obj.addMapOutputFiles(outfile);
	    			}    		
				hrtresp_obj.addReduceTasks(reduceinfo_obj);
			
	    	}
	    }
		    
		byte [] buffer = hrtresp_obj.build().toByteArray();
		return buffer;
	}
}
