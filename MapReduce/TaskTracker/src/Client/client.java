package Client;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import com.google.protobuf.InvalidProtocolBufferException;
import Client.MapReduce.JobSubmitRequest;
public class client
{
	public static void main(String args[]) throws RemoteException, InvalidProtocolBufferException
	{
	String map_name=args[0];
	String red_name=args[1];
	String inp_file=args[2];
	String out_file=args[3];
	int numred_tasks=Integer.parseInt(args[4]);
	
	MapReduce.JobSubmitRequest.Builder jobreqobj = JobSubmitRequest.newBuilder();
	jobreqobj.setMapName(map_name);
	jobreqobj.setReducerName(red_name);
	jobreqobj.setInputFile(inp_file);
	jobreqobj.setOutputFile(out_file);
	jobreqobj.setNumReduceTasks(numred_tasks);
	byte[] job_req_byte =jobreqobj.build().toByteArray();
	JobTracker.IJobTracker stub_job=null;
	try {
			stub_job=(JobTracker.IJobTracker)Naming.lookup("rmi://10.0.0.1:5020/jobtracker");
	} catch (MalformedURLException | RemoteException | NotBoundException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	
	//Getting JOBResponse
	byte[] job_res_byte=stub_job.jobSubmit(job_req_byte);
	MapReduce.JobSubmitResponse job_submit_res=MapReduce.JobSubmitResponse.parseFrom(job_res_byte);
	int status=job_submit_res.getStatus();
	int jobId=job_submit_res.getJobId();	
	boolean jobdone=false;
	System.out.println("Client Started!!!!");
	
	while(jobdone==false)
		
		{
		 try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		MapReduce.JobStatusRequest.Builder jobreq_obj = MapReduce.JobStatusRequest.newBuilder();
		jobreq_obj.setJobId(jobId);
		byte[] jobreq = new byte[1024];
		jobreq = jobreq_obj.build().toByteArray();
		byte[] jobres_status=stub_job.getJobStatus(jobreq);
		
		//Retrive the JobStatusResponse
		 MapReduce.JobStatusResponse job_status_res=MapReduce.JobStatusResponse.parseFrom(jobres_status);
		 int status_job = job_status_res.getStatus();
		 jobdone = job_status_res.getJobDone();
		 int totalMapTasks = job_status_res.getTotalMapTasks();		
		 int numMapTasksStarted = job_status_res.getNumMapTasksStarted();
		 int totalReduceTasks = job_status_res.getTotalReduceTasks();	
		 int numReduceTasksStarted = job_status_res.getNumReduceTasksStarted();
		 int progressofmap=0;
		 int progressofreduce=0;		 
		 if(totalMapTasks==0)
			 progressofmap=0;
		 else
			 progressofmap = (numMapTasksStarted/totalMapTasks)*100;
		
		 if(totalReduceTasks==0)
			 progressofreduce=0;
		 else
			 progressofreduce = (numReduceTasksStarted/totalReduceTasks)*100;
		 System.out.println("**********************************************");
		 System.out.println("Progress of Maptasks: "+progressofmap+"%");
		 System.out.println("Progress of Reducetasks: "+progressofreduce+"%");	
		 System.out.println("**********************************************");
		 
		if (progressofreduce==100)
		 	{
			 try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		 	}
		}
	 System.out.println("Job Completed");
	}
}
