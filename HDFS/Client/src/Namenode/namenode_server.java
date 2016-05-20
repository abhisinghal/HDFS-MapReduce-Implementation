package Namenode;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.rmi.Naming;
import java.rmi.RMISecurityManager;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;

public class namenode_server 
{
	public static HashMap<String,Integer> hm=new HashMap<String,Integer>();
	
	//Blockno to Ip mapping
	
	
	public static int block_no_counter=0;
	public static int handle_counter=0;
	/*public static HashMap<Integer,List<Integer>> block_ip_temp =new HashMap<Integer,List<Integer>>();	
	public static HashMap<Integer,List<Integer>> blockreport =new HashMap<Integer,List<Integer>>();
	public static HashMap<String, List<Integer>> namenode = new HashMap <String, List<Integer>>();	
	*/
	public static ConcurrentHashMap<Integer, List<Integer>> block_ip_temp=new ConcurrentHashMap<Integer,List<Integer>>();
	public static ConcurrentHashMap<Integer,List<Integer>> blockreport =new ConcurrentHashMap<Integer,List<Integer>>();
	public static ConcurrentHashMap<String, List<Integer>> namenode = new ConcurrentHashMap <String, List<Integer>>();
	
   @SuppressWarnings("resource")
	public static void main(String args[]) throws IOException{ 
	    System.setProperty("java.rmi.server.hostname","10.0.0.1");
      
	    //Setting up the name node hash structure
	    BufferedReader br=null;
        br=new BufferedReader(new FileReader("name_node_config"));
        String line;
        while((line=br.readLine()) != null)
        {
        	StringTokenizer st=new StringTokenizer(line,":");
        	String filename = st.nextToken();
        	String blocklist = st.nextToken();
        	
        	StringTokenizer st_blocks=new StringTokenizer(blocklist,",");
        	List<Integer> block_list=new ArrayList<Integer>();
        	while(st_blocks.hasMoreTokens())
        	{
        		block_list.add(Integer.parseInt(st_blocks.nextToken()));
        	}
        	List<Integer> newblocklist=new ArrayList<Integer>();
        	for(int k=0;k<block_list.size();k++)
        		newblocklist.add(block_list.get(k));
        	namenode.put(filename, newblocklist);
        	//System.out.println("Namenode during loading: "+namenode);
        	block_list.clear();
        	
        }
        br.close();
   
        //Setting block_ip_hash value
        br=new BufferedReader(new FileReader("block_ip_temp"));
        System.out.println("Name Node Server started at 10.0.0.1");
        int blockno = 0;
        while((line=br.readLine()) != null)
        {
        	StringTokenizer st=new StringTokenizer(line,":");
        	blockno =Integer.parseInt( st.nextToken());
        	String iplist = st.nextToken();
        	StringTokenizer st_ips=new StringTokenizer(iplist,",");
        	List<Integer> ip_list=new ArrayList<Integer>();
        	while(st_ips.hasMoreTokens())
        	{
        		ip_list.add(Integer.parseInt(st_ips.nextToken()));
        		
        	}
        	block_ip_temp.put(blockno, ip_list);
        	
        }
        block_no_counter = blockno;
        br.close();
            
        
	    try
	    {
	        
	    }
	    catch (Exception e)
	    {
	        // ignore
	    }  
	    try{  
	    INameNode stub=new INameNodeRemote();  
	    Registry registry=LocateRegistry.createRegistry(5010);
	    registry.rebind("namenode", stub);  
	    }catch(Exception e){System.out.println(e);}  
	    }  
      
}
