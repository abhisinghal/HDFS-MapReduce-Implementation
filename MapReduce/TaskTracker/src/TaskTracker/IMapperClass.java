package TaskTracker;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class IMapperClass implements IMapper
{
	@Override
	public String map(String str) 
	{	
		//System.out.println(str);
		//System.out.println(MapTask.word);
		StringTokenizer st=new StringTokenizer(str," ");
		List<String> word_list=new ArrayList<String>();
    	while(st.hasMoreTokens())
    	{
    		word_list.add(st.nextToken());
    		
    	}
    	//System.out.println(word_list);
    	for (int i=0; i<word_list.size(); i++)
    	{
    		if (MapTask.word.equals(word_list.get(i)))
    				{
    					String str_new = str+":20";
    					//System.out.println(str_new);
    					return str_new;
    				}
    	}
		return null;
	}

}
