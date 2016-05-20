package TaskTracker;

import java.util.StringTokenizer;

public class IReducerClass implements IReducer {

	@Override
	public String reduce(String str) {
		StringTokenizer temp=new StringTokenizer(str, ":");
		String t=new String();
		int tokens = temp.countTokens()-1;
		int count=0;
		while(tokens!=count)
		{
			t+=temp.nextToken();
			count+=1;
		}
		return t;
	}
	
	

}
