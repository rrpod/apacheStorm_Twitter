package storm.starter.trident.project.countmin.state;

import storm.trident.state.BaseQueryFunction;
import storm.trident.tuple.TridentTuple;
import storm.trident.operation.TridentCollector;
import java.util.List;
import java.util.ArrayList;
import backtype.storm.tuple.Values;
import java.util.*;
import storm.starter.trident.project.countmin.state.CountMinSketchState;

/**
*@author: Rohit Poduval
**/

public class CountMinTopK extends BaseQueryFunction<CountMinSketchState, String>{
	public List<String> batchRetrieve(CountMinSketchState state,List<TridentTuple> inputs){
		List<String> topKList = new ArrayList();

		PriorityQueue<String> topKPriorityQ;
		String tweet;
		/*Get the entire priority queue as string format*/
		tweet = state.returnTopPQ();
		topKList.add(tweet);
		
		/*Send this to get it printed*/
		return topKList;

	}

	public void execute(TridentTuple tuple,String count,TridentCollector collector)
	{
		collector.emit(new Values(count));
	}
}