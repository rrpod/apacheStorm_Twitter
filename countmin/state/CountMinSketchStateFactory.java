package storm.starter.trident.project.countmin.state;

import storm.trident.state.StateFactory;
import storm.trident.state.State;
import java.util.Map;
import backtype.storm.task.IMetricsContext;

/**
 *@author: Preetham MS (pmahish@ncsu.edu)
 */

public class CountMinSketchStateFactory implements StateFactory {

	protected int depth;
	protected int width;
	protected int seed;
	int topK;

	public CountMinSketchStateFactory( int depth, int width, int seed, int k) {
		this.depth=depth;
		this.width = width;
		this.seed = seed;
		this.topK = k; /*Update value of topk*/
	}


   @Override
   public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
      return new CountMinSketchState(depth,width,seed,topK);
   } 
}
