package storm.starter.trident.project.countmin; 

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.state.StateFactory;
import storm.trident.testing.MemoryMapState;
import storm.trident.testing.Split;
import storm.trident.testing.FixedBatchSpout;
import backtype.storm.tuple.Values;


import storm.trident.operation.builtin.Count;

import storm.starter.trident.project.countmin.state.CountMinSketchStateFactory;
import storm.starter.trident.project.countmin.state.CountMinQuery;
import storm.starter.trident.project.countmin.state.CountMinSketchUpdater;
//import storm.starter.trident.tutorial.functions.SplitFunction;
import storm.starter.trident.project.countmin.state.CountMinTopK;

import storm.starter.trident.project.filters.Print;
import storm.starter.trident.project.functions.Tweet;
import storm.starter.trident.project.functions.TextBuilder;
import storm.starter.trident.project.functions.ToLowerCase;

import storm.starter.trident.project.countmin.Bloom;
import storm.starter.trident.project.functions.ParseTweet;
import storm.starter.trident.project.spouts.TwitterSampleSpout;
import java.util.*;
/**
 *@author: Rohit Poduval 
 */


public class CountMinSketchTopology {

	 public static StormTopology buildTopology( String[] args_TwitterCredentials, LocalDRPC drpc ) {

        TridentTopology topology = new TridentTopology();

        int width = 1500;	//Total Number of columns in Count-min
		int depth = 15; //Total Number of rows in Count-min
		int seed = 10;	
		int K= 10; //Value of K in Top-K List

		/*Twitter Credentials received from arguments of this function*/
		String consumerKey = args_TwitterCredentials[0];
		String consumerSecret = args_TwitterCredentials[1];
		String accessToken = args_TwitterCredentials[2];
		String accessTokenSecret = args_TwitterCredentials[3];

		// Twitter topic of interest
        String[] arguments = args_TwitterCredentials.clone();
        String[] topicWords = Arrays.copyOfRange(arguments, 4, arguments.length);

        // Create Twitter Spout

        TwitterSampleSpout spoutTweets = new TwitterSampleSpout(consumerKey,consumerSecret,
        	accessToken,accessTokenSecret,topicWords);

        // Building a persistent state of words from the stream received
        TridentState countMinDBMS =topology.newStream("tweets",spoutTweets)
        /*From stream parse it into 3 categories, text, tweetId and the user*/
        	.each(new Fields("tweet"),new ParseTweet(),new Fields("text","tweetId","user"))
        /*Form a sentence from the above 3 categories*/
        	.each(new Fields("text","tweetId","user"),new TextBuilder(),new Fields("sentence"))
        /*Split the setence into words which are separated by space*/
        	.each(new Fields("sentence"),new Split(),new Fields("allwords"))
        /*Convert these words into lowercase*/
        	.each(new Fields("allwords"),new ToLowerCase(),new Fields("words"))
        /*Send each word to the bloom filter*/
        	.each(new Fields("words"),new Bloom())
        /*Create a persistent count min data structure for these filtered words*/
        	.partitionPersist(new CountMinSketchStateFactory(depth,width,seed,K),
        		new Fields("words"),
        		new CountMinSketchUpdater());

        /*Query the persistent storage to get the Top-K words*/

        topology.newDRPCStream("get_count",drpc)
        	.stateQuery(countMinDBMS, new Fields("args"),new CountMinTopK(), new Fields("count"))
        	.project(new Fields("args","count"));

        /*The topology which was built is returned*/
        return topology.build();    	
	}


	public static void main(String[] args) throws Exception {
		Config conf = new Config();
        	conf.setDebug( false );
        	conf.setMaxSpoutPending( 10 );

        	/*A local cluster is created*/
        	LocalCluster cluster = new LocalCluster();
        	/*A local drpc is created*/
        	LocalDRPC drpc = new LocalDRPC();

        	/*Topology get_count is submitted
        	* Topology is built by calling the buildTopology function*/
        	cluster.submitTopology("get_count",conf,buildTopology(args,drpc));

        	for (int i = 0; i < 5; i++) {
        		/*TopK parameter is passed to drpc execute as a dummy parameter*/
        		System.out.println("DRPC RESULT:"+ drpc.execute("get_count","TopK"));
                        System.out.println("");
        		Thread.sleep(10000);
        	}

		System.out.println("STATUS: OK");
		//cluster.shutdown();
        //drpc.shutdown();
	}
}
