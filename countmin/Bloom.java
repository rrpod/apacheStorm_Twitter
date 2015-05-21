package storm.starter.trident.project.countmin;

import java.io.*;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.BaseFilter;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import storm.starter.trident.project.countmin.filters.BloomFilter;

public class Bloom  extends BaseFilter{

	int noHashes;
	int bitsPerItem;
	int countStopWords;
	BloomFilter bFilter;
	String Path;
	
	/*Function to read from stop words file and update the bloom filter*/
	public Bloom()
	{
		boolean fileFound = false;
		String stopWord;

		noHashes = 5;
		bitsPerItem = 8;
		Path = "data/stopwords/stopwords.txt";

		BufferedReader fileHandler = null;
		
		try
		{
			/*Get the total number of stop words*/
			LineNumberReader lnr = new LineNumberReader(new FileReader(Path));
			lnr.skip(Long.MAX_VALUE);
			countStopWords = lnr.getLineNumber()+1;
			lnr.close();
			fileFound = true;
		}
		catch(Exception e)
		{
			System.out.println("Class Bloom: File "+Path+ " is not found");
		} 
		if(fileFound == true)
		{
			try
			{
				/*Read the file and update the bloom filter*/
				fileHandler = new BufferedReader(new FileReader(Path));
				
				bFilter= new BloomFilter(1000,bitsPerItem,noHashes);
				while((stopWord = fileHandler.readLine())!=null)
				{
					bFilter.add(stopWord);
				}
				System.out.println("Class Bloom: Adding Stopwords to BloomFilter Over");
				//bFilter1 = bFilter;
			}
			catch(Exception e)
			{
				System.out.println("Class Bloom: File "+Path+ " is not found");
			}

		}
		

	}
	
	@Override
	public boolean isKeep(TridentTuple tuple)
	{
		return !(bFilter.contains(tuple.getString(0)));
	}

};
