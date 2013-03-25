package mapred;

import java.io.IOException;
import java.util.Iterator;



import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import common.*;

public class PageRankReducer extends MapReduceBase implements
		Reducer<LongWritable, Text, LongWritable, DoubleWritable> {

	@Override
	public void reduce(LongWritable dest, Iterator<Text> weights,
			OutputCollector<LongWritable, DoubleWritable> output, Reporter reporter)
			throws IOException {
		PageRankVector vector = new PageRankVector(new Configuration(), new Path("/tmp/page_rank"));
		PageRankVector.PageRankVectorIterator iterator = vector.iterator();
		double pageRank = 0.0;
		while(iterator.hasNext() && weights.hasNext()) {
			PageRankVector.PageRankPair pair = iterator.next();
			String[] parts = weights.next().toString().split("\t");
			LongWritable srcPage = new LongWritable(Long.parseLong(parts[0]));
			double weight = Double.parseDouble(parts[1]);
		    FileInputFormat.LOG.error(new Boolean(pair.getPage().equals(srcPage)));
		    
		    
		    pageRank += pair.getPageRank().get() * weight;		
			
		}
		output.collect(dest, new DoubleWritable(pageRank));
		vector.close();
	}

}
