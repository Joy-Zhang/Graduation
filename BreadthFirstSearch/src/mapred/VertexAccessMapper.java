package mapred;

import java.io.*;
import common.*;


import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class VertexAccessMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, IntWritable, Text> {

	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<IntWritable, Text> output, Reporter reporter)
			throws IOException {
		GraphIndexer indexer = new GraphIndexer();
		int vertex = Integer.parseInt(value.toString().trim());
		indexer.markupAccessed(vertex);
		System.out.println(String.valueOf(vertex));
		int[] neighbourhood = indexer.queryNeighbours(vertex);
		for(int i : neighbourhood)
		{
			if(!indexer.isAccessed(i))
				output.collect(new IntWritable(i), new Text(""));
		}
		indexer.close();
	}

}
