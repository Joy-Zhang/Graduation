package mapred;

import java.io.*;

import common.*;


import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;


public class VertexAccessMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		GraphIndexer indexer = new GraphIndexer();
		int vertex = Integer.parseInt(value.toString().trim());
		indexer.markupAccessed(vertex);
		System.out.println(String.valueOf(vertex));
		int[] neighbourhood = indexer.queryNeighbours(vertex);
		for(int i : neighbourhood)
		{
			if(!indexer.isAccessed(i))
				context.write(new IntWritable(i), new Text(""));
		}
		indexer.close();
	}



}
