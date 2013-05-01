package mapred;

import java.io.*;



import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;



public class VertexAccessReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

	@Override
	protected void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		context.write(key, new Text(""));

	}
	
	





}
