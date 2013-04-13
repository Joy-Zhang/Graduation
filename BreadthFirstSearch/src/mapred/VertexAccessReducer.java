package mapred;

import java.io.*;
import java.util.*;


import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;



public class VertexAccessReducer extends MapReduceBase implements
		Reducer<IntWritable, Text, IntWritable, Text> {

	@Override
	public void reduce(IntWritable key, Iterator<Text> values,
			OutputCollector<IntWritable, Text> output, Reporter reporter)
			throws IOException {
		output.collect(key, new Text(""));

	}

}
