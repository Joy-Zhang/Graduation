package mapred;

import java.io.*;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class PageReducer extends MapReduceBase implements
		Reducer<Text, Text, Text, IntWritable> {

	@Override
	public void reduce(Text src, Iterator<Text> dest,
			OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
		
		int links = 0;
		

		
		while(dest.hasNext())
		{
			links++;
			dest.next();
		}
		
		output.collect(src, new IntWritable(links));
	}

}
