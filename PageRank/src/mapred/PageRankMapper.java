package mapred;


import java.io.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class PageRankMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, LongWritable, Text> {

	@Override
	public void map(LongWritable number, Text line,
			OutputCollector<LongWritable, Text> output, Reporter reporter)
			throws IOException {	
		String[] parts = line.toString().split("\t");
		LongWritable src = new LongWritable(Long.parseLong(parts[0]));
		LongWritable dest = new LongWritable(Long.parseLong(parts[1]));
		DoubleWritable weight = new DoubleWritable(Double.parseDouble(parts[2]));
		output.collect(dest, new Text(src.toString() + "\t" + weight.toString()));
		
	}

}
