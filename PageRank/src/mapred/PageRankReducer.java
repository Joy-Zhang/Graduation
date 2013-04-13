package mapred;

import io.PageRankOutputFormat;

import java.io.IOException;
import java.util.Iterator;



import org.apache.hadoop.conf.*;

import org.apache.hadoop.hbase.client.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;



public class PageRankReducer extends MapReduceBase implements
		Reducer<LongWritable, Text, LongWritable, DoubleWritable> {

	@Override
	public void reduce(LongWritable dest, Iterator<Text> weights,
			OutputCollector<LongWritable, DoubleWritable> output, Reporter reporter)
			throws IOException {

		HTable table = new HTable(new Configuration(), PageRankOutputFormat.TABLE_NAME);

		double pageRank = 0.0;
		while(weights.hasNext()) {
			String[] parts = weights.next().toString().split("\t");
			LongWritable srcPage = new LongWritable(Long.parseLong(parts[0]));
			double weight = Double.parseDouble(parts[1]);

			Result result = table.get(new Get(String.valueOf(srcPage.get()).getBytes()));
			double value = Double.parseDouble(new String(
					result.getValue(PageRankOutputFormat.COLUMN_NAME.getBytes(), "old".getBytes())));
		    pageRank += value * weight;		
			
		}
		System.out.println("A pageRank");
		output.collect(dest, new DoubleWritable(pageRank));

	}

}
