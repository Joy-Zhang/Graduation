package deprecated.mapred;

import java.io.IOException;

import deprecated.job.ShortestPathJob;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import common.GraphImporter;




public class WarshallMapper extends MapReduceBase implements
		Mapper<Text, Text, Text, Text> {




	@Override
	public void configure(JobConf job) {
		mid = Integer.parseInt(job.get(ShortestPathJob.CURRENT_WARSHALL));
		super.configure(job);
	}
	private int mid;
	@Override
	public void map(Text key, Text value, OutputCollector<Text, Text> output,
			Reporter reporter) throws IOException {



		
		
		String[] pair = key.toString().split(" ");
		int src = Integer.parseInt(pair[0]);
		int dest = Integer.parseInt(pair[1]);

		HTable table  = new HTable(HBaseConfiguration.create(), GraphImporter.TABLE_NAME);
		
		Result r1 = table.get(new Get(String.format("%d %d", src, mid).getBytes()));
		Result r2 = table.get(new Get(String.format("%d %d", mid, dest).getBytes()));
		
		double add1 = Double.parseDouble(new String(
				r1.getValue(GraphImporter.COLUMN_NAME.getBytes(), "".getBytes())));
		double add2 = Double.parseDouble(new String(
				r2.getValue(GraphImporter.COLUMN_NAME.getBytes(), "".getBytes())));
		double origin = Double.parseDouble(value.toString());
		
		double newValue = add1 + add2; 
		Text resultValue = new Text(value);
		if(newValue < origin){
			Put put = new Put(key.getBytes());
			put.add(GraphImporter.COLUMN_NAME.getBytes(), "".getBytes(), 
					String.valueOf(newValue).getBytes());
			table.put(put);
			table.flushCommits();
			resultValue.set(String.valueOf(newValue));
		}
		
		table.close();
		

		output.collect(key, resultValue);
	}

}
