package mapred;

import java.io.*;

import job.ShortestPathJob;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.*;
import org.apache.hadoop.hbase.mapreduce.*;

import org.apache.hadoop.io.*;

import common.GraphImporter;





public class WarshallMapper extends TableMapper<Text, Text>{

	@Override
	protected void map(ImmutableBytesWritable key, Result value,
			Context context)
			throws IOException, InterruptedException {

		int mid = Integer.parseInt(
				context.getConfiguration().get(ShortestPathJob.CURRENT_WARSHALL));
		String[] pair = (new String(key.get())).split(" ");
		int src = Integer.parseInt(pair[0]);
		int dest = Integer.parseInt(pair[1]);

		HTable table  = new HTable(HBaseConfiguration.create(), GraphImporter.TABLE_NAME);
		
		Result r1 = table.get(new Get(String.format("%d %d", src, mid).getBytes()));
		Result r2 = table.get(new Get(String.format("%d %d", mid, dest).getBytes()));
		
		double add1 = Double.parseDouble(new String(
				r1.getValue(GraphImporter.COLUMN_NAME.getBytes(), "".getBytes())));
		double add2 = Double.parseDouble(new String(
				r2.getValue(GraphImporter.COLUMN_NAME.getBytes(), "".getBytes())));
		double origin = Double.parseDouble(new String(
				value.getValue(GraphImporter.COLUMN_NAME.getBytes(), "".getBytes())));
		
		double newValue = add1 + add2; 
		
		if(newValue < origin){
			Put put = new Put(key.get());
			put.add(GraphImporter.COLUMN_NAME.getBytes(), "".getBytes(), 
					String.valueOf(newValue).getBytes());
			table.put(put);
			table.flushCommits();
		}
		
		table.close();



	}



	
	

}
