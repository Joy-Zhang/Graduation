package common;

import java.io.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.RegionSplitter;

public class GraphImporter {
	public static final String TABLE_NAME = "graph_sp";
	public static final String COLUMN_NAME = "value";

	public static int importGraph(Path graph) throws Exception	 {
		Configuration conf = HBaseConfiguration.create();
		HBaseAdmin hbase = new HBaseAdmin(conf);
		
		if(hbase.tableExists(TABLE_NAME)) {
			hbase.disableTable(TABLE_NAME);
			hbase.deleteTable(TABLE_NAME);			
		}

		RegionSplitter.main(new String[]{"-c", "3", "-f", 
				COLUMN_NAME, TABLE_NAME, "UniformSplit"});
		
		FileSystem fs = FileSystem.get(conf);
		BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(graph)));
		
		String line =  reader.readLine();
		HTable table = new HTable(conf, TABLE_NAME.getBytes());
		
		int src = 0;
		while (line != null) {
			String[] values = line.trim().split(" ");
			int dest = 0;
			for(String value : values) {
				Put put = new Put(
						String.format("%d %d", src, dest).getBytes())		;
				put.add(COLUMN_NAME.getBytes(), "".getBytes(), value.getBytes());
				table.put(put);
				dest++;
				
			}
			line = reader.readLine();
			src++;

		}
		reader.close();
		
		

		
		table.flushCommits();
		table.close();

		
		return src;

	}
}
