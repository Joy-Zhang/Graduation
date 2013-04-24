package common;

import java.io.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

public class GraphImporter {
	public static final String TABLE_NAME = "graph_sp";
	public static final String COLUMN_NAME = "value";
	public static final String CURRENT_WARSHALL = "warshall";
	public static int importGraph(Path graph) throws IOException {
		Configuration conf = HBaseConfiguration.create();
		HBaseAdmin hbase = new HBaseAdmin(conf);
		
		if(hbase.tableExists(TABLE_NAME)) {
			hbase.disableTable(TABLE_NAME);
			hbase.deleteTable(TABLE_NAME);			
		}
		HTableDescriptor tableDesc = new HTableDescriptor(TABLE_NAME.getBytes());;
		tableDesc.addFamily(new HColumnDescriptor(COLUMN_NAME.getBytes()));
		hbase.createTable(tableDesc);
		hbase.close();
		
		FileSystem fs = FileSystem.get(conf);
		BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(graph)));
		
		String line =  reader.readLine();
		HTable table = new HTable(conf, TABLE_NAME.getBytes());
		
		int src = 0;
		while (line != null) {
			String[] values = line.split(" ");
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
