package common;

import java.io.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.*;

public class GraphImporter {
	public static final String TABLE_NAME = "graph_sp";
	public static final String COLUMN_NAME = "value";
	public static final String TMP_INPUT = "/tmp/sp_in";
	public static final String SYNC_TABLE_NAME = "sync_sp";
	
	
	public static int importGraph(Path graph) throws Exception	 {
		Configuration conf = HBaseConfiguration.create();
		HBaseAdmin hbase = new HBaseAdmin(conf);
		
		if(hbase.tableExists(TABLE_NAME)) {
			hbase.disableTable(TABLE_NAME);
			hbase.deleteTable(TABLE_NAME);			
		}
		
		if(hbase.tableExists(SYNC_TABLE_NAME)) {
			hbase.disableTable(SYNC_TABLE_NAME);
			hbase.deleteTable(SYNC_TABLE_NAME);
		}
		HTableDescriptor syncTable = new HTableDescriptor(SYNC_TABLE_NAME);
		syncTable.addFamily(new HColumnDescriptor(COLUMN_NAME));
		hbase.createTable(syncTable);
		hbase.close();
		
		RegionSplitter.main(new String[]{"-c", "2", "-f", 
				COLUMN_NAME, TABLE_NAME, "UniformSplit"});


		FileSystem fs = FileSystem.get(conf);
		BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(graph)));
		
		String line =  reader.readLine();
		HTable table = new HTable(conf, TABLE_NAME.getBytes());

		
		if(fs.exists(new Path(TMP_INPUT)))
			fs.delete(new Path(TMP_INPUT), true);
		FSDataOutputStream tmpInput = fs.create(new Path(TMP_INPUT));
		int src = 0;
		while (line != null) {
			String[] values = line.trim().split(" ");
			int dest = 0;
			for(String value : values) {
				Put put = new Put(
						String.format("%d %d", src, dest).getBytes())		;
				put.add(COLUMN_NAME.getBytes(), "".getBytes(), value.getBytes());
				table.put(put);
				tmpInput.write(String.format("%d %d\t%s\n", src, dest, value).getBytes());
				dest++;
				
			}

			line = reader.readLine();
			src++;
			
		}
		reader.close();
		
		

		
		table.flushCommits();
		table.close();
		tmpInput.close();
		fs.close();
		return src;

	}
}
