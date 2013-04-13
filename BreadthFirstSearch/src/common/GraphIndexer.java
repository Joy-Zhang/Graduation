package common;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;


public class GraphIndexer {

	
	

	public static final String HBASE_TABLE_NAME = "graph_bfs";
	
	public static final String NEIGHBOURHOOD = "neighbourhood";
	public static final String ACCESS = "access";
	
	public static final Configuration HBASE_CONFIGURATION = HBaseConfiguration.create();
	public void init() throws IOException {
		HBaseAdmin hbase = new HBaseAdmin(HBASE_CONFIGURATION);
		if(!hbase.tableExists(HBASE_TABLE_NAME)) {
			throw new IOException("Graph table doesn't exist");
 		}
		


		List<Put> puts = new ArrayList<Put>();
		ResultScanner scanner = table.getScanner(new Scan());

		for(Result result : scanner) {
			Put put = new Put(result.getRow());
			put.add("access".getBytes(), "".getBytes(), String.valueOf(false).getBytes());
			puts.add(put);
		}
		table.put(puts);
		table.flushCommits();


	}
	
	public GraphIndexer() throws IOException {


	} 
	
	private HTable table = new HTable(HBASE_CONFIGURATION, HBASE_TABLE_NAME);
	
	
	public int[] queryNeighbours(int vertex) throws IOException {
		Get get = new Get(String.valueOf(vertex).getBytes());
		Result result = table.get(get);
		String value = new String(
				result.getValue(NEIGHBOURHOOD.getBytes(), "".getBytes()));
		String[] neighbourhood = value.split(" ");
		int[] neighbours = new int[neighbourhood.length];
		for (int i = 0; i < neighbours.length; i++) {
			neighbours[i] = Integer.parseInt(neighbourhood[i]);
		}
		return neighbours;
	}
	
	
	public boolean isAccessed(int vertex) throws IOException {
		Get get = new Get(String.valueOf(vertex).getBytes());
		
		Result result = table.get(get);

		String accessed = new String(result.getValue(ACCESS.getBytes(), "".getBytes()));
		return Boolean.parseBoolean(accessed);
				
				
	}
	
	public void markupAccessed(int vertex) throws IOException {
		Put put = new Put(String.valueOf(vertex).getBytes());
		put.add(ACCESS.getBytes(), "".getBytes(), String.valueOf("true").getBytes());
			
		table.put(put);

	}
	
	public boolean isFinished() throws IOException {
		ResultScanner scanner = table.getScanner(new Scan());
		boolean result = true;
		
		for(Result row : scanner) {
			result &= Boolean.parseBoolean(
					new String(row.getValue(ACCESS.getBytes(), "".getBytes())));
		}
		
		
		
		return result;
	}
	
	
	public void close() throws IOException {
		table.flushCommits();		
		table.close();
	}

	
	@Override
	protected void finalize() throws Throwable {
		close();
		super.finalize();
	}
	
	
}
