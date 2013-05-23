package plain;


import org.apache.hadoop.fs.*;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

import common.*;

public class ShortestPath {


	public static void main(String[] args) throws Exception {

		Path graph = new Path(args[0]);
		int size = GraphImporter.importGraph(graph);
		HTable table = new HTable(HBaseConfiguration.create(), GraphImporter.TABLE_NAME);
		
		for(int i = 0; i < size; i++) {
			ResultScanner scanner = table.getScanner(
					GraphImporter.COLUMN_NAME.getBytes(), "".getBytes());
			System.out.println(String.format("Iteration %d", i));
			for(Result row : scanner) {
				String[] pair = (new String(row.getRow())).split(" ");
				double ab = Double.parseDouble(new String(row.getValue(
						GraphImporter.COLUMN_NAME.getBytes(), "".getBytes())));
				
				Get aiGet = new Get(String.format("%s %d", pair[0], i).getBytes());
				Get ibGet = new Get(String.format("%d %s", i, pair[1]).getBytes());
				double ai = Double.parseDouble(new String(table.get(aiGet).getValue(
						GraphImporter.COLUMN_NAME.getBytes(), "".getBytes())));
				double ib = Double.parseDouble(new String(table.get(ibGet).getValue(
						GraphImporter.COLUMN_NAME.getBytes(), "".getBytes())));
				double newab = ai + ib;
				if(newab < ab) {
					Put put = new Put(row.getRow());
					put.add(GraphImporter.COLUMN_NAME.getBytes(), "".getBytes(),
							String.valueOf(newab).getBytes());
					table.put(put);
				}
			}
	
		}
		table.flushCommits();
		table.close();
	}

}
