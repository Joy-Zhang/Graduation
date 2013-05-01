package job;



import mapred.*;


import org.apache.commons.logging.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;



import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.*;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.*;


import common.*;

public class ShortestPathJob {
	public static final String CURRENT_WARSHALL = "warshall";
	public static final Log LOG = LogFactory.getLog(ShortestPathJob.class);

	public static void main(String[] args) throws Exception {
		Path graph = new Path(args[0]);
		int size = GraphImporter.importGraph(graph);


		for(int i = 0; i < size; i++) {

			LOG.info(String.format("Interation : %d", i));
			
			Configuration conf = new Configuration();
			conf.set(CURRENT_WARSHALL, String.valueOf(i));

			Scan scan = new Scan();
			scan.addColumn(GraphImporter.COLUMN_NAME.getBytes(), "".getBytes());
			scan.setCaching(2000);

			Job job = new Job(conf);
			job.setJobName(String.format("Shortest Path %d", i));
			job.setJarByClass(ShortestPathJob.class);
			job.setInputFormatClass(TableInputFormat.class);
			job.setOutputFormatClass(NullOutputFormat.class);

			



			
			TableMapReduceUtil.initTableMapperJob(GraphImporter.TABLE_NAME, 
					scan, WarshallMapper.class, Text.class, Text.class, job);
			job.waitForCompletion(true);
			
		}
	}
}
