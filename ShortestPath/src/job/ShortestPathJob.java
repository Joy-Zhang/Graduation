package job;


import mapred.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;



import org.apache.hadoop.hbase.mapreduce.*;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.*;


import common.*;

public class ShortestPathJob {
	public static final String CURRENT_WARSHALL = "warshall";
	public static void main(String[] args) throws Exception {
		Path graph = new Path(args[0]);
		int size = GraphImporter.importGraph(graph);

		
		for(int i = 0; i < size; i++) {

			Configuration conf = new Configuration();
			conf.set(CURRENT_WARSHALL, String.valueOf(i));

			conf.set(TableInputFormat.INPUT_TABLE, GraphImporter.TABLE_NAME);
			Job job = new Job(conf);
			job.setJarByClass(ShortestPathJob.class);
			job.setInputFormatClass(TableInputFormat.class);
			job.setOutputFormatClass(NullOutputFormat.class);
			job.setMapperClass(WarshallMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			

			job.waitForCompletion(true);
			
		}
	}
}
