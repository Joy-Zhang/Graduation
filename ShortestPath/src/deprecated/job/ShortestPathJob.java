package deprecated.job;








import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import org.apache.hadoop.mapred.lib.*;

import common.GraphImporter;

import deprecated.mapred.*;

public class ShortestPathJob {
	public static final String CURRENT_WARSHALL = "warshall";

	public static void main(String[] args) throws Exception {

		
		Path graph = new Path(args[0]);
		int size = GraphImporter.importGraph(graph);
		JobConf job = new JobConf();
		job.setJar("ShortestPath.jar");
		job.setInputFormat(KeyValueTextInputFormat.class);



		for(int i = 0; i < size; i++) {
			JobConf mapConf = new JobConf(false);
			mapConf.setInt(CURRENT_WARSHALL, i);

			ChainMapper.addMapper(job, WarshallMapper.class, Text.class, Text.class, 
					Text.class, Text.class, false, mapConf);
		}
		FileInputFormat.setInputPaths(job, new Path(GraphImporter.TMP_INPUT));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		JobClient.runJob(job).waitForCompletion();
	}

}
