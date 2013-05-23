package job;



import mapred.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import common.*;

public class BreadthFirstSearchJob {
	public static void main(String[] args) throws Exception {
		//System.out.println("\n\nNew Job");
		GraphIndexer indexer = new GraphIndexer();
		indexer.init();
		Path start = new Path("/tmp/start");
		Path result = new Path("/tmp/result");
		//System.out.print("\n\n\nNew BFS");
		Path output = new Path(args[1]);
		
		FileSystem fs = FileSystem.get(new Configuration());
		fs.delete(start, true);
		fs.delete(result, true);
		fs.mkdirs(start);
		fs.mkdirs(output);
		
		FileUtil.copy(fs, new Path(args[0]), fs, start, false, new Configuration());
		int i = 0;
		do {
			Job job = new Job();
			job.setJobName(String.format("BFS Level %d", i));
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(Text.class);
			job.setJarByClass(BreadthFirstSearchJob.class);
			job.setMapperClass(VertexAccessMapper.class);
			job.setReducerClass(VertexAccessReducer.class);
			FileInputFormat.setMaxInputSplitSize(job, 1000);
			


			FileInputFormat.setInputPaths(job, start);
			FileOutputFormat.setOutputPath(job, result);
			
			
			job.waitForCompletion(true);
			FileUtil.copyMerge(fs, start, fs, new Path(output.getName() + Path.SEPARATOR + String.valueOf(i)), 
					true, new Configuration(), null);
			
			fs.rename(result, start);
			i++;
			
		} while(!indexer.isFinished());
		fs.close();
		indexer.close();
	}
}
