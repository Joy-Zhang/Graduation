package job;

import java.io.IOException;

import mapred.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import common.*;

public class BreadthFirstSearchJob {
	public static void main(String[] args) throws IOException {
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
			JobConf conf = new JobConf(BreadthFirstSearchJob.class);
			conf.setMapOutputKeyClass(IntWritable.class);
			conf.setMapOutputValueClass(Text.class);
			conf.setJar("BreadthFirstSearch.jar");
			conf.setMapperClass(VertexAccessMapper.class);
			conf.setReducerClass(VertexAccessReducer.class);
			
			FileInputFormat.setInputPaths(conf, start);
			FileOutputFormat.setOutputPath(conf, result);
			
			
			JobClient.runJob(conf).waitForCompletion();
			FileUtil.copyMerge(fs, start, fs, new Path(output.getName() + Path.SEPARATOR + String.valueOf(i)), 
					true, new Configuration(), null);
			
			fs.rename(result, start);
			i++;
			
		} while(!indexer.isFinished());
		fs.close();
		indexer.close();
	}
}
