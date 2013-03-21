package job;

import java.io.*;


import mapred.*;
import io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapred.*;

public class PageRankJob {

	
	public static void main(String[] args) throws IOException {
		JobConf job = new JobConf(PageRankJob.class);
		job.setJobName("test");
		job.setMapperClass(PageMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setInputFormat(PageInputFormat.class);
		
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		
		
		JobClient.runJob(job);
	}

}
