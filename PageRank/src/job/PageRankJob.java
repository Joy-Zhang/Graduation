package job;

import java.io.*;


import mapred.*;
import io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.*;

public class PageRankJob {

	
	public static void main(String[] args) throws IOException {
		JobConf numberPages = new JobConf(PageRankJob.class);
		
		
		numberPages.setJobName("number pages");
		numberPages.setMapOutputKeyClass(Text.class);
		numberPages.setMapOutputValueClass(LongWritable.class);
		numberPages.setMapperClass(IdentityMapper.class);
		numberPages.setInputFormat(PageNumbererInputFormat.class);
		FileInputFormat.setInputPaths(numberPages, new Path(args[0]));
        FileOutputFormat.setOutputPath(numberPages, new Path("/tmp/pages_number"));
        JobClient.runJob(numberPages);
		
        FileSystem fs = FileSystem.get(numberPages);
        FileUtil.copyMerge(fs, new Path("/tmp/pages_number"), fs, new Path("/tmp/pages_index"), true, numberPages, null);
        fs.close();
        
        
        JobConf analyzePages = new JobConf(PageRankJob.class);
        analyzePages.setMapperClass(PageMapper.class);
        analyzePages.setInputFormat(PageInputFormat.class);
        //analyzePages.setReducerClass(PageReducer.class);
		FileInputFormat.setInputPaths(analyzePages, new Path(args[0]));
        FileOutputFormat.setOutputPath(analyzePages, new Path(args[1]));
        JobClient.runJob(analyzePages);
        
	}

}
