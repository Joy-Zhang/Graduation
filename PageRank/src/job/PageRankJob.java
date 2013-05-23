package job;

import java.io.*;


import mapred.*;
import io.*;


import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.*;

import common.*;

public class PageRankJob {

	
	public static void main(String[] args) throws IOException {

		JobConf numberPages = new JobConf(PageRankJob.class);
		
		
		numberPages.setJobName("PR-number pages");
        numberPages.setJar("PageRank.jar");
		numberPages.setMapOutputKeyClass(Text.class);
		numberPages.setMapOutputValueClass(LongWritable.class);
		numberPages.setMapperClass(IdentityMapper.class);
		numberPages.setInputFormat(PageNumbererInputFormat.class);
		FileInputFormat.setInputPaths(numberPages, new Path(args[0]));
		
		FileOutputFormat.setOutputPath(numberPages, new Path("/tmp/pages_number"));
        JobClient.runJob(numberPages).waitForCompletion();

        FileSystem fs = FileSystem.get(new Configuration());
        FileUtil.copyMerge(fs, new Path("/tmp/pages_number"), fs, PageIndexer.DEFAULT_PATH, true, numberPages, null);

        FileInputFormat.LOG.error("Generated pages number");
        
        Path matrix = new Path("/tmp/matrix");
        JobConf analyzePages = new JobConf(PageRankJob.class);
        analyzePages.setJar("PageRank.jar");
        analyzePages.setJobName("PR-analyze pages");
        analyzePages.setMapperClass(PageMapper.class);
        analyzePages.setInputFormat(PageInputFormat.class);
        analyzePages.setReducerClass(PageReducer.class);
        analyzePages.setMapOutputKeyClass(Text.class);
		FileInputFormat.setInputPaths(analyzePages, new Path(args[0]));
        FileOutputFormat.setOutputPath(analyzePages, matrix);

        JobClient.runJob(analyzePages).waitForCompletion();
        
        FileInputFormat.LOG.error("Generated pages rel matrix");
        
        

        PageRankOutputFormat.init();

        double deltaVectorNorm = 0.0;
        do {

        
        
            JobConf pageRank = new JobConf(PageRankJob.class);
            pageRank.setJar("PageRank.jar");
            pageRank.setJobName("PR-page rank");
            pageRank.setMapperClass(PageRankMapper.class);
            pageRank.setReducerClass(PageRankReducer.class);
            pageRank.setMapOutputKeyClass(LongWritable.class);
            pageRank.setMapOutputValueClass(Text.class);

            pageRank.setOutputFormat(PageRankOutputFormat.class);
    		FileInputFormat.setInputPaths(pageRank, matrix);

    		
    		
    		
    		JobClient.runJob(pageRank).waitForCompletion();
	        
    		deltaVectorNorm = PageRankOutputFormat.deltaVectorNorm();

	    } while(deltaVectorNorm > 0.0001);

        


        

        fs.delete(matrix, true);
        fs.delete(PageIndexer.DEFAULT_PATH, true);

        fs.close();
        

	}

}
