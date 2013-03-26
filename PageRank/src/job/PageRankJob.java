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
		
		
		numberPages.setJobName("number pages");
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
        Path matrix = new Path("/tmp/matrix");
        
        
        JobConf analyzePages = new JobConf(PageRankJob.class);
        analyzePages.setJar("PageRank.jar");
        analyzePages.setJobName("analyze pages");
        analyzePages.setMapperClass(PageMapper.class);
        analyzePages.setInputFormat(PageInputFormat.class);
        analyzePages.setReducerClass(PageReducer.class);
        analyzePages.setMapOutputKeyClass(Text.class);
		FileInputFormat.setInputPaths(analyzePages, new Path(args[0]));
        FileOutputFormat.setOutputPath(analyzePages, matrix);
        analyzePages.setJarByClass(PageRankJob.class);
        JobClient.runJob(analyzePages).waitForCompletion();
        
        
        
        Path pageRankVector = new Path("/tmp/page_rank");
        PageRankVector vector = new PageRankVector(new Configuration(), pageRankVector);
        
        Path lastResult = new Path("/tmp/page_rank_new");
        PageRankVector vectorNew = new PageRankVector(new Configuration(), lastResult);
        vector.init();

        double deltaVectorNorm = 0.0;
        do {
            JobConf pageRank = new JobConf(PageRankJob.class);
            pageRank.setJar("PageRank.jar");
            pageRank.setJobName("page rank");
            pageRank.setMapperClass(PageRankMapper.class);
            pageRank.setReducerClass(PageRankReducer.class);
            pageRank.setMapOutputKeyClass(LongWritable.class);
            pageRank.setMapOutputValueClass(Text.class);
            
            
    		FileInputFormat.setInputPaths(pageRank, matrix);
            FileOutputFormat.setOutputPath(pageRank, new Path("/tmp/vector"));    
	        JobClient.runJob(pageRank).waitForCompletion();
	        FileUtil.copyMerge(fs, new Path("/tmp/vector/"), fs, lastResult, true, pageRank, null);
	        PageRankVector.PageRankVectorIterator vectorIterator = vector.iterator();
	        PageRankVector.PageRankVectorIterator vectorIteratorNew = vectorNew.iterator();
	        deltaVectorNorm = 0.0;
	        while(vectorIterator.hasNext() && vectorIteratorNew.hasNext()) {
	        	PageRankVector.PageRankPair pair = vectorIterator.next();
	        	PageRankVector.PageRankPair pairNew = vectorIteratorNew.next();
	        	FileInputFormat.LOG.error(new Boolean(pair.getPage().equals(pairNew.getPage())));
	        	
	        	deltaVectorNorm += Math.pow((pair.getPageRank().get() - pairNew.getPageRank().get()), 2);
	        }
	        vector.close();
	        vectorNew.close();
	        fs.rename(lastResult, pageRankVector);
	        FileInputFormat.LOG.error(new Double(deltaVectorNorm));
	    } while(deltaVectorNorm > 0.0001);
        fs.delete(matrix, true);
        fs.delete(PageIndexer.DEFAULT_PATH, true);
        fs.close();
        

	}

}
