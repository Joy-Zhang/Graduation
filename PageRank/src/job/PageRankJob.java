package job;

import java.io.IOException;

import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class PageRankJob {

	
	public static void main(String[] args) throws IOException {
		JobConf job = new JobConf();
		
		
		
		JobClient.runJob(job);
	}

}
