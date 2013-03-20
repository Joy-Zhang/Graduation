package io;

import java.io.*;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class PageInputFormat extends InputFormat<Text, Text> {

	@Override
	public RecordReader<Text, Text> createRecordReader(InputSplit arg0,
			TaskAttemptContext arg1) throws IOException, InterruptedException {
		
		return null;
	}

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException,
			InterruptedException {
			
		return null;
	}
	
}
