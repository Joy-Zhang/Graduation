package io;


import java.io.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.*;

public class PageNumbererInputFormat extends CombineFileInputFormat<Text, LongWritable> {

	@Override
	public RecordReader<Text, LongWritable> getRecordReader(InputSplit split,
			JobConf job, Reporter reporter) throws IOException {
		if(!(split instanceof CombineFileSplit))
			return null;
		
		CombineFileSplit fileSplit = (CombineFileSplit)split;
		
		
		return new PageNumbererRecordReader(fileSplit, reporter);
	}
	
	private class PageNumbererRecordReader implements RecordReader<Text, LongWritable> {

		public PageNumbererRecordReader(CombineFileSplit split, Reporter reporter) {
			pagePaths = split.getPaths();
			index = 0;
			numberer = reporter.getCounter("page", "number");
		}
		
		private int index;
		private Path[] pagePaths;
		private Counters.Counter numberer;
		@Override
		public void close() throws IOException {}

		@Override
		public Text createKey() {
			return new Text();
		}

		@Override
		public LongWritable createValue() {
			return new LongWritable();
		}

		@Override
		public long getPos() throws IOException {
			return index;
		}

		@Override
		public float getProgress() throws IOException {
			return index;
		}

		@Override
		public boolean next(Text url, LongWritable number) throws IOException {
			if(index >= pagePaths.length)
				return false;
			url.set(pagePaths[index].toString());
			number.set(numberer.getValue());
			numberer.increment(1);
			
			index++;
			return true;
		}

		
	}

}
