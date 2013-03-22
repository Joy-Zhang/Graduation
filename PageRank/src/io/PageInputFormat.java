package io;

import java.io.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.*;
import common.*;

public class PageInputFormat extends CombineFileInputFormat<LongWritable, Text> {

	@Override
	public RecordReader<LongWritable, Text> getRecordReader(InputSplit split,
			JobConf job, Reporter reporter) throws IOException {
			
		if(!(split instanceof CombineFileSplit))
			return null;
		
		CombineFileSplit fileSplit = (CombineFileSplit)split;
		
		
		return new  PageRecordReader(fileSplit, job, reporter);
	}
	private class PageRecordReader implements RecordReader<LongWritable, Text> {

		public PageRecordReader(CombineFileSplit split, JobConf job, Reporter reporter) throws IOException {
			pagePaths = split.getPaths();
			index = 0;

			indexer = new PageIndexer(job, new Path("/tmp/pages_index"));
			fs = FileSystem.get(job);
		}
		
		private Path[] pagePaths;
		private int index;
		private FileSystem fs;
		private PageIndexer indexer;
		@Override
		public void close() throws IOException {
			indexer.close();
			fs.close();
		}

		@Override
		public LongWritable createKey() {
			return new LongWritable();
		}

		@Override
		public Text createValue() {
			return new Text();
		}

		@Override
		public long getPos() throws IOException {
			
			return index;
		}

		@Override
		public float getProgress() throws IOException {
			
			return index * 1.0f / pagePaths.length;
		}

		@Override
		public boolean next(LongWritable page, Text content) throws IOException {
			if(index >= pagePaths.length)
				return false;
			
			Path filePath = pagePaths[index];
			
			page.set(indexer.queryNumber(new Text(filePath.toString())).get());
			
			InputStream input = fs.open(filePath);
			byte[] data = new byte[input.available()];
			input.read(data);
			input.close();
			content.set(data);
			
			index++;
			return true;
		}
		
	}


	
}
