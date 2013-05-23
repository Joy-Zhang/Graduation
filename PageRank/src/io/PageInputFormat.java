package io;

import java.io.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.*;


public class PageInputFormat extends CombineFileInputFormat<Text, Text> {

	
	
	@Override
	public RecordReader<Text, Text> getRecordReader(InputSplit split,
			JobConf job, Reporter reporter) throws IOException {
			
		if(!(split instanceof CombineFileSplit))
			return null;
		
		CombineFileSplit fileSplit = (CombineFileSplit)split;
		
		
		return new  PageRecordReader(fileSplit, job, reporter);
	}
	private class PageRecordReader implements RecordReader<Text, Text> {

		public PageRecordReader(CombineFileSplit split, JobConf job, Reporter reporter) throws IOException {
			pagePaths = split.getPaths();
			index = 0;

			fs = FileSystem.get(job);
		}
		
		private Path[] pagePaths;
		private int index;
		private FileSystem fs;

		@Override
		public void close() throws IOException {
			fs.close();
		}

		@Override
		public Text createKey() {
			return new Text();
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
		public boolean next(Text page, Text content) throws IOException {
			if(index >= pagePaths.length)
				return false;
			
			Path filePath = pagePaths[index];
			
			page.set(filePath.getName());
			
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
