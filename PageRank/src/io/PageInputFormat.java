package io;

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.*;

public class PageInputFormat extends CombineFileInputFormat<Path, Text> {

	@Override
	public RecordReader<Path, Text> getRecordReader(InputSplit split,
			JobConf job, Reporter reporter) throws IOException {
			
		if(!(split instanceof CombineFileSplit))
			return null;
		
		CombineFileSplit fileSplit = (CombineFileSplit)split;
		
		return new  PageRecordReader(fileSplit.getPaths(), job);
	}
	private class PageRecordReader implements RecordReader<Path, Text> {

		public PageRecordReader(Path[] paths, JobConf job) throws IOException {
			pagePaths = paths;
			index = 0;
			filesData = new ArrayList<byte[]>();
			
			FileSystem fs = FileSystem.get(job);
			
			for(int i = 0; i < paths.length; i++) {
				FSDataInputStream input = fs.open(paths[i]);		
				byte[] data = new byte[input.available()];
				input.read(data);
				filesData.add(data);
				input.close();
			}
			
			
			
		}
		
		private Path[] pagePaths = null;
		private int index = 0;

		ArrayList<byte[]> filesData = null;
		
		@Override
		public void close() throws IOException {
			
		}

		@Override
		public Path createKey() {
			
			return pagePaths[index];
		}

		@Override
		public Text createValue() {
			return new Text(filesData.get(index));
		}

		@Override
		public long getPos() throws IOException {
			
			return 0;
		}

		@Override
		public float getProgress() throws IOException {
			
			return index * 1.0f / pagePaths.length;
		}

		@Override
		public boolean next(Path path, Text text) throws IOException {
			return index < pagePaths.length;
		}
		
	}


	
}
