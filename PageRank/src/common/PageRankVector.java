package common;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.FileInputFormat;


public class PageRankVector {

	
	public PageRankVector(Configuration conf, Path path) throws IOException {
		vectorFilePath = path;
		fs = FileSystem.get(conf);
				
	}
	
	private Path vectorFilePath;
	private FileSystem fs;
	private LinkedList<InputStream> sessions = new LinkedList<InputStream>();
	
	public synchronized void init() throws IOException {
		PageIndexer indexer = new PageIndexer(new Configuration(), PageIndexer.DEFAULT_PATH);
		int count = indexer.getCount();
		FSDataOutputStream output = fs.create(vectorFilePath, true);
		for(int i = 0; i < count; i++)
		{
			PageRankPair pair = new PageRankPair(new LongWritable(i), new DoubleWritable(1.0 / count));
			
			output.writeBytes(pair.toString());
			if (i != count - 1)
				output.write('\n');
		}
		indexer.close();
		output.close();
	}

	
	public PageRankVectorIterator iterator() throws IOException {
		FSDataInputStream input = fs.open(vectorFilePath);
		sessions.add(input);
		return new PageRankVectorIterator(input);
	}
	
	public void close() throws IOException {
		fs.close();
		for(InputStream input : sessions) 
			input.close();
	}
	
	
	@Override
	protected void finalize() throws Throwable {
		close();
		super.finalize();
	}


	public class PageRankPair {
		private LongWritable page;
		private DoubleWritable pageRank;
		public LongWritable getPage() {
			return page;
		}
		public void setPage(LongWritable page) {
			this.page = page;
		}
		public DoubleWritable getPageRank() {
			return pageRank;
		}
		public void setPageRank(DoubleWritable pageRank) {
			this.pageRank = pageRank;
		}
		public PageRankPair(LongWritable page, DoubleWritable pageRank) {
			super();
			this.page = page;
			this.pageRank = pageRank;
		}
		@Override
		public String toString() {
			return page.toString() + "\t" + pageRank.toString();
		}
		
		
	}
	
	
	public class PageRankVectorIterator {

		public PageRankVectorIterator(FSDataInputStream input) {
			reader = new BufferedReader(new InputStreamReader(input));
		}
		
		private BufferedReader reader;
		private String cache;
		
		public boolean hasNext() throws IOException {
			
			cache = reader.readLine();
			return cache != null;
		}

		public PageRankPair next() {
			String[] parts = cache.split("\t");
			String s = parts[0];
			FileInputFormat.LOG.error(s);
			long src = Long.parseLong(s);
			PageRankPair pair = new PageRankPair(
					new LongWritable(src), 
					new DoubleWritable(Double.parseDouble(parts[1])));
			return pair;
		}


		
	}
}
