package mapred;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import common.*;

public class PageReducer extends MapReduceBase implements
		Reducer<Text, Text, PageReducer.PagePair, DoubleWritable> {

	public class PagePair {
		private LongWritable src = new LongWritable();
		private LongWritable dest = new LongWritable();
		public PagePair(LongWritable src, LongWritable dest) {
			super();
			this.src = src;
			this.dest = dest;
		}
		public LongWritable getSrc() {
			return src;
		}
		public void setSrc(LongWritable src) {
			this.src = src;
		}
		public LongWritable getDest() {
			return dest;
		}
		public void setDest(LongWritable dest) {
			this.dest = dest;
		}
		@Override
		public String toString() {
			
			return src.toString() + "\t" + dest.toString();
		}
	}
	
	
	@Override
	public void reduce(Text src, Iterator<Text> dest,
			OutputCollector<PagePair, DoubleWritable> output, Reporter reporter) throws IOException {
		PageIndexer indexer = new PageIndexer(new Configuration(), PageIndexer.DEFAULT_PATH);
		
		LongWritable srcNumer = indexer.queryNumber(src);
		double[] values = new double[indexer.getCount()];
		
		for(int i = 0; i < values.length; i++) 
			values[i] = 0.0;
		int count = 0;
		while(dest.hasNext()) {
			LongWritable destNumber = indexer.queryNumber(dest.next());
			values[(int) destNumber.get()] = 1.0;
			count++;
		}
		for(int i = 0; i < values.length; i++)
		{	
			output.collect(new PagePair(srcNumer, new LongWritable(i)),
					new DoubleWritable(values[i] / count));
		}
		indexer.close();
	}

}
