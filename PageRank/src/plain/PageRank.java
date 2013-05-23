package plain;

import io.PageRankOutputFormat;

import java.io.*;

import java.util.regex.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.io.*;


import common.*;

public class PageRank {
	public static void main(String[] args) throws IOException { 
		FileSystem fs = FileSystem.get(new Configuration());
		Path[] pages = FileUtil.stat2Paths(fs.listStatus(new Path(args[0]), new PathFilter(){
			@Override
			public boolean accept(Path path) {
				return path.getName().endsWith(".html");
			}
			
		}));
		FSDataOutputStream number = fs.create(PageIndexer.DEFAULT_PATH);
		for(int i = 0; i < pages.length; i++) {
			number.write(String.format("%s\t%d\n", pages[i].getName(), i).getBytes());
		}
		number.flush();
		number.close();
		double[][] matrix = new double[pages.length][pages.length];
		
		PageIndexer indexer = new PageIndexer(new Configuration(), PageIndexer.DEFAULT_PATH);
		Pattern link = Pattern.compile("<a *href=\"(.*?)\"", Pattern.MULTILINE);

		for(Path page : pages) {
			InputStream input = fs.open(page);
			byte[] data = new byte[input.available()];
			input.read(data);
			input.close();
			Matcher linkMatcher = link.matcher(new String(data));
			int src = (int)indexer.queryNumber(new Text(page.getName())).get();
			
			for(int i = 0; i < pages.length; i++) {
				matrix[src][i] = 0.0;
			}
			while(linkMatcher.find()) {
				int dest = (int)indexer.queryNumber(new Text(linkMatcher.group(1))).get();
				matrix[src][dest] = 1;
			}
			double sum = 0;
			for(int i = 0; i < pages.length; i++) {
				sum += matrix[src][i];
			}
			for(int i = 0; i < pages.length; i++) {
				matrix[src][i] /= sum;
			}
			
		}
		PageRankOutputFormat.init();
		double d = 1;
		HTable t = new HTable(HBaseConfiguration.create(), 
				PageRankOutputFormat.TABLE_NAME);
		do {
			for(int i = 0; i < pages.length; i++)
			{
				double sum = 0;
				for(int j = 0; j < pages.length; j++) {
					Get get = new Get(String.valueOf(j).getBytes());
					Result r = t.get(get);
					double val = Double.parseDouble(new String(r.getValue(
							PageRankOutputFormat.COLUMN_NAME.getBytes(), "old".getBytes())));
					sum += (matrix[j][i] * val);
					
				}
				Put put = new Put(String.valueOf(i).getBytes());
				put.add(PageRankOutputFormat.COLUMN_NAME.getBytes(), "new".getBytes(), 
						String.valueOf(sum).getBytes());
				t.put(put);
			}
			d = PageRankOutputFormat.deltaVectorNorm();
			t.flushCommits();
		} while(d > 0.00001);
		t.close();
		indexer.close();
		
		fs.close();
	}
}
