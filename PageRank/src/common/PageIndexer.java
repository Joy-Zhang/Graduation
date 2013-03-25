package common;

import java.io.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;


public class PageIndexer {
	public PageIndexer(Configuration conf, Path path) throws IOException {
		input = FileSystem.get(conf).open(path);

		
	}

	private FSDataInputStream input;

	public static final Path DEFAULT_PATH = new Path("/tmp/pages_index") ;
	
	public Text queryPage(LongWritable number) throws IOException {
		input.seek(0);
		BufferedReader db = new BufferedReader(new InputStreamReader(input)); 
		String record = db.readLine();
		while(record != null)
		{
			String[] parts = record.split("\t");
			LongWritable n = new LongWritable(Long.parseLong(parts[1]));
			Text t = new Text(parts[0]);
			if(n.equals(number))
				return t;
			record = db.readLine();
		}			
		return null;
		
	}
	
	public LongWritable queryNumber(Text page) throws IOException {
		input.seek(0);
		BufferedReader db = new BufferedReader(new InputStreamReader(input)); 
		String record = db.readLine();
		while(record != null)
		{
			String[] parts = record.split("\t");
			LongWritable n = new LongWritable(Long.parseLong(parts[1]));
			Text t = new Text(parts[0]);
			if(t.equals(page))
				return n;
			record = db.readLine();
		}		
		return null;		
	}

	public int getCount() throws IOException {
		input.seek(0);
		BufferedReader db = new BufferedReader(new InputStreamReader(input)); 
		String record = db.readLine();
		int result = 0;
		while(record != null)
		{
			result++;
			record = db.readLine();
		}
		return result;
	}
	
	
	public void close() throws IOException {
		input.close();
	}

	@Override
	protected void finalize() throws Throwable {
		close();
		super.finalize();
	}
	
	
}
