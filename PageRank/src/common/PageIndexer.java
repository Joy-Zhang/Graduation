package common;

import java.io.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

public class PageIndexer {
	public PageIndexer(Configuration conf, Path path) throws IOException {
		db = new BufferedReader(new InputStreamReader(FileSystem.get(conf).open(path)));
		
	}

	private BufferedReader db;
	
	public Text queryPage(LongWritable number) throws IOException {
		
		String record = db.readLine();
		while(record != null)
		{
			String[] parts = record.split("\t");
			LongWritable n = new LongWritable(Long.parseLong(parts[1]));
			Text t = new Text(parts[0]);
			if(n.equals(number))
				return t;
			
		}			
		return null;
		
	}
	
	
	public LongWritable queryNumber(Text page) throws IOException {
		String record = db.readLine();
		while(record != null)
		{
			String[] parts = record.split("\t");
			LongWritable n = new LongWritable(Long.parseLong(parts[1]));
			Text t = new Text(parts[0]);
			if(t.equals(page))
				return n;
			
		}			
		return null;		
	}

	public void close() throws IOException {
		db.close();
		
	}

	@Override
	protected void finalize() throws Throwable {
		close();
		super.finalize();
	}
	
	
}
