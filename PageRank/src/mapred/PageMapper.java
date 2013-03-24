package mapred;

import java.io.*;
import java.util.regex.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.fs.*;


public class PageMapper extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text> {

	@Override
	public void map(LongWritable url, Text content, OutputCollector<LongWritable, Text> output,
			Reporter reporter) throws IOException {
				
		Pattern link = Pattern.compile("<a *href=\"(.*?)\"", Pattern.MULTILINE);
		Matcher linkMatcher = link.matcher(content.toString());
		FileSystem fs = FileSystem.get (new Configuration ());
		
		while(linkMatcher.find())
			output.collect(url, new Text(linkMatcher.group(1)));
		
		
		
		
		
		
	}

}
