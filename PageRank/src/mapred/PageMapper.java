package mapred;

import java.io.*;
import java.util.regex.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;


public class PageMapper extends MapReduceBase implements Mapper<Text, Text, Text, Text> {

	@Override
	public void map(Text url, Text content, OutputCollector<Text, Text> output,
			Reporter reporter) throws IOException {
				
		Pattern link = Pattern.compile("<a *href=\"(.*?)\"", Pattern.MULTILINE);
		Matcher linkMatcher = link.matcher(content.toString());
		while(linkMatcher.find())
			output.collect(url, new Text(linkMatcher.group(1)));
		
		
		
		
	}

}
