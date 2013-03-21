package mapred;

import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class PageMapper extends MapReduceBase implements Mapper<Text, Text, Text, Text> {

	@Override
	public void map(Text url, Text content, OutputCollector<Text, Text> output,
			Reporter reporter) throws IOException {
		output.collect(url, content);
	}

}
