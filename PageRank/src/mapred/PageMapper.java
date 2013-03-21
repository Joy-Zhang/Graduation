package mapred;

import java.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class PageMapper extends MapReduceBase implements Mapper<Path, Text, Text, Text> {

	@Override
	public void map(Path url, Text content, OutputCollector<Text, Text> output,
			Reporter reporter) throws IOException {
		output.collect(new Text(url.getName()), content);
	}

}
