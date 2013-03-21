package mapred;

import java.io.*;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class PageReducer extends MapReduceBase implements
		Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text arg0, Iterator<Text> arg1,
			OutputCollector<Text, Text> arg2, Reporter arg3) throws IOException {
		
	}

}
