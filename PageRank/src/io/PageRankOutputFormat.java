package io;

import java.io.IOException;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class PageRankOutputFormat implements OutputFormat<LongWritable, DoubleWritable> {

	public static final String TABLE_NAME = "page_rank";	
	public static final String COLUMN_NAME = "value";
	@Override
	public RecordWriter<LongWritable, DoubleWritable> getRecordWriter(
			FileSystem ignored, JobConf job, String name, Progressable progress)
			throws IOException {

		
		
		return new PageRankRecordWriter(job);
	}

	@Override
	public void checkOutputSpecs(FileSystem ignored, JobConf job)
			throws IOException {
		HBaseAdmin hbase = new HBaseAdmin(job);
		if(!hbase.tableExists(TABLE_NAME)) {
			HTableDescriptor table = new HTableDescriptor(TABLE_NAME);
			HColumnDescriptor column = new HColumnDescriptor(COLUMN_NAME);
			table.addFamily(column);
			hbase.createTable(table);
		}
		hbase.close();

	}

	
	public class PageRankRecordWriter implements RecordWriter<LongWritable, DoubleWritable> {

		
		public PageRankRecordWriter(Configuration conf) throws IOException {
			table = new HTable(conf, TABLE_NAME);
		}
		
		
		@Override
		public void write(LongWritable key, DoubleWritable value)
				throws IOException {
			Put put = new Put(String.valueOf(key.get()).getBytes());
			put.add(COLUMN_NAME.getBytes(), "new".getBytes(), String.valueOf(value.get()).getBytes());
			table.put(put);
		}

		@Override
		public void close(Reporter reporter) throws IOException {
			table.flushCommits();
			table.close();
		}
		
		private HTable table;
		
	}
}
