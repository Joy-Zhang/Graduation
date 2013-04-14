package io;

import java.io.IOException;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import common.PageIndexer;

public class PageRankOutputFormat implements OutputFormat<LongWritable, DoubleWritable> {

	public static final String TABLE_NAME = "page_rank";	
	public static final String COLUMN_NAME = "value";
	
	public static void init() throws IOException {
		Configuration conf = HBaseConfiguration.create();
		HBaseAdmin hbase = new HBaseAdmin(conf);
		if(hbase.tableExists(TABLE_NAME)) {
			hbase.disableTable(TABLE_NAME);
			hbase.deleteTable(TABLE_NAME);
		}
		
		HTableDescriptor tableDesc = new HTableDescriptor(TABLE_NAME);
		HColumnDescriptor column = new HColumnDescriptor(COLUMN_NAME);
		tableDesc.addFamily(column);
		hbase.createTable(tableDesc);
		hbase.close();
		HTable table = new HTable(conf, TABLE_NAME.getBytes());
		PageIndexer indexer = new PageIndexer(new Configuration(), PageIndexer.DEFAULT_PATH);
		int count = indexer.getCount();
		for(int i = 0; i < count; i++) {
			Put put = new Put(String.valueOf(i).getBytes());
			put.add(COLUMN_NAME.getBytes(), "old".getBytes(), String.valueOf(1.0 / count).getBytes());
			table.put(put);
		}
		table.flushCommits();
		table.close();
		
	}

	public static double deltaVectorNorm() throws IOException {
		HTable table = new HTable(HBaseConfiguration.create(), TABLE_NAME.getBytes());
		ResultScanner scan = table.getScanner(COLUMN_NAME.getBytes());
		double norm = 0;

		for(Result row : scan) {
			double newRank = Double.parseDouble(new String(row.getValue(COLUMN_NAME.getBytes(), "new".getBytes())));
			double oldRank = Double.parseDouble(new String(row.getValue(COLUMN_NAME.getBytes(), "old".getBytes())));
			norm += Math.pow(newRank - oldRank, 2);
			Put put = new Put(row.getRow());
			put.add(COLUMN_NAME.getBytes(), "old".getBytes(), String.valueOf(newRank).getBytes());
			table.put(put);
		}
		table.flushCommits();
		return Math.sqrt(norm);
	}
	
	@Override
	public RecordWriter<LongWritable, DoubleWritable> getRecordWriter(
			FileSystem ignored, JobConf job, String name, Progressable progress)
			throws IOException {

		
		
		return new PageRankRecordWriter(job);
	}

	@Override
	public void checkOutputSpecs(FileSystem ignored, JobConf job)
			throws IOException {



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
