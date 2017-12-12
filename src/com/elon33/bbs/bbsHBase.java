package com.elon33.bbs;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class bbsHBase extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		final Configuration configuration = new Configuration();
		// 设置zookeeper
		configuration.set("hbase.zookeeper.quorum", "hadoop");
		// 设置hbase表名称
		configuration.set(TableOutputFormat.OUTPUT_TABLE, "bbs_log");// 先在shell下创建一个表：create
		// 将该值改大，防止hbase超时退出
		configuration.set("dfs.socket.timeout", "180000");

		final Job job = new Job(configuration, "bbsHBaseBatchImport");
		job.setJarByClass(bbsHBase.class);  //是否打jar包运行
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		// 设置map的输出类型，不设置reduce的输出类型
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		
		// 不再设置输出路径，而是设置输出格式类型
		job.setOutputFormatClass(TableOutputFormat.class);

		FileInputFormat.setInputPaths(job, args[0]);  // 设置输入文件为mapreduce中已经清洗过的文件
		job.waitForCompletion(true);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new bbsHBase(), args);
	}
	
	static class MyMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
		Text v2 = new Text();
		public static final SimpleDateFormat dateformat = new SimpleDateFormat("yyyyMMddHHmmss");
		public static final SimpleDateFormat dateformat1 = new SimpleDateFormat("yyyyMMdd");

		protected void map(LongWritable key, Text value,
				org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, LongWritable, Text>.Context context)
						throws java.io.IOException, InterruptedException {
			final String[] parsed = value.toString().split("\t");
			if(parsed.length==3){
				Date parseDate = null;
				String time1 = "";
				try {
					parseDate = dateformat.parse(parsed[1]);
					time1 = dateformat1.format(parseDate);
				} catch (ParseException e) {
					e.printStackTrace();
				}
				String rowKey = parsed[0] + ":" + time1;// 设置行键：ip+time(只保留日期，去除时分秒)
				v2.set(rowKey + "\t" + parsed[0] + "\t" + parsed[1] + "\t" + parsed[2]); // ip+time	ip 	time  url
				context.write(key, v2);
			}else{
				return;
			}
		};
	}

	// 数据按行键和值 存入HBase中
	static class MyReducer extends TableReducer<LongWritable, Text, NullWritable> {
		protected void reduce(LongWritable k2, java.lang.Iterable<Text> v2s, Context context)
				throws java.io.IOException, InterruptedException {
			for (Text v2 : v2s) {
				final String[] splited = v2.toString().split("\t");
				final Put put = new Put(Bytes.toBytes(splited[0])); // 第一列行键
				put.add(Bytes.toBytes("cf"), Bytes.toBytes("date"), Bytes.toBytes(splited[1])); // 第二列IP
				put.add(Bytes.toBytes("cf"), Bytes.toBytes("time"), Bytes.toBytes(splited[2])); // 第三列time
				put.add(Bytes.toBytes("cf"), Bytes.toBytes("url"), Bytes.toBytes(splited[3])); // 第四列url
				context.write(NullWritable.get(), put);
			}
		};
	}
}
