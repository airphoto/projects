package com.hq.hadoop.format;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.codehaus.jackson.map.ObjectMapper;

import com.hq.hadoop.format.model.Message;
import com.hq.hadoop.format.utils.logUtil;

public class FormatTrackingLog {

	public static class FormatMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, NullWritable> {

		private final Text k = new Text();
		private ObjectMapper mapper = new ObjectMapper();

		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, NullWritable> out, Reporter arg3)
				throws IOException {
			// TODO Auto-generated method stub
			String line = value.toString();
			String[] fields = line.split("\t");
			// 前两个字段没有使用
			String timeField = fields[0];
			String adField = fields[1];

			// "hqClientId","hqGroupId","hqAdId","hqCreativeId","hqRefer","hqTime","hqEvent",
			// "hqURL","hqPrice","hqSource","hquid","hqCity","tagId",
			// "channelId"
			String contentField = fields[2];
			try {
				Message model = mapper.readValue(contentField, Message.class);

				if (logUtil.filter(model)) {
					k.set(logUtil.getKeyString(model));
					out.collect(k, NullWritable.get());
				}
			} catch (Exception e) {
				System.err.println(line);//无效的数据打印到stderr中
			}
		}
	}

	/**
	 * args[0]:job名称 args[1]:输入路径 args[2]:输出路径
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		JobConf conf = new JobConf(FormatTrackingLog.class);
		conf.setCompressMapOutput(true);
		try {
			conf.setJobName(args[0]);
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(NullWritable.class);

			conf.setMapperClass(FormatMapper.class);

			FileSystem fs = FileSystem.get(conf);
			Path in = new Path(args[1]);
			Path out = new Path(args[2]);
			if (fs.exists(out)) {
				fs.delete(out, true);
			}
			FileInputFormat.addInputPath(conf, in);
			FileOutputFormat.setOutputPath(conf, out);
			JobClient.runJob(conf);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
