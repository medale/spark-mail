package com.uebercomputing.hadoop;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.uebercomputing.mailrecord.MailRecord;

public class FolderAnalyticsDriver extends Configured implements Tool {

	public static class FolderAnalyticsMapper extends
			Mapper<AvroKey<MailRecord>, NullWritable, Text, Text> {

		private Text userName = new Text();
		private Text folderName = new Text();

		@Override
		public void map(AvroKey<MailRecord> key, NullWritable value,
				Context context) throws IOException, InterruptedException {
			MailRecord mailRecord = key.datum();
			Map<String, String> mailFields = mailRecord.getMailFields();
			String userNameStr = mailFields.get("UserName");
			String folderNameStr = mailFields.get("FolderName");
			if (userNameStr != null && folderNameStr != null) {
				userName.set(userNameStr);
				folderName.set(folderNameStr);
				context.write(userName, folderName);
			}
		}
	}

	public static class FolderAnalyticsReducer extends
			Reducer<Text, Text, Text, NullWritable> {

		private double totalNumberOfFolders = 0;
		private long totalUsers = 0;
		private long maxCount = 0;
		private long minCount = Long.MAX_VALUE;
		private String maxUserName = "";

		@Override
		public void reduce(Text userName, Iterable<Text> folderNames,
				Context context) throws IOException, InterruptedException {
			Set<String> uniqueFoldersPerUser = new HashSet<String>();
			for (Text folderName : folderNames) {
				uniqueFoldersPerUser.add(folderName.toString());
			}
		
		  int count = uniqueFoldersPerUser.size();
		  if (count > maxCount) { 
			  maxCount = count;
			  maxUserName = userName.toString();
		  }
		  if (count < minCount){
			  minCount = count;
		  }
		  totalNumberOfFolders += count;
		  totalUsers++;
		}

		@Override
		public void cleanup(Context context) throws IOException,
				InterruptedException {
			double avgFolderCountPerPartition = totalNumberOfFolders / totalUsers;
			
			String resultStr = "AvgPerPart=" + avgFolderCountPerPartition + 
					"\tTotalFolders=" +  totalNumberOfFolders +
					"\tTotalUsers=" + totalUsers +
					"\tMaxCount=" + maxCount +
					"\tMaxUser=" + maxUserName +
					"\tMinCount=" + minCount;
			Text resultKey = new Text(resultStr);
			context.write(resultKey, NullWritable.get());
		}

	}

	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new Configuration(),
				new FolderAnalyticsDriver(), args);
		System.exit(result);
	}

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		// ensure Avro 1.7.6 library is used instead of Hadoop 2.6's 1.7.4
		// mapreduce.job.user.classpath.first
		conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);

		Job job = Job.getInstance(conf);
		job.setJobName("FolderAnalytics");
		job.setJarByClass(FolderAnalyticsDriver.class);

		FileInputFormat.addInputPath(job, new Path("enron.avro"));
		FileOutputFormat.setOutputPath(job, new Path("folderAnalytics"));

		job.setInputFormatClass(AvroKeyInputFormat.class);
		job.setMapperClass(FolderAnalyticsMapper.class);
		job.setReducerClass(FolderAnalyticsReducer.class);

		AvroJob.setInputKeySchema(job, MailRecord.getClassSchema());

		// map output not the same as reducer output (default)
		// so we must explicitly set
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputFormatClass(TextOutputFormat.class);
		
		return (job.waitForCompletion(true) ? 0 : 1);
	}
}
