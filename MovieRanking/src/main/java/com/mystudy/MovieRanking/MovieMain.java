package com.mystudy.MovieRanking;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MovieMain {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		args = new String[] {"E:\\金峰\\学习\\大三\\Hadoop\\Hadoop部署实践\\Film.json","E:\\金峰\\学习\\大三\\Hadoop\\Hadoop部署实践\\output"};
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);
		job.setJarByClass(MovieMain.class);
		
		//k2,v2
		job.setMapperClass(MovieMapper.class);
		job.setMapOutputKeyClass(MovieInfo.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		//k4,v4
		job.setReducerClass(MovieReducer.class);
		job.setOutputKeyClass(MovieInfo.class);
		job.setOutputValueClass(NullWritable.class);
		
//		job.addFileToClassPath(new Path("/lib/fastjson-1.2.62.jar"));
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
		
	}

}
