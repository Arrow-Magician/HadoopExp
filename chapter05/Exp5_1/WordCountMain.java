package hadoop.Exp5_1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountMain {

	public static void main(String[] args) throws Exception {
		
//		args = new String[] {"E:\\金峰\\学习\\大三\\Hadoop\\ch05.txt","E:\\金峰\\学习\\大三\\Hadoop\\output"};
		// TODO Auto-generated method stub
		//创建一个job和任务入口
		Job job = Job.getInstance(new Configuration());
		job.setCombinerClass(WordCountCombiner.class);
		job.setJarByClass(WordCountMain.class);//main方法所在的class
		//指定job的mapper和输出的类型<k2 v2>
		job.setMapperClass(WordCountMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		//指定job的reducer和输出的类型<k4 v4>
		job.setReducerClass(WordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		//指定job的输入和输出
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//执行job
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0:1);
		
	}

}
