package hadoop.Exp5_2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PartEmployeeMain {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		//1.创建一个job和任务入口（指定主类）
		Job job = Job.getInstance(new Configuration());
		job.setJarByClass(PartEmployeeMain.class);
		
		//2.指定Job的mapper和输出的类型
		job.setMapperClass(PartEmployeeMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);//部门号
		job.setMapOutputValueClass(Employee.class);//员工
		
		//指定任务的分区规则的类
		job.setPartitionerClass(MyEmployeeParitioner.class);
		//指定建立几个分区
		job.setNumReduceTasks(3);
		
		//指定job的reducer和输出类型
		job.setReducerClass(PartEmployeeReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Employee.class);
		
		//4.指定Job的输入和输出路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		
		job.waitForCompletion(true);
		
	}

}
