package hadoop.Exp5_2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class PartEmployeeReducer extends Reducer<IntWritable, Employee, IntWritable, Employee>{
	protected void reduce(IntWritable k3, Iterable<Employee> v3, Context context) throws IOException, InterruptedException {
		/*
		 * k3 部门号 
		 * v3 部门员工
		 */
		for(Employee e:v3) {
			context.write(k3, e);
		}
	}
}
