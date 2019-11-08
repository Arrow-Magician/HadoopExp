package hadoop.Exp5_2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

//建立分区规则
public class MyEmployeeParitioner extends Partitioner<IntWritable, Employee>{

	@Override
	public int getPartition(IntWritable k2, Employee v2, int numPartition) {
		// TODO Auto-generated method stub\
		//如何建立分区
		if(v2.getSal() < 1500) {
			return 1%numPartition;
		}else if (v2.getSal() >=1500 && v2.getSal() < 3000) {
			return 2%numPartition;
		}else {
			return 3%numPartition;
		}
	}
	

}
