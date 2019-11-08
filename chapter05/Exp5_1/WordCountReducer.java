package hadoop.Exp5_1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	@Override
	protected void reduce(Text k3, Iterable<IntWritable> v3, Context context) throws IOException, InterruptedException{
		
		int total = 0;
		for(IntWritable v:v3) {
			total += v.get();//计算总和
		}
		
		context.write(k3, new IntWritable(total));
	}
	

}
