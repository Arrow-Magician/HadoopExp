package hadoop.Exp5_4;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LogMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String data = new String(value.getBytes(), 0, value.getLength(), "GBK");
		String word[] = data.split("\\s+");
		if (word.length != 6) {
			return;// return语句后不带返回值，作用是退出该程序的运行
		}
		String newData = data.replaceAll("\\s+", ",");
		String words[] = newData.split(",");
		if (words[3].equals("2") && words[4].equals("1")) {
			context.write(new Text(newData), NullWritable.get());
		} else {
			return;
		}
	}
}
