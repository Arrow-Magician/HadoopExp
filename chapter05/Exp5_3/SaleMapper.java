package hadoop.Exp5_3;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SaleMapper extends Mapper<LongWritable, Text, Text, Sales>{
	@Override
	protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
		//数据：13,987,1998-01-10,3,999,1,1232.16
		String data = v1.toString();
		String [] words = data.split(",");
		//数据：t1=987,1998-01-10,3,999,1,1232.16
		String t1 = StringUtils.substringAfter(data, ",");
		//数据：t2=1998-01-10,3,999,1,1232.16
		String t2 = StringUtils.substringAfter(t1, ",");
		//年份为偏移量，数据words2[0]=1998,words2[1]=01,words2[2]=10
		String[] words2 = t2.split("-");
		
		//设置属性
		Sales sales = new Sales();
		sales.setTime(words[2]);
		sales.setQuantity_sold(Integer.parseInt(words[5]));
		sales.setAmount_sold(Float.valueOf(words[6]));
		
		context.write(new Text(words2[0]) , sales);
		
	}

}
