package hadoop.Exp5_3;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SaleReducer extends Reducer<Text, Sales, Text, Text>{
	@Override
	protected void reduce(Text k3, Iterable<Sales> v3, Context context) throws IOException, InterruptedException {
		int total1 = 0;
		float total2 = 0;
		
		for(Sales s:v3) {
			total1 = total1 + s.getQuantity_sold();
			total2 = total2 + s.getAmount_sold();
		}
		String total = "销售笔数：" + Integer.toString(total1) + "," + "销售总额：" + Float.toString(total2);
		context.write(k3, new Text(total));
	}

}
