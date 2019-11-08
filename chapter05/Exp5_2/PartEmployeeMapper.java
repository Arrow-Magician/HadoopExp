package hadoop.Exp5_2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PartEmployeeMapper extends Mapper<LongWritable, Text, IntWritable, Employee>{
	@Override
	protected void map(LongWritable key1, Text value1, Context context) throws IOException, InterruptedException {
		//数据
		String data = value1.toString();
		//分词
		String[] words = data.split(",");
		//创建员工对象
		Employee e = new Employee();
		//设置员工属性
		
		e.setEmpno(Integer.parseInt(words[0]));//员工号
		e.setEname(words[1]);//姓名
		e.setJob(words[2]);//职位
		try {
			e.setMgr(Integer.parseInt(words[3]));//有老板号
		} catch (Exception ex) {
			// TODO: handle exception
			e.setMgr(-1);//没有老板号
		}
		e.setHiredate(words[4]);//入职日期
		e.setSal(Integer.parseInt(words[5]));//月薪
		try {
			e.setComm(Integer.parseInt(words[6]));//奖金
		} catch (Exception ex) {
			// TODO: handle exception
			e.setComm(0);//没有奖金
		}
		e.setDeptno(Integer.parseInt(words[7]));//部门号
		
		context.write(new IntWritable(e.getDeptno()), e);//员工部门号，员工对象
	}

}
