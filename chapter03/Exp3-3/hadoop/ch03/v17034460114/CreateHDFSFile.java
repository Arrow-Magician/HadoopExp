package hadoop.ch03.v17034460114;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import java.net.*;

public class CreateHDFSFile {

	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		//配置NameNode地址，具体根据NameNode IP
		URI uri = new URI("hdfs://192.168.30.128:9000");
		//指定用户名，获取FileSystem对象
		FileSystem fs = FileSystem.get(uri, conf, "root");
		//定义文件路径
		Path dfs = new Path("/17034460114/test2.txt");
		FSDataOutputStream os = fs.create(dfs, true);
		//往文件写入信息
		os.writeBytes("hello,hdfs!");
		//关闭流
		os.close();
		//不需要再操作FileSystem，关闭
		fs.close();
		
		System.out.println("创建成功");
	}

}
