package hadoop.ch03.v17034460114;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import java.net.*;

public class DownloadHDFSFile {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		//新建文件并写入内容
		Configuration conf = new Configuration();
		URI uri = new URI("hdfs://192.168.30.128:9000");
		FileSystem fs = FileSystem.get(uri, conf, "root");
		Path dfs = new Path("/17034460114/test5.txt");
		FSDataOutputStream os = fs.create(dfs, true);
		os.writeBytes("Hello,World!");
		System.out.println("创建成功");
		
		System.out.println("------------------");
		
		Path dst = new Path("E:\\金峰\\学习\\大三\\Hadoop");
		fs.copyToLocalFile(false, dfs, dst, true);
		System.out.println("下载成功");
		
		fs.close();

	}

}
