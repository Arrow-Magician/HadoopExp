package hadoop.ch03.v17034460114;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import java.net.*;

public class UploadHDFSFile {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		URI uri = new URI("hdfs://192.168.30.128:9000");
		FileSystem fs = FileSystem.get(uri, conf, "root");
		
		Path src = new Path("E:\\金峰\\学习\\大三\\Hadoop\\test4.txt");
		
		Path dst = new Path("/17034460114/test4.txt");
		fs.copyFromLocalFile(src, dst);
		
		fs.close();
		
		System.out.println("上传成功");
		

	}

}
