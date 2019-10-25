package hadoop.ch03.v17034460114;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import java.net.*;

public class DeleteHDFSFile {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		URI uri = new URI("hdfs://192.168.30.128:9000");
		FileSystem fs = FileSystem.get(uri, conf, "root");
		
		Path path = new Path("/17034460114/test2.txt");
		fs.delete(path);
		
		fs.close();
		
		System.out.println("删除成功");

	}

}
