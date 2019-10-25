package hadoop.ch03.v17034460114;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.net.*;

public class ReadHDFSFileAttr {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		URI uri = new URI("hdfs://192.168.30.128:9000");
		FileSystem fs = FileSystem.get(uri, conf, "root");
		Path dfs = new Path("/17034460114/test5.txt");
		
		String name = "user.id";
		byte[] value = new byte[2];
		value [0] = 114;
		try {
			fs.setXAttr(dfs, name, value);
		} catch (IOException e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		byte[] result = null;
		try {
			result = fs.getXAttr(dfs, name);
		} catch (IOException e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		for (int i=0;i<1;i++) {
			System.out.println("my user.id ="+result[i]);
		}
		
		fs.close();
		
		
	}

}
