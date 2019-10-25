package hadoop.ch03.v17034460114;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import java.net.*;
import java.text.SimpleDateFormat;
import java.util.Date;

public class ReadHDFSFile {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		URI uri = new URI("hdfs://192.168.30.128:9000");
		FileSystem fs = FileSystem.get(uri, conf, "root");
		
		Path path = new Path("/17034460114/test5.txt");
		
		FileStatus fileStatus = fs.getFileLinkStatus(path);
		
		long blockSize = fileStatus.getBlockSize();
		System.out.println("blockSize:"+blockSize);
		
		long fileSize = fileStatus.getLen();
		System.out.println("fileSize:"+fileSize);
		
		String fileOwner = fileStatus.getOwner();
		System.out.println("fileOwner"+fileOwner);
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss");
		long accessTime = fileStatus.getAccessTime();
		System.out.println("accessTime:"+sdf.format(new Date(accessTime)));
		
		long modifyTime = fileStatus.getModificationTime();
		System.out.println("modifyTime:"+sdf.format(new Date(modifyTime)));
		
		fs.close();
		

	}

}
