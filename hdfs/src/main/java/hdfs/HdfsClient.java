package hdfs;

import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.CharSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

public class HdfsClient {
	public static void main(String[] args) {
		
		String rootPath = "hdfs://10.192.14.13:9000/";
		rootPath = "hdfs://27.50.132.76:9076/";
		String infile = "F:\\trade.rar";
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", rootPath);
		try {
			FileSystem fs = FileSystem.get(new URI(rootPath),conf,"hadoop");
			
			Path path = new Path(rootPath +"trade.rar") ;
			
			FSDataOutputStream fsDataOutputStream = fs.create(path );
			FileInputStream is = new FileInputStream(infile);
			IOUtils.copy(is,fsDataOutputStream);
			fs.delete(new Path(rootPath +"jdk-8u144-linux-x64.tar.gz"), true);
			RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
			
			while(listFiles.hasNext()){
				System.out.println(listFiles.next().getPath());
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
