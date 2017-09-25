package hdfs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Before;
import org.junit.Test;

public class HdfsClient2 {
	
	FileSystem fs = null;
	
	//@Before 可以让该方法先执行
	@Before
	public void getFs(){
		Configuration conf = new Configuration();
		String bathPath="hdfs://10.192.14.87:9000";
		conf.set("fs.defaultFS", bathPath);
		try {
			 fs = FileSystem.get(new URI(bathPath),conf,"hadoop");
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
	
	/**
	 * 查询列表功能
	 */
	@Test
	public  void list() {
		
		try {
//			fs.create(f) 
			
			RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
			
			while(listFiles.hasNext()){
				LocatedFileStatus fileStatus = listFiles.next();
				System.out.println(fileStatus.getPath().getName()+"["+fileStatus.getPermission()+"]"+(fileStatus.isFile()?"f":"b")+"="+fileStatus.getPath());
			}
			
			System.out.println("--------------------------------------------");
			FileStatus[] listStatus = fs.listStatus(new Path("/"));
			for(int i=0;i<listStatus.length;i++){
				FileStatus fileStatus = listStatus[i];
				System.out.println(fileStatus.getPath().getName()+"["+fileStatus.getPermission()+"]"+(fileStatus.isFile()?"f":"b")+"="+fileStatus.getPath());
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * 上传文件
	 */
	@Test
	public void testUpload(){
		try {
			fs.copyFromLocalFile(new Path("F:\\trade.rar"), new Path("/liuming/workspaces/trade.upload"));
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * 下载文件
	 * 
	 */
	@Test
	public void testDownload(){
		try {
			fs.copyToLocalFile(new Path("/liuming/workspaces/trade.upload"), new Path("D:\\trade.download"));
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	/**
	 * 创建文件夹功能
	 */
	@Test
	public void testMkdir(){
		try {
			fs.mkdirs(new Path("/liuming/workspaces"));
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 *删除路径，可以连带子文件都删掉
	 */
	@Test
	public void testDelete(){
		//是否级联删除
		 boolean recursive = true;
		try {
			boolean ok = fs.delete(new Path("/liuming"), recursive );
			System.out.println("删除结果："+ok);
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * 重命名
	 */
	@Test
	public void testRename(){
		try {
			fs.rename(new Path("/liuming/workspaces/trade"), new Path("/liuming/workspaces/trade.upload"));
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
