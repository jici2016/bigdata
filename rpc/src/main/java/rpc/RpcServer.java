package rpc;

import java.io.IOException;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Builder;
import org.apache.hadoop.ipc.RPC.Server;

public class RpcServer {
	
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		RPC.Builder builder = new Builder(conf);
		CalBirthYearImpl instance = new CalBirthYearImpl();
		//设置监听的ip地址
		String bindAddress="127.0.0.1";
		//设置监听的端口
		int port=12345;
		//设置协议（要实现的接口类）设置一个具体的接口类实现对象，设置端口地址
		builder.setProtocol(ICalBirthYear.class).setInstance(instance).setBindAddress(bindAddress).setPort(port);
		try {
			Server server = builder.build();
			server.start();
		} catch (HadoopIllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
		}
	}
}
