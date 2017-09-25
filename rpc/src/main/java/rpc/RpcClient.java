package rpc;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

public class RpcClient {
	
	
	
	public static void main(String args[]){
		Class protocol=ICalBirthYear.class;
		long clientVersion=1L;
		String hostname = "10.192.14.211";
		int port=12345;
		InetSocketAddress addr = new InetSocketAddress(hostname, port);
		Configuration conf=new Configuration();
		try {
			ICalBirthYear proxy = RPC.getProxy(protocol, clientVersion, addr, conf);
			int age=27;
			String name="刘铭";
			String res = proxy.calBirthYear(name, age);
			System.out.println(res);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
