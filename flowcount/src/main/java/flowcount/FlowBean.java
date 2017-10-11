package flowcount;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * 网络传输需要进行序列化，在hadoop里面进行序列化需要实现Writable接口
 * @author ming
 *
 */
public class FlowBean implements WritableComparable<FlowBean>{
	private String mobile;
	private long up_flow;
	private long down_flow;
	private long sum_flow;
	
	public void set(String mobile ,long up_flow,long down_flow){
		this.mobile = mobile;
		this.up_flow = up_flow;
		this.down_flow = down_flow;
		this.sum_flow = up_flow + down_flow;
	}
	
	
	public String getMobile() {
		return mobile;
	}
	public void setMobile(String mobile) {
		this.mobile = mobile;
	}
	public long getUp_flow() {
		return up_flow;
	}
	public void setUp_flow(long up_flow) {
		this.up_flow = up_flow;
	}
	public long getDown_flow() {
		return down_flow;
	}
	public void setDown_flow(long down_flow) {
		this.down_flow = down_flow;
	}
	public long getSum_flow() {
		return sum_flow;
	}
	public void setSum_flow(long sum_flow) {
		this.sum_flow = sum_flow;
	}
	@Override
	public String toString() {
		return up_flow + "\t" + down_flow + "\t"
				+ sum_flow ;
	}


	/**
	 * 序列化
	 * @param out
	 * @throws IOException
	 */
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(mobile);
		out.writeLong(up_flow);
		out.writeLong(down_flow);
		out.writeLong(sum_flow);
	}

/**
 * 反序列化
 * @param in
 * @throws IOException
 */
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		mobile = in.readUTF();
		up_flow = in.readLong();
		down_flow = in.readLong();
		sum_flow = in.readLong();
	}


public int compareTo(FlowBean o) {
	// TODO Auto-generated method stub
	return sum_flow>o.sum_flow?-1:1;
}
	
}
