package flowcount;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlowCountRunner {
	
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		Job job = null;
		try {
//			conf.set("mapreduce.job.jar", "flow");
			job = Job.getInstance(conf ,"flowcountjob");
			job.setJarByClass(FlowCountRunner.class);
			job.setMapperClass(FlowCountMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(FlowBean.class);
			
			job.setReducerClass(FlowCountReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(FlowBean.class);
			String inputPath = null;
			if(args[0]==null){
				System.out.println("请输入被处理文件的路径arg[0]");
			}else{
				inputPath = String.valueOf(args[0]);
			}
			
			Path path = null;
			if(args[1]==null){
				System.out.println("请输入处理结束后文件的保存路径arg[1]");
			}else{
				path = new Path(String.valueOf(args[1]));
			}
			FileInputFormat.setInputPaths(job, inputPath);
			FileOutputFormat.setOutputPath(job,path);
			job.waitForCompletion(true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean>{
		
		FlowBean flowBean = new FlowBean();
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context)
				throws IOException, InterruptedException {
			//首先获取一行数据
			String line = value.toString();
			
			//然后进行切割
			String[] fields = StringUtils.split(line, "\t");
			
			String mobile = fields[1];
			long up_flow = Long.parseLong(fields[fields.length-3]);
			long down_flow =  Long.parseLong(fields[fields.length-2]);
			flowBean.set(mobile,up_flow, down_flow);
			
			context.write(new Text(mobile), flowBean);
		}
	}
	public static class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean>{
		FlowBean flowBean = new FlowBean();
		@Override
		protected void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context)
				throws IOException, InterruptedException {
			long up_flow_sum =  0;
			long down_flow_sum = 0;
			
			for (FlowBean flowBean : values) {
				up_flow_sum  += flowBean.getUp_flow();
				down_flow_sum += flowBean.getDown_flow();
			}
			flowBean.set(key.toString(), up_flow_sum, down_flow_sum);
			context.write(key, flowBean);
		}
	}
}
