package mapreduce;

import java.io.IOException;
import java.util.Calendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountRunner {
	public static void main1(String[] args) {
		System.out.println(System.currentTimeMillis());
		Calendar instance = Calendar.getInstance();
		System.out.println(instance.getTimeZone());
	}
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		Job job = null;
		try {
			
		   conf.set("mapreduce.job.jar", "wc.jar");
		   job	 = Job.getInstance(conf );

		  
		   
		   job.setJarByClass(WordCountRunner.class);
		   job.setMapperClass(WordCountMapper.class);
		   job.setMapOutputKeyClass(Text.class);
		   job.setMapOutputValueClass(LongWritable.class);
		   
		   job.setReducerClass(WordCountReducer.class);
		   job.setOutputKeyClass(Text.class);
		   job.setOutputValueClass(LongWritable.class);
		   
		   FileInputFormat.setInputPaths(job, "/wc/in");
		   
		   FileSystem fs = FileSystem.get(conf);
		   if(  fs.exists(new Path("/wc/out"))){
			   fs.delete(new Path("/wc/out"), true);
		   }
		   
		   FileOutputFormat.setOutputPath(job, new Path("/wc/out"));
		   
		   try {
			job.waitForCompletion(true);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		   
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
