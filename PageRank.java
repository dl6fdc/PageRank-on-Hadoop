import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class PageRank {
	public static void main(String[] args) throws Exception {
		if (args.length != 1)
			System.err.println("Usage: PageRank [number of iteration]");
			
		long tenTime = 0;
		long startTime = System.currentTimeMillis();
		for (int i = 0; i < Integer.parseInt(args[0]); ++i) {
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf);
			job.setJobName("PageRank_iteration_" + (i+1));
			job.setJarByClass(PageRank.class);
			job.setMapperClass(PageRankMapper.class);
			job.setReducerClass(PageRankReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, new Path("/results/iteration_" + i));
			FileOutputFormat.setOutputPath(job, new Path("/results/iteration_" + (i+1)));
			job.waitForCompletion(true);
			if (i == 9)
				tenTime = System.currentTimeMillis() - startTime;
		}
		long endTime = System.currentTimeMillis();
		long totalTime = endTime - startTime;
		double averageTime = (double) totalTime / Integer.parseInt(args[0]);
		System.out.println("total time of " + args[0] + " iterations is: " + totalTime + " ms.");
		System.out.println("average time per iteration is: " + averageTime + " ms.");
		if (Integer.parseInt(args[0]) > 10)
			System.out.println("time for the first 10 iteration is: " + tenTime + " ms.");
	}
	
	public static class PageRankMapper extends Mapper<Object, Text, Text, Text> {
	
		private Text keyOut = new Text();
		private Text valueOut = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] inputValues = value.toString().trim().split("\\s+");
			String nodeid = inputValues[0];
			int outDegrees = inputValues.length - 2;
			if (outDegrees != 0) {
				Double rankContribution = Double.parseDouble(inputValues[1]) / outDegrees;
				valueOut.set(rankContribution.toString());
				StringBuilder sb = new StringBuilder();
				for (int i = 2; i < inputValues.length; ++i) {
					keyOut.set(inputValues[i]);
					context.write(keyOut, valueOut);
					sb.append(inputValues[i] + " ");
				}
				context.write(new Text(nodeid), new Text(":" + sb.toString()));
			}
		}
	}	
	
	public static class PageRankReducer extends Reducer<Text, Text, Text, Text> {
		
		private Text valueOut = new Text();
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String outLinks = "";
			double sum = 0.0;
			
			for (Text val: values) {
				String str = val.toString();
				if (str.indexOf(":") == 0) {
					outLinks = str.substring(str.indexOf(":") + 1, str.length());
				}
				else 
					sum += Double.parseDouble(str);
				
			}
			
			double pageRank = sum * 0.85 + 0.15;
			valueOut.set(String.valueOf(pageRank) + " " + outLinks);
			context.write(key, valueOut);
		}
	}
}
