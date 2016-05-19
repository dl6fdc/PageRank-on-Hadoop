import java.io.IOException;
import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Top10Nodes {
	public static void main(String[] args) throws Exception {
	
		if (args.length != 2)
			System.err.println("Usage: Top10Nodes [inputFile: PageRank output] [outputFile]");
	
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "top10nodes");
		job.setJarByClass(Top10Nodes.class);
		job.setMapperClass(Top10NodesMapper.class);
		job.setReducerClass(Top10NodesReducer.class);
		job.setOutputKeyClass(DoubleWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
		
		Path resultInputPath = new Path(args[1]);
		Path resultInputFile = new Path(resultInputPath, "part-r-00000");
		Path resultOutputFile = new Path(resultInputPath, "top10Nodes.txt");
		
		try (	FileSystem fs = resultInputPath.getFileSystem(conf);
			BufferedReader buffReader = new BufferedReader(new InputStreamReader(fs.open(resultInputFile)));
			BufferedWriter buffWriter = new BufferedWriter(new OutputStreamWriter(fs.create(resultOutputFile,true)));) {
			for (int i = 0; i < 10; ++i) 
				buffWriter.write(buffReader.readLine() + "\n");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public static class Top10NodesMapper extends Mapper<Object, Text, DoubleWritable, Text> {
	
		private DoubleWritable keyOut = new DoubleWritable();
		private Text valueOut = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] inputRecord = value.toString().trim().split("\\s+");
			String nodeid = inputRecord[0];
			double pageRank = Double.parseDouble(inputRecord[1]);
			keyOut.set(1.0 / pageRank);
			valueOut.set(nodeid);
			context.write(keyOut, valueOut);
		}
	}	
	

	public static class Top10NodesReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {
		private DoubleWritable valueOut = new DoubleWritable();

		public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			double pagerank = key.get();
			valueOut.set(1.0 / pagerank);
			for (Text val: values) {
				context.write(val, valueOut);
			}
		}
	}	
	
}
