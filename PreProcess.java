import java.io.IOException;
import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PreProcess {
	public static void main(String[] args) throws Exception {
	
		if (args.length != 1)
			System.err.println("Usage: PreProcess inputFile");
	
		// pre-process graph
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "graph pre-process");
		job.setJarByClass(PreProcess.class);
		job.setMapperClass(PreProcessMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path("/results/iteration_0"));
		job.waitForCompletion(true);
		
		
		Path resultInputPath = new Path("/results/iteration_0");
		Path resultInputFile = new Path(resultInputPath, "part-r-00000");
		Path resultOutputFile = new Path("/results/graphInfo.txt");
		String line = "", info = "";
		int numNodes = 0, numEdge = 0, sumEdge = 0;
		int maxDegree = 0, minDegree = Integer.MAX_VALUE;
		double averageDegree = 0.0;
		try (	FileSystem fs = resultInputPath.getFileSystem(conf);
			BufferedReader buffReader = new BufferedReader(new InputStreamReader(fs.open(resultInputFile)));
			BufferedWriter buffWriter = new BufferedWriter(new OutputStreamWriter(fs.create(resultOutputFile,true)));) {
			while ((line = buffReader.readLine()) != null) {
				++numNodes;
				String[] ids = line.trim().split("\\s+");
				numEdge = ids.length - 2;
				if (maxDegree < numEdge)
					maxDegree = numEdge;
				if (minDegree > numEdge)
					minDegree = numEdge;
				sumEdge += numEdge;
				info = info + "node " + ids[0] + " has " + numEdge + " outlinks.\n";
			}
			averageDegree = (double) sumEdge / numNodes;
			buffWriter.write("\ntotal number of nodes is: " + numNodes + "\n");
			buffWriter.write("total number of edges is: " + sumEdge + "\n");
			buffWriter.write("minimum out-degree is: " + minDegree + "\n");
			buffWriter.write("maximum out-degree is: " + maxDegree + "\n");
			buffWriter.write("average out-degree is: " + averageDegree + "\n\n");
			buffWriter.write(info);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	
	// class PreProcessMapper used to pre-process graph
	// output is: nodeid, pagerank, adjacency list
	// also remove dangling nodes	
	public static class PreProcessMapper extends Mapper<Object, Text, Text, Text> {
	
		private Text keyOut = new Text();
		private Text valueOut = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] inputRecord = value.toString().trim().split("\\s+");
			String nodeid = inputRecord[0];
			StringBuilder adjacencyList = new StringBuilder();
			adjacencyList.append("1.0");
			for (int i = 1; i < inputRecord.length; ++i)
				adjacencyList.append(" " + inputRecord[i]);
			
			keyOut.set(nodeid);
			valueOut.set(adjacencyList.toString());
			context.write(keyOut, valueOut);
		}
	}	
	
}
