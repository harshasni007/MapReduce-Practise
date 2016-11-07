package MapReduce.mapr;


import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

	public static class TokenizerMapper
	extends Mapper<Object, Text, Text, IntWritable>{

		Map<String, Integer> wordMap;
		
		@Override
	    protected void setup(Context context) throws IOException, InterruptedException {
			wordMap = new HashMap<String, Integer>();
	    }
		
		
		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				String localWord = itr.nextToken();
				int count;
				//if(localWord.toLowerCase().matches("^[mnopq].*")){
					//minVal = (a < b) ? a : b;
					count = (wordMap.get(localWord)==null) ? 1: (wordMap.get(localWord)+1);
					
					wordMap.put(localWord, count);
					
					
				//}
			}
		}
		
		@Override
	    protected void cleanup(Context context)
	    		  throws IOException, InterruptedException {
	    	for (Entry<String, Integer> entry : wordMap.entrySet()) {
	    		Text word = new Text(entry.getKey());
	    		IntWritable counter = new IntWritable(entry.getValue());
				context.write(word, counter);
				}
	    	} 
		}
		
	

	public static class IntSumReducer
	extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context
				) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
	
	//Custom Partitioner class
	
	   public static class wordPartitioner extends Partitioner < Text, IntWritable >
	   {
	      @Override
	      public int getPartition(Text key, IntWritable value, int numReduceTasks)
	      {
	         String wordKey = key.toString();
	         
	         if(numReduceTasks == 0)  return 0;
	         
	         if(wordKey.toLowerCase().matches("^[m].*") )
	         {
	            return 0;
	         }
	         
	         else if(wordKey.toLowerCase().matches("^[n].*"))
	         {
	            return 1% numReduceTasks;
	         }
	         else if(wordKey.toLowerCase().matches("^[o].*"))
	         {
	            return 2 % numReduceTasks;
	         }
	         else if(wordKey.toLowerCase().matches("^[p].*"))
	         {
	            return 3 % numReduceTasks;
	         }
	         else
	         {
	            return 4 % numReduceTasks;
	         }
	      }
	   }
	   

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(WordCount.class);
		
		job.setMapperClass(TokenizerMapper.class);
		//job.setCombinerClass(IntSumReducer.class);
		job.setPartitionerClass(wordPartitioner.class);
		job.setReducerClass(IntSumReducer.class);
		
		job.setNumReduceTasks(5);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
