package other;

import java.io.IOException;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

 
/**
 * The main driver program for the sorting of the dictionary. This sorts the
 * dictionary by the document frequency of the words.
 * 
 * @author UP
 * 
 */
public class SortDriver {
 
    private static final Log logger = LogFactory.getLog(SortDriver.class);
 
    /**
     * This is the main method that drives the creation of the inverted index.
     * It expects the following input arguments - the location of the input
     * files the location of the partition files
     * 
     * @param args
     *            - the command line arguments
     */
    public static void main(String[] args) {
        try {
            runJob(args[0], args[1], args[2], Integer.parseInt(args[3]));
        } catch (Exception ex) {
            logger.error(null, ex);
        }
    }
 
    /**
     * This creates and runs the job for creating the inverted index
     * 
     * @param input
     *            - location of the input folder
     * @param output
     *            - location of the output folder
     * @param partitionLocation
     *            - location of the partition folder
     * @param numReduceTasks
     *            - number of reduce tasks
     * @throws IOException
     * @throws URISyntaxException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    public static void runJob(String input, String output,
            String partitionLocation, int numReduceTasks) throws IOException,
            URISyntaxException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "DictionarySorter");
        job.setJarByClass(SortDriver.class);
        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);
        job.setPartitionerClass(TotalOrderPartitioner.class);
        job.setNumReduceTasks(numReduceTasks);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
 
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        job.setSortComparatorClass(SortKeyComparator.class);
 
        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, new Path(output
                + ".dictionary.sorted." + getCurrentDateTime()));
        job.setPartitionerClass(TotalOrderPartitioner.class);
 
        Path inputDir = new Path(partitionLocation);
        Path partitionFile = new Path(inputDir, "partitioning");
        TotalOrderPartitioner.setPartitionFile(job.getConfiguration(),
                partitionFile);
 
        double pcnt = 10.0;
        int numSamples = numReduceTasks;
        int maxSplits = numReduceTasks - 1;
        if (0 >= maxSplits)
            maxSplits = Integer.MAX_VALUE;
 
        InputSampler.Sampler sampler = new InputSampler.RandomSampler(pcnt,
                numSamples, maxSplits);
        InputSampler.writePartitionFile(job, sampler);
 
        try {
            job.waitForCompletion(true);
        } catch (InterruptedException ex) {
            logger.error(ex);
        } catch (ClassNotFoundException ex) {
            logger.error(ex);
        }
    }
 
    /**
     * Returns todays date and time formatted as "yyyy.MM.dd.HH.mm.ss"
     * 
     * @return String - date formatted as yyyy.MM.dd.HH.mm.ss
     */
    private static String getCurrentDateTime() {
        Date d = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss");
        return sdf.format(d);
    }
 
}


/**
 * Mapper class for the dictionary sort task. This is an identity class which
 * simply outputs the key and the values that it gets, the intermediate key is
 * the document frequency of a word.
 * 
 * @author UP
 * 
 */
class SortMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
 
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String val = value.toString();
        if (val != null && !val.isEmpty() && val.length() >= 5) {
            String[] splits = val.split(",");
            context.write(new LongWritable(Long.parseLong(splits[1].trim())),
                    new Text(splits[0] + "," + splits[2]));
        }
    }
}
    
    /**
     * Reducer class for the dictionary sort class. This is an identity reducer 
     * which simply outputs the values received from the map output.
     * 
     * @author UP
     *
     */
    class SortReducer extends
            Reducer<LongWritable, Text, Text, LongWritable> {
     
        @Override
        protected void reduce(LongWritable key, Iterable<Text> value, Context context)
                throws IOException, InterruptedException {
            for(Text val : value) {
                context.write(new Text(val + "," + key), null);
            }
        }
     
    }




/**
 * This comparator is used to sort the output of the dictionary in the 
 * descending order of the counts. The descending order enables to pick a 
 * dictionary of the required size by any aplication using the dictionary.
 * 
 * @author UP
 *
 */
class SortKeyComparator extends WritableComparator {
     
    protected SortKeyComparator() {
        super(LongWritable.class, true);
    }
 
    /**
     * Compares in the descending order of the keys.
     */
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        LongWritable o1 = (LongWritable) a;
        LongWritable o2 = (LongWritable) b;
        if(o1.get() < o2.get()) {
            return 1;
        }else if(o1.get() > o2.get()) {
            return -1;
        }else {
            return 0;
        }
    }
     
}