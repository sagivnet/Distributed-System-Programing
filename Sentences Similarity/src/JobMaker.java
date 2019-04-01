import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class JobMaker {

	// args will be the n-gram that its probability needs to be calculated
	// args[0] = corpus
	// args[1] = output path
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "First");
		job.setJarByClass(First.class);
		job.setMapperClass(First.Map.class);
		job.setReducerClass(First.Reduce.class);
		job.setOutputKeyClass(Triple.class);
		job.setOutputValueClass(Pair.class);
		job.setMapOutputKeyClass(Triple.class);
		job.setMapOutputValueClass(Pair.class);
		//job.setInputFormatClass(TextInputFormat.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		for (int i = 0; i < args.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(args[i]));// the input (corpus) which we (the mapper) go through
		}
		FileOutputFormat.setOutputPath(job, new Path(args[args.length - 1] + "1")); // outputPath will be some directory in s3 bucket
		if (!job.waitForCompletion(true))
			System.exit(1);

		Counter totalSlot = job.getCounters().findCounter(First.totalSlotCounter.totalSlot);
		

		/******************************************************************************************/
		conf = new Configuration();
		job = Job.getInstance(conf, "Second");
		job.setJarByClass(Second.class);
		job.setMapperClass(Second.Map.class);
		job.getConfiguration().setLong(First.totalSlotCounter.totalSlot.name(), totalSlot.getValue());
		job.setReducerClass(Second.Reduce.class);
		job.setOutputKeyClass(Triple.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setMapOutputKeyClass(Triple.class);
		job.setMapOutputValueClass(Pair.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		//job.setNumReduceTasks(1);
		FileInputFormat.addInputPath(job, new Path(args[args.length - 1] + "1"));
		FileOutputFormat.setOutputPath(job, new Path(args[args.length - 1] + "2"));
		if (!job.waitForCompletion(true))
			System.exit(1);
		/******************************************************************************************/
		conf = new Configuration();
		job = Job.getInstance(conf, "Third");
		job.setJarByClass(Third.class);
		job.setMapperClass(Third.Map.class);
		job.setReducerClass(Third.Reduce.class);
		job.setMapOutputKeyClass(Triple.class);
		job.setMapOutputValueClass(PairDouble.class);
		job.setOutputKeyClass(Triple.class);
		job.setOutputValueClass(PairDouble.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[args.length - 1] + "2"));
		FileOutputFormat.setOutputPath(job, new Path(args[args.length - 1] + "3"));
		if (!job.waitForCompletion(true))
			System.exit(1);
		/******************************************************************************************/
		conf = new Configuration();
		job = Job.getInstance(conf, "Fourth");
		job.setJarByClass(Fourth.class);
		job.setMapperClass(Fourth.Map.class);
		job.setReducerClass(Fourth.Reduce.class);
		job.setMapOutputKeyClass(Triple.class);
		job.setMapOutputValueClass(PairDouble.class);
		job.setOutputKeyClass(PairTextText.class);
		job.setOutputValueClass(Triple.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[args.length - 1] + "3"));
		FileOutputFormat.setOutputPath(job, new Path(args[args.length - 1] + "4"));
		if (!job.waitForCompletion(true))
			System.exit(1);
		/******************************************************************************************/
		conf = new Configuration();
		job = Job.getInstance(conf, "Fifth");
		job.setJarByClass(Fifth.class);
		job.setMapperClass(Fifth.Map.class);
		job.setReducerClass(Fifth.Reduce.class);
		job.setMapOutputKeyClass(PairTextText.class);
		job.setMapOutputValueClass(Triple.class);
		job.setOutputKeyClass(PairTextText.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[args.length - 1] + "4"));
		FileOutputFormat.setOutputPath(job, new Path(args[args.length - 1] + "5"));
		if (!job.waitForCompletion(true))
			System.exit(1);
		/******************************************************************************************/
	}
}
