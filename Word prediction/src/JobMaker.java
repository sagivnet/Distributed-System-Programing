import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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
		Job job = Job.getInstance(conf, "First Computation");
		job.setJarByClass(FirstComputation.class);
		job.setMapperClass(FirstComputation.Map.class);
		job.setCombinerClass(FirstComputation.Reduce.class);
		job.setReducerClass(FirstComputation.Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(OccurencesPair.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(OccurencesPair.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0])); // the input (corpus) which we (the mapper) go through
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "1")); // outputPath will be some directory in s3 bucket
		if (!job.waitForCompletion(true))
			System.exit(1);

		Counter N = job.getCounters().findCounter(FirstComputation.NCounter.N);

		/******************************************************************************************/
		conf = new Configuration();
		job = Job.getInstance(conf, "Second Computation");
		job.setJarByClass(SecondComputation.class);
		job.setMapperClass(SecondComputation.Map.class);
		job.setCombinerClass(SecondComputation.Reduce.class);
		job.setReducerClass(SecondComputation.Reduce.class);
		job.setOutputKeyClass(EquationComponentPair.class);
		job.setOutputValueClass(LongWritable.class);
		job.setMapOutputKeyClass(EquationComponentPair.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[1] + "1"));// <w,<occA,occB>>
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "2")); // <<"Nr",r>,Nr> | <<"Tr",r>,Tr>
		if (!job.waitForCompletion(true))
			System.exit(1);
		/******************************************************************************************/
		conf = new Configuration();
		job = Job.getInstance(conf, "make <r,Pr>");
		job.getConfiguration().setLong(FirstComputation.NCounter.N.name(), N.getValue());
		job.setJarByClass(ThirdComputation.class);
		job.setMapperClass(ThirdComputation.Map.class);
		job.setReducerClass(ThirdComputation.Reduce.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(EquationComponentPair.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[1] + "2")); // <<"Nr",r>,Nr> | <<"Tr",r>,Tr>
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "3")); // <r,Pr>
		if (!job.waitForCompletion(true))
			System.exit(1);
		/******************************************************************************************/
		conf = new Configuration();
		job = Job.getInstance(conf, "Make <w,Pr>");
		job.setJarByClass(FourthComputation.class);
		job.setMapperClass(FourthComputation.Map.class);
		job.setReducerClass(FourthComputation.Reduce.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[1] + "1")); // <w,<occA,occB>>
		FileInputFormat.addInputPath(job, new Path(args[1] + "3")); // <r,Pr>

		FileOutputFormat.setOutputPath(job, new Path(args[1] + "4")); // <w,Pr>
		if (!job.waitForCompletion(true))
			System.exit(1);
		/******************************************************************************************/

		conf = new Configuration();
		job = Job.getInstance(conf, "make sorted");
		job.setJarByClass(FifthComputation.class);
		job.setMapperClass(FifthComputation.Map.class);
		job.setReducerClass(FifthComputation.Reduce.class);
		job.setMapOutputKeyClass(SortPair.class);
		job.setMapOutputValueClass(SortPair.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[1] + "4"));
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "5"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		/******************************************************************************************/
	}
}
