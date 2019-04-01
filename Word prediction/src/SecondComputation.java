import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class SecondComputation {

	public static class RecordReader {
		private Text wordGram;
		private LongWritable occA, occB;

		public RecordReader(Text value) {
			String[] afterValues = value.toString().split("\\t");
			if (afterValues.length != 3) {
				wordGram = null;
				occA = new LongWritable(0);
				occB = new LongWritable(0);
				System.out.println("2nd Error(?) at: " + afterValues);
			}
			wordGram = new Text(afterValues[0]);
			occA = new LongWritable(Long.parseLong(afterValues[1]));
			occB = new LongWritable(Long.parseLong(afterValues[2]));
		}

		public Text getWordGram() {
			return wordGram;
		}

		public LongWritable getOccA() {
			return occA;
		}

		public LongWritable getOccB() {
			return occB;
		}
	}

	public static class Map extends Mapper<LongWritable, Text, EquationComponentPair, LongWritable> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			RecordReader line = new RecordReader(value);
			if (line.wordGram == null)
				return;
			context.write(new EquationComponentPair(new Text("Nr"), line.getOccA()), new LongWritable(1));
			context.write(new EquationComponentPair(new Text("Tr"), line.getOccA()), line.getOccB());
			context.write(new EquationComponentPair(new Text("Nr"), line.getOccB()), new LongWritable(1));
			context.write(new EquationComponentPair(new Text("Tr"), line.getOccB()), line.getOccA());

		}
	}

	public static class Reduce
			extends Reducer<EquationComponentPair, LongWritable, EquationComponentPair, LongWritable> {

		public void reduce(EquationComponentPair key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			long sum = 0;
			for (LongWritable val : values) {
				sum += val.get();
			}
			context.write(key, new LongWritable(sum));
		}
	}
}