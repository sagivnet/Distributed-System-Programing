import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class ThirdComputation {

	public static class RecordReader {
		private Text component;
		private LongWritable firstArg, secondArg;

		public RecordReader(Text value) {
			String[] afterValues = value.toString().split("\\t");
			if (afterValues.length != 3) {
				component = null;
				firstArg = new LongWritable(0);
				secondArg = new LongWritable(0);
				System.out.println("3rd Error(?) at: " + afterValues);
			} else {
				component = new Text(afterValues[0]);
				firstArg = new LongWritable(Long.parseLong(afterValues[1]));
				secondArg = new LongWritable(Long.parseLong(afterValues[2]));
			}
		}

		public Text getComponent() {
			return component;
		}

		public LongWritable getFirstArg() {
			return firstArg;
		}

		public LongWritable getSecondArg() {
			return secondArg;
		}
	}

	public static class Map extends Mapper<LongWritable, Text, LongWritable, EquationComponentPair> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			RecordReader line = new RecordReader(value);
			context.write(line.getFirstArg(), new EquationComponentPair(line.getComponent(), line.getSecondArg()));
		}
	}

	public static class Reduce extends Reducer<LongWritable, EquationComponentPair, LongWritable, DoubleWritable> {

		private long N;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			this.N = context.getConfiguration().getLong(FirstComputation.NCounter.N.name(), 0);
		}

		public void reduce(LongWritable key, Iterable<EquationComponentPair> values, Context context)
				throws IOException, InterruptedException {
			long nr = 0, tr = 0;
			for (EquationComponentPair pair : values) {
				if (pair.getFirst().toString().equals("Nr"))
					nr += pair.getSecond().get();
				else if (pair.getFirst().toString().equals("Tr"))
					tr += pair.getSecond().get();
			}
			double probability = (double) tr / ((double) N * (double) nr);
			context.write(key, new DoubleWritable(probability));

		}
	}
}