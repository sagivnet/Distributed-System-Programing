import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class FourthComputation {
	private static final String probabilitySign = "%Pr% ";

	public static class RecordReader {
		private Text wordGram;
		private LongWritable r;
		private Text probability;
		private boolean isWord;

		public RecordReader(Text value) {
			String[] afterValues = value.toString().split("\\t");
			if (afterValues.length == 2) {
				isWord = false;
				r = new LongWritable(Long.parseLong(afterValues[0]));
				probability = new Text(probabilitySign + afterValues[1]);
			} else {
				isWord = true;
				wordGram = new Text(afterValues[0]);
				r = new LongWritable(Long.parseLong(afterValues[1]) + Long.parseLong(afterValues[2]));
			}
		}

		public LongWritable getR() {
			return r;
		}

		public Text getWordGram() {
			return wordGram;
		}

		public boolean isWord() {
			return isWord;
		}

		public Text getProbability() {
			return probability;
		}
	}

	public static class Map extends Mapper<Object, Text, LongWritable, Text> {

		Random rand = new Random();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			RecordReader line = new RecordReader(value);

			if (line.isWord())
				context.write(line.getR(), line.getWordGram());
			else
				context.write(line.getR(), line.getProbability());
		}
	}

	public static class Reduce extends Reducer<LongWritable, Text, Text, DoubleWritable> {
		public void reduce(LongWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			DoubleWritable probability = new DoubleWritable(0);
			List<Text> wordGrams = new ArrayList<>();
			for (Text element : values) {
				String s = element.toString();
				if (element.toString().contains(probabilitySign)) {
					probability = new DoubleWritable(Double.parseDouble(s.substring(probabilitySign.length())));

				} else
					wordGrams.add(new Text(s));
			}

			for (Text wordGram : wordGrams) {
				context.write(wordGram, probability);
			}
		}
	}
}