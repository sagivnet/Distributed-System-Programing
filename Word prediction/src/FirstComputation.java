import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class FirstComputation {

	public static enum NCounter {
		N
	}

	public static class RecordReader {
		private Text wordGram;
		private LongWritable occY;

		public RecordReader(Text value) {
			String[] afterValues = value.toString().split("\\t");
			wordGram = new Text(afterValues[0]);
			occY = new LongWritable(Long.parseLong(afterValues[2]));
		}

		public Text getWordGram() {
			return wordGram;
		}

		public LongWritable getOccY() {
			return occY;
		}
	}

	public static class Map extends Mapper<Object, Text, Text, OccurencesPair> {

		Random rand = new Random();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			RecordReader line = new RecordReader(value);

			int group = rand.nextInt(2);
			switch (group) {
			case 0:
				context.write(line.getWordGram(), new OccurencesPair(line.getOccY(), new LongWritable(0)));
				break;
			case 1:
				context.write(line.getWordGram(), new OccurencesPair(new LongWritable(0), line.getOccY()));
				break;
			}
			context.getCounter(NCounter.N).increment(line.getOccY().get());

		}
	}

	public static class Reduce extends Reducer<Text, OccurencesPair, Text, OccurencesPair> {

		public void reduce(Text key, Iterable<OccurencesPair> values, Context context)
				throws IOException, InterruptedException {

			long occA = 0, occB = 0;

			for (OccurencesPair pair : values) {
				occA += pair.getFirst().get();
				occB += pair.getSecond().get();
			}

			context.write(key, new OccurencesPair(new LongWritable(occA), new LongWritable(occB)));
		}
	}
}