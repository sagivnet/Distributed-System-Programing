import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class FifthComputation {
	public static class RecordReader {
		private Text[] wordGram;
		private DoubleWritable probability;

		public RecordReader(Text value) {
			String[] afterValues = value.toString().split("\\t");
			if (afterValues.length != 2) {
				wordGram = null;
				probability = null;
				System.out.println("5th Error(?) at: " + afterValues);
			} else {
				String[] words = afterValues[0].split(" ");
				if (words.length >= 3) {
					wordGram = new Text[] { new Text(words[0]), new Text(words[1]), new Text(words[2]) };
					probability = new DoubleWritable(Double.parseDouble(afterValues[1]));
				} else {
					wordGram = null;
					probability = null;
				}
			}
		}

		public Text[] getWordGram() {
			return wordGram;
		}

		public DoubleWritable getProbability() {
			return probability;
		}
	}

	public static class Map extends Mapper<Object, Text, SortPair, SortPair> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			RecordReader line = new RecordReader(value);
			if (line.getWordGram() == null)
				return;
			context.write(new SortPair(line.getWordGram()[0], line.getWordGram()[1]),
					new SortPair(new Text(line.getWordGram()[2]), new Text(line.getProbability().toString())));
		}
	}

	public static class Reduce extends Reducer<SortPair, SortPair, Text, DoubleWritable> {
		public void reduce(SortPair key, Iterable<SortPair> values, Context context)
				throws IOException, InterruptedException {
			HashMap<Double, Text> pairMap = new HashMap<Double, Text>();
			for (SortPair pair : values) {
				Text reduceKey = new Text(key.getFirst() + " " + key.getSecond() + " " + pair.getFirst());
				pairMap.put(Double.parseDouble(pair.getSecond().toString()), reduceKey);
			}
			TreeMap<Double, Text> newMap = new TreeMap<Double, Text>(Collections.reverseOrder());
			newMap.putAll(pairMap);
			for (Double myKey : newMap.keySet()) {
				Text value = newMap.get(myKey);
				context.write(value, new DoubleWritable(myKey));
			}

		}
	}
}