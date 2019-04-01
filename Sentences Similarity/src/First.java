import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class First {

	private static String[] nouns = { "NN", "NNS", "NNP", "NNPS", "PRP", "PRP$", "WP", "WP$" };

	public static enum totalSlotCounter {
		totalSlot
	}
	
	public static class RecordReader {
		private Text root;
		private Text sentence;
		private LongWritable count;

		public RecordReader(Text value) {
			String[] afterValues = value.toString().split("\\t");
			root = new Text(afterValues[0]);
			sentence = new Text(afterValues[1]);
			count = new LongWritable(Long.parseLong(afterValues[2]));
		}

		public Text getRoot() {
			return root;
		}

		public Text getSentence() {
			return sentence;
		}

		public LongWritable getCount() {
			return count;
		}
	}

	public static class Map extends Mapper<Object, Text, Triple, Pair> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			RecordReader line = new RecordReader(value);

			String sentence = line.getSentence().toString();
			String[] sentenceParts = sentence.split(" ");
			for (int i = 0; i < sentenceParts.length; i++) {
				String[] sentenceWords = sentenceParts[i].split("/");
				if (sentenceWords[0].equals(line.getRoot().toString())) {
					if (!sentenceWords[1].equals("VB"))
						return;
					break;
				}
			}
			String[] slotXParts = sentenceParts[0].split("/");
			String[] slotYParts = sentenceParts[sentenceParts.length - 1].split("/");
			String slotX = null, slotY = null;
			boolean slotXbool = false, slotYbool = false;
			for (int i = 0; i < nouns.length; i++) {
				if (slotXParts[1].equals(nouns[i])) {
					slotXbool = true;
					slotX = slotXParts[0];
				}
				if (slotYParts[1].equals(nouns[i])) {
					slotYbool = true;
					slotY = slotYParts[0];
				}
				if (slotXbool && slotYbool) {
					break;
				}
			}
			if (!slotXbool || !slotYbool) {
				return;
			}

			String path = "";

			for (int i = 1; i < sentenceParts.length - 1; i++) {
				String[] pathParts = sentenceParts[i].split("/");
				if (i == sentenceParts.length - 2)
					path += pathParts[0];
				else
					path += pathParts[0] + " ";
			}
			
			if (path.equals("") || path.equals(null) || slotX.equals("") || slotX.equals(null) || slotY.equals("") || slotY.equals(null)) {
				return;
			}
			// <p,s,w>
			context.write(new Triple(new Text(path), new Text("x"), new Text(slotX)),
					new Pair(new Text(), line.getCount()));
			context.write(new Triple(new Text(path), new Text("y"), new Text(slotY)),
					new Pair(new Text(), line.getCount()));
//			context.write(new Triple(new Text(path + "*"), new Text("x"), new Text(slotY)),
//					new Pair(new Text(), line.getCount()));
//			context.write(new Triple(new Text(path + "*"), new Text("y"), new Text(slotX)),
//					new Pair(new Text(), line.getCount()));
			

			// <p,s,*>
			context.write(new Triple(new Text(path), new Text("x"), new Text("*")),
					new Pair(new Text(slotX), line.getCount()));
			context.write(new Triple(new Text(path), new Text("y"), new Text("*")),
					new Pair(new Text(slotY), line.getCount()));
//			context.write(new Triple(new Text(path + "*"), new Text("x"), new Text("*")),
//					new Pair(new Text(slotY), line.getCount()));
//			context.write(new Triple(new Text(path + "*"), new Text("y"), new Text("*")),
//					new Pair(new Text(slotX), line.getCount()));

			// <*,s,w>
			context.write(new Triple(new Text("*"), new Text("x"), new Text(slotX)),
					new Pair(new Text(path), line.getCount()));
			context.write(new Triple(new Text("*"), new Text("y"), new Text(slotY)),
					new Pair(new Text(path), line.getCount()));
//			context.write(new Triple(new Text("*"), new Text("x"), new Text(slotY)),
//					new Pair(new Text(path + "*"), line.getCount()));
//			context.write(new Triple(new Text("*"), new Text("y"), new Text(slotX)),
//					new Pair(new Text(path + "*"), line.getCount()));
			
			
			context.getCounter(totalSlotCounter.totalSlot).increment(line.getCount().get());
		}
	}

	public static class Reduce extends Reducer<Triple, Pair, Triple, Pair> {

		public void reduce(Triple key, Iterable<Pair> values, Context context)
				throws IOException, InterruptedException {

			List<Text> valuesInMemory = new ArrayList<Text>();
			Long sum = new Long(0);
			boolean found = false;
			for (Pair value : values) {
				found = false;
				sum += value.getSecond().get();
				for (Text val : valuesInMemory) {
					if (val.equals(value.getFirst())) {
						found = true;
						break;
					}
				}
				if (!found)
					valuesInMemory.add(new Text(value.getFirst()));
			}
			// <p,s,w>
			if (!key.getFirst().toString().equals("*") && !key.getThird().toString().equals("*"))
				context.write(key, new Pair(new Text("<p,s,w>"), new LongWritable(sum)));
			// <p,s,*>
			else if (!key.getFirst().toString().equals("*") && key.getThird().toString().equals("*")) {
				for (Text value : valuesInMemory) {
					context.write(new Triple(key.getFirst(), key.getSecond(), value),
							new Pair(new Text("<p,s,*>"), new LongWritable(sum)));
				}
			}
			// <*,s,w>
			else {
				for (Text value : valuesInMemory) {
					context.write(new Triple(value, key.getSecond(), key.getThird()),
							new Pair(new Text("<*,s,w>"), new LongWritable(sum)));
				}
			}
		}
	}
}