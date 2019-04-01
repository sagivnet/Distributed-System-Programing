import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.tartarus.snowball2.runStemmer;

public class Second {

	public static class RecordReader {
		private Triple key;
		private Pair val;

		public RecordReader(Text value) {
			String[] afterValues = value.toString().split("\\t");
			if (afterValues.length != 2) {
				key = null;
				val = null;
				return;
			}
			key = new Triple();
			Text valFirst = null;
			LongWritable valSecond = null;
			try {
				key.fromString(afterValues[0]);
				valFirst = new Text(afterValues[1].substring(1, afterValues[1].indexOf(">") + 1));
				valSecond = new LongWritable(Long.parseLong(afterValues[1]
						.substring(afterValues[1].lastIndexOf(",") + 1, afterValues[1].lastIndexOf(">"))));
			} catch (Exception e) {
				System.out.println("Error with(mapred 2): " + value.toString());
				key = null;
				val = null;
			}
			val = new Pair(valFirst, valSecond);
		}

		public Triple getKey() {
			return key;
		}

		public Pair getValue() {
			return val;
		}
	}

	public static class Map extends Mapper<Object, Text, Triple, Pair> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			RecordReader line = new RecordReader(value);
			if (line.getKey() == null)
				return;
			context.write(new Triple(new Text(runStemmer.englishStem(line.getKey().getFirst().toString())),
					line.getKey().getSecond(), line.getKey().getThird()), line.getValue());
//			context.write(line.getKey(), line.getValue());
			
		} 
	}

	public static class Reduce extends Reducer<Triple, Pair, Triple, DoubleWritable> {

		private long totalSlot;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			this.totalSlot = context.getConfiguration().getLong(First.totalSlotCounter.totalSlot.name(), 0);
		}

		public void reduce(Triple key, Iterable<Pair> values, Context context)
				throws IOException, InterruptedException {
			Long pSlotW = new Long(0), pSlotStar = new Long(0), starSlotW = new Long(0);
			for (Pair val : values) {
				if (val.getFirst().toString().equals("<p,s,w>"))
					pSlotW += Math.abs(val.getSecond().get());
				else if (val.getFirst().toString().equals("<p,s,*>"))
					pSlotStar += Math.abs(val.getSecond().get());
				else if (val.getFirst().toString().equals("<*,s,w>"))
					starSlotW += Math.abs(val.getSecond().get());
			}
			Double partMI = new Double((double) (pSlotW * totalSlot) / (double) (pSlotStar * starSlotW));
			Double MI = new Double(Math.log1p(partMI) / Math.log1p(2));
			context.write(key, new DoubleWritable(MI));
		}
	}
}