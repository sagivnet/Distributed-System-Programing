import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Fifth {

	public static class RecordReader {
		private PairTextText key;
		private Triple val;

		public RecordReader(Text value) {
			String[] afterValues = value.toString().split("\\t");
			if (afterValues.length != 2) {
				key = null;
				val = null;
				return;
			}
			key = new PairTextText();
			val = new Triple();
			try {
				key.fromString(afterValues[0]);
				val.fromString(afterValues[1]);
			} catch (Exception e) {
				System.out.println("Error with(mapred 5): " + value.toString());
				key = null;
				val = null;
			}
			
		}

		public PairTextText getKey() {
			return key;
		}

		public Triple getValue() {
			return val;
		}
	}

	public static class Map extends Mapper<Object, Text, PairTextText, Triple> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			RecordReader line = new RecordReader(value);
			if (line.getKey() == null)
				return;
			context.write(line.getKey(), line.getValue());
		}
	}

	public static class Reduce extends Reducer<PairTextText, Triple, PairTextText, DoubleWritable> {

		public void reduce(PairTextText key, Iterable<Triple> values, Context context)
				throws IOException, InterruptedException {

			Double moneX = new Double(0);
			Double moneY = new Double(0);
			Double mehaneX = new Double(0);
			Double mehaneY = new Double(0);

			for (Triple val : values) {
				if (val.getFirst().toString().equals("x")) {
					if (val.getSecond().toString().equals("Mone")) {
						moneX = new Double(moneX + Double.parseDouble(val.getThird().toString()));
					} else {
						mehaneX = new Double(mehaneX + Double.parseDouble(val.getThird().toString()));
					}
				} else {
					if (val.getSecond().toString().equals("Mone")) {
						moneY = new Double(moneY + Double.parseDouble(val.getThird().toString()));
					} else {
						mehaneY = new Double(mehaneY + Double.parseDouble(val.getThird().toString()));
					}
				}
			}
			if (!moneX.equals(new Double(0)) && !moneY.equals(new Double(0))) {
				Double simX = new Double(moneX / mehaneX);
				Double simY = new Double(moneY / mehaneY);
				Double S = new Double(Math.sqrt(simX * simY));
				context.write(key, new DoubleWritable(S));
			}
		}
	}
}