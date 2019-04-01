import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Fourth {

	public static class RecordReader {
		private Triple key;
		private PairDouble val;

		public RecordReader(Text value) {
			String[] afterValues = value.toString().split("\\t");
			if (afterValues.length != 2) {
				key = null;
				val = null;
				return;
			}
			key = new Triple();
			val = new PairDouble();
			try {
				key.fromString(afterValues[0]);
				val.fromString(afterValues[1]);
			} catch (Exception e) {
				System.out.println("Error with(mapred 4): " + value.toString());
				key = null;
				val = null;
			}
		}

		public Triple getKey() {
			return key;
		}

		public PairDouble getVal() {
			return val;
		}
	}

	public static class Map extends Mapper<Object, Text, Triple, PairDouble> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			RecordReader line = new RecordReader(value);
			if (line.getKey() == null)
				return;
			context.write(line.getKey(), line.getVal());
		}
	}

	public static class Reduce extends Reducer<Triple, PairDouble, PairTextText, Triple> {
		
//		public void reduce(PairTextText key, Iterable<Triple> values, Context context)
//				throws IOException, InterruptedException {
//			Double sumMIMoneX = new Double(0);
//			Double sumMIMoneY = new Double(0);
//			Double sumMIMehaneX = new Double(0);
//			Double sumMIMehaneY = new Double(0);
//			int mehaneXCounter = 0, mehaneYCounter = 0;
//			for (Triple val : values) {
//				if (val.getFirst().toString().equals("*")) { // mehane
//					if (val.getSecond().toString().equals("x")) { // left
//						sumMIMehaneX += Double.parseDouble(val.getThird().toString());
//						mehaneXCounter++;
//					} else { // right
//						sumMIMehaneY += Double.parseDouble(val.getThird().toString());
//						mehaneYCounter++;
//					}
//				} else { // mone
//					if (val.getSecond().toString().equals("x")) { // left
//						sumMIMoneX += Double.parseDouble(val.getThird().toString());
//					} else { // right
//						sumMIMoneY += Double.parseDouble(val.getThird().toString());
//					}
//				}
//			}
//			if (mehaneXCounter != 1 || mehaneYCounter != 1) {
//				Double simX = sumMIMoneX / sumMIMehaneX;
//				Double simY = sumMIMoneY / sumMIMehaneY;
//				Double S = Math.sqrt(simX * simY);
//				System.out.println("MoneX: " + sumMIMoneX + "\tMehaneX: " + sumMIMehaneX + "\tMoneY: " + sumMIMoneY + "\tMehaneY: " + sumMIMehaneY + "\tSimX: " + simX + "\tSimY: " + simY + "\tS: " + S);
//				context.write(key, new DoubleWritable(S));
//			}
//			
//		}

		public void reduce(Triple key, Iterable<PairDouble> values, Context context)
				throws IOException, InterruptedException {
			if (key.getThird().toString().equals("*")) { // mehane
				for (PairDouble val : values) {
					context.write(
							new PairTextText(new Text(key.getFirst()), new Text(key.getSecond())),
							new Triple(val.getFirst(), new Text("Mehane"), new Text(val.getSecond().toString())));
				}
			} else { // mone
				Double sumMIX = new Double(0);
				Double sumMIY = new Double(0);
				int counterX = 0, counterY = 0;
				for (PairDouble value : values) {
					Double mi = Double.parseDouble(value.getSecond().toString());
					if (value.getFirst().toString().equals("x")) {
						sumMIX = new Double(sumMIX + mi);
						counterX++;
					} else {
						sumMIY = new Double(sumMIY + mi);
						counterY++;
					}
				}
				if (counterX > 1) {
					context.write(new PairTextText(key.getFirst(), key.getSecond()), new Triple(new Text("x"), new Text("Mone"), new Text(sumMIX.toString())));
				}
				if (counterY > 1) {
					context.write(new PairTextText(key.getFirst(), key.getSecond()), new Triple(new Text("y"), new Text("Mone"), new Text(sumMIY.toString())));
				}
			}
		}
	}
}