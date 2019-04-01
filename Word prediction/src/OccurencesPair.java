import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;

public class OccurencesPair extends Pair<LongWritable, LongWritable> {
	public OccurencesPair() {
		super();
	}
	public OccurencesPair(LongWritable first, LongWritable second) {
		super(first, second);
	}

	@Override
	public void readFields(DataInput data) throws IOException {
		first = new LongWritable(data.readLong());
		second = new LongWritable(data.readLong());
	}

	@Override
	public void write(DataOutput data) throws IOException {
		first.write(data);
		second.write(data);
	}
	
	public int compareTo(Pair<LongWritable, LongWritable> other) {
		int longCompare = first.compareTo(other.getFirst());
		if (longCompare == 0)
			return second.compareTo(other.getSecond());
		return longCompare;
	}
	
	@Override
	public String toString() {
		return first.toString() + "\t" + second.toString();
	}
}
