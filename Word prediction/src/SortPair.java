import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class SortPair extends Pair<Text, Text> implements WritableComparable<SortPair>{
	public SortPair() {
		super();
	}
	public SortPair(Text first, Text second) {
		super(first, second);
	}

	@Override
	public void readFields(DataInput data) throws IOException {
		first = new Text();
		second = new Text();
		first.readFields(data);
		second.readFields(data);
	}

	@Override
	public void write(DataOutput data) throws IOException {
		first.write(data);
		second.write(data);
	}

	@Override
	public int compareTo(SortPair other) {
		int textCompare = first.compareTo(other.getFirst());
		if (textCompare == 0)
			return second.compareTo(other.getSecond());
		return textCompare;
	}
	
	@Override
	public String toString() {
		return first.toString() + "\t" + second.toString();
	}
	
}
