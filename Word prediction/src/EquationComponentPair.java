import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class EquationComponentPair extends Pair<Text, LongWritable> implements WritableComparable<EquationComponentPair>{
	public EquationComponentPair() {
		super();
	}
	public EquationComponentPair(Text first, LongWritable second) {
		super(first, second);
	}

	@Override
	public void readFields(DataInput data) throws IOException {
		first = new Text();
		second = new LongWritable();
		first.readFields(data);
		second.readFields(data);
	}

	@Override
	public void write(DataOutput data) throws IOException {
		first.write(data);
		second.write(data);
	}

	@Override
	public int compareTo(EquationComponentPair other) {
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
