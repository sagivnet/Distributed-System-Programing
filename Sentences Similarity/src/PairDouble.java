import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class PairDouble implements WritableComparable<PairDouble> {
	
	protected Text first;
	protected DoubleWritable second;
	
	public PairDouble() {
		first = null;
		second = null;
	}
	
	public PairDouble(Text first, DoubleWritable second) {
		this.first = first;
		this.second = second;
	}
	
	public void setFirst(Text first) {
		this.first = first;
	}
	public void setSecond(DoubleWritable second) {
		this.second = second;
	}
	
	public Text getFirst() {
		return first;
	}
	public DoubleWritable getSecond() {
		return second;
	}

	@Override
	public void readFields(DataInput data) throws IOException {
		first = new Text();
		first.readFields(data);
		second = new DoubleWritable(data.readDouble());
	}

	@Override
	public void write(DataOutput data) throws IOException {
		first.write(data);
		second.write(data);
	}
	
	public int compareTo(PairDouble other) {
		int textCompare = first.compareTo(other.getFirst());
		if (textCompare == 0)
			return second.compareTo(other.getSecond());
		return textCompare;
	}
	
	@Override
	public String toString() {
		return "<" + first.toString() + "," + second.toString() + ">";
	}
	
	public void fromString(String pair) {
		first = new Text(pair.substring(1, pair.indexOf(",")));
		second = new DoubleWritable(Double.parseDouble(pair.substring(pair.indexOf(",") + 1, pair.indexOf(">"))));
	}
	
	@Override
	public int hashCode() {
		return first.hashCode() + second.hashCode();
	}
}
