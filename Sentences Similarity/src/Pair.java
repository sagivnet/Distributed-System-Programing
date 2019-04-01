import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class Pair implements WritableComparable<Pair> {
	protected Text first;
	protected LongWritable second;
	
	public Pair() {
		first = null;
		second = null;
	}
	
	public Pair(Text first, LongWritable second) {
		this.first = first;
		this.second = second;
	}
	
	public void setFirst(Text first) {
		this.first = first;
	}
	public void setSecond(LongWritable second) {
		this.second = second;
	}
	
	public Text getFirst() {
		return first;
	}
	public LongWritable getSecond() {
		return second;
	}

	@Override
	public void readFields(DataInput data) throws IOException {
		first = new Text();
		first.readFields(data);
		second = new LongWritable(data.readLong());
	}

	@Override
	public void write(DataOutput data) throws IOException {
		first.write(data);
		second.write(data);
	}
	
	public int compareTo(Pair other) {
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
		second = new LongWritable(Long.parseLong(pair.substring(pair.indexOf(",") + 1, pair.indexOf(">"))));
	}
	
	@Override
	public int hashCode() {
		return first.hashCode() + second.hashCode();
	}
}
