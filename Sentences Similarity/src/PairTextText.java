import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class PairTextText implements WritableComparable<PairTextText> {
	protected Text first;
	protected Text second;
	
	public PairTextText() {
		first = null;
		second = null;
	}
	
	public PairTextText(Text first, Text second) {
		this.first = first;
		this.second = second;
	}
	
	public void setFirst(Text first) {
		this.first = first;
	}
	public void setSecond(Text second) {
		this.second = second;
	}
	
	public Text getFirst() {
		return first;
	}
	public Text getSecond() {
		return second;
	}

	@Override
	public void readFields(DataInput data) throws IOException {
		first = new Text();
		first.readFields(data);
		second = new Text();
		second.readFields(data);
	}

	@Override
	public void write(DataOutput data) throws IOException {
		first.write(data);
		second.write(data);
	}
	
	public int compareTo(PairTextText other) {
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
		second = new Text(pair.substring(pair.indexOf(",") + 1, pair.indexOf(">")));
	}
	
	@Override
	public int hashCode() {
		return first.hashCode() + second.hashCode();
	}
}
