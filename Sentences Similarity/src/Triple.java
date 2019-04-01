import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class Triple implements WritableComparable<Triple> {
	protected Text first;
	protected Text second;
	protected Text third;
	
	public Triple() {
		first = null;
		second = null;
		third = null;
	}
	
	public Triple(Text first, Text second, Text third) {
		this.first = first;
		this.second = second;
		this.third = third;
	}
	
	public void setFirst(Text first) {
		this.first = first;
	}
	public void setSecond(Text second) {
		this.second = second;
	}
	public void setThird(Text third) {
		this.third = third;
	}
	
	public Text getFirst() {
		return first;
	}
	public Text getSecond() {
		return second;
	}
	public Text getThird() {
		return third;
	}

	@Override
	public void readFields(DataInput data) throws IOException {
		first = new Text();
		second = new Text();
		third = new Text();
		first.readFields(data);
		second.readFields(data);
		third.readFields(data);
	}

	@Override
	public void write(DataOutput data) throws IOException {
		first.write(data);
		second.write(data);
		third.write(data);
	}
	
	public int compareTo(Triple other) {
		int textCompare = first.compareTo(other.getFirst());
		if (textCompare == 0) {
			int textCompare2 = second.compareTo(other.getSecond());
			if (textCompare2 == 0)
				return third.compareTo(other.getThird());
			return textCompare2;
		}
		return textCompare;
	}
	
	@Override
	public String toString() {
		return "<" + first.toString() + "," + second.toString() + "," + third.toString() + ">";
	}
	
	public void fromString(String triple) {
		first = new Text(triple.substring(1, triple.indexOf(",")));
		second = new Text(triple.substring(triple.indexOf(",") + 1, triple.lastIndexOf(",")));
		third = new Text(triple.substring(triple.lastIndexOf(",") + 1, triple.indexOf(">")));
	}
	
	@Override
	public int hashCode() {
		return first.hashCode() + second.hashCode() + third.hashCode();
	}
}
