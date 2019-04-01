import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.tartarus.snowball2.runStemmer;

public class Third {

	public static class RecordReader {
		private Triple key;
		private DoubleWritable MI;

		public RecordReader(Text value) {
			String[] afterValues = value.toString().split("\\t");
			if (afterValues.length != 2) {
				key = null;
				MI = null;
				return;
			}
			key = new Triple();
			key.fromString(afterValues[0]);
			MI = new DoubleWritable(Double.parseDouble(afterValues[1]));
		}

		public Triple getKey() {
			return key;
		}

		public DoubleWritable getMI() {
			return MI;
		}
	}

	public static class Map extends Mapper<Object, Text, Triple, PairDouble> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			RecordReader line = new RecordReader(value);
			if (line.getKey() == null)
				return;
			// <'*',x,w>
			context.write(new Triple(new Text("*"), line.getKey().getSecond(), line.getKey().getThird()),
					new PairDouble(line.getKey().getFirst(), line.getMI()));
			// <p,x,'*'>
			context.write(new Triple(line.getKey().getFirst(), line.getKey().getSecond(), new Text("*")),
					new PairDouble(line.getKey().getThird(), line.getMI()));
		}
	}

	public static class Reduce extends Reducer<Triple, PairDouble, Triple, PairDouble> {
		private HashMap<String, List<String>> testSet;

		private String testSetPos = "X confuse with Y	Y confound with X\r\n" + 
				"X release into Y	X secrete into Y\r\n" + 
				"X suffer from Y	X get Y\r\n" + 
				"X relate to Y	Y cause X\r\n" + 
				"X associate with Y	Y associate with X\r\n" + 
				"X make from Y	X derive from Y\r\n" + 
				"X eradicate Y	X be against Y\r\n" + 
				"X give Y	X expose to Y\r\n" + 
				"X use for Y	X recommend for Y\r\n" + 
				"X die from Y	X die of Y\r\n" + 
				"X cause by Y	Y give X\r\n" + 
				"X cause by Y	X involve Y\r\n" + 
				"X give Y	X indicate for Y\r\n" + 
				"X involve Y	X define as Y\r\n" + 
				"X precede by Y	X complicate Y\r\n" + 
				"X suggest Y	Y accompany by X\r\n" + 
				"X be in Y	Y control with X\r\n" + 
				"X license for Y	X be for Y\r\n" + 
				"X give to Y	X use in Y\r\n" + 
				"X result in Y	X accompany by Y\r\n" + 
				"X require for Y	Y respond to X\r\n" + 
				"X administer for Y	X give for Y\r\n" + 
				"X accompany Y	Y develop with X\r\n" + 
				"X associate with Y	X develop from Y\r\n" + 
				"X accompany Y	X complicate Y\r\n" + 
				"X produce Y	Y secrete by X\r\n" + 
				"X recommend for Y	X administer for Y\r\n" + 
				"X control with Y	Y prevent X\r\n" + 
				"X contain Y	X have Y\r\n" + 
				"X produce Y	X increase Y\r\n" + 
				"X require for Y	X use in Y\r\n" + 
				"X associate with Y	Y result in X\r\n" + 
				"X be against Y	X use in Y\r\n" + 
				"X order for Y	X reduce Y\r\n" + 
				"X enter Y	X reach Y\r\n" + 
				"X be in Y	Y present with X\r\n" + 
				"X occur in Y	X present with Y\r\n" + 
				"X consist of Y	X begin with Y\r\n" + 
				"X relieve by Y	Y prescribe for X\r\n" + 
				"X follow Y	X develop after Y\r\n" + 
				"X confound with Y	X confuse with Y\r\n" + 
				"X give as Y	X use as Y\r\n" + 
				"X offer Y	X provide Y\r\n" + 
				"X control Y	X be in Y\r\n" + 
				"X contain Y	Y find in X\r\n" + 
				"X give to Y	Y take X\r\n" + 
				"X develop Y	Y occur in X\r\n" + 
				"X involve Y	X occur in Y\r\n" + 
				"X improve Y	Y relieve by X\r\n" + 
				"X result from Y	Y associate with X\r\n" + 
				"X result in Y	X produce Y\r\n" + 
				"X induce by Y	Y produce X\r\n" + 
				"X join with Y	Y unite with X\r\n" + 
				"X provide Y	Y obtain with X\r\n" + 
				"X help Y	Y manage with X\r\n" + 
				"X license for Y	X use for Y\r\n" + 
				"X take for Y	X prescribe for Y\r\n" + 
				"X help Y	X prescribe for Y\r\n" + 
				"X base on Y	X make from Y\r\n" + 
				"X alleviate Y	X prescribe for Y\r\n" + 
				"X cause Y	X lead to Y\r\n" + 
				"X use in Y	Y take X\r\n" + 
				"X control with Y	X treat by Y\r\n" + 
				"X accompany by Y	X result in Y\r\n" + 
				"X bring Y	X cause Y\r\n" + 
				"X find in Y	Y associate with X\r\n" + 
				"X give for Y	X take for Y\r\n" + 
				"X have Y	X use for Y\r\n" + 
				"X use for Y	X help Y\r\n" + 
				"X see in Y	X cause by Y\r\n" + 
				"X find Y	X discover Y\r\n" + 
				"X use for Y	X be against Y\r\n" + 
				"X reserve for Y	Y treat by X\r\n" + 
				"X administer for Y	Y relieve by X\r\n" + 
				"X create Y	X produce Y\r\n" + 
				"X lead to Y	X complicate by Y\r\n" + 
				"X develop Y	Y be in X\r\n" + 
				"X characterize by Y	X mean Y\r\n" + 
				"X be in Y	X describe as Y\r\n" + 
				"X associate with Y	Y give X\r\n" + 
				"X treat with Y	Y mask X\r\n" + 
				"X present as Y	Y follow X\r\n" + 
				"X eradicate Y	Y control by X\r\n" + 
				"X stimulate Y	X promote Y\r\n" + 
				"X give for Y	X use in Y\r\n" + 
				"X precede by Y	X cause Y\r\n" + 
				"X use in Y	X use for Y\r\n" + 
				"X kill Y	Y kill by X\r\n" + 
				"X find in Y	X be in Y\r\n" + 
				"X wipe Y	X prevent Y\r\n" + 
				"X use as Y	Y derive from X\r\n" + 
				"X fertilize by Y	Y inject into X\r\n" + 
				"X inactivate by Y	Y inactivate X\r\n" + 
				"X suffer from Y	X develop Y\r\n" + 
				"X treat with Y	X affect by Y\r\n" + 
				"X cause Y	X increase Y\r\n" + 
				"X complicate Y	Y accompany by X\r\n" + 
				"X cause Y	Y precipitate by X\r\n" + 
				"X see in Y	Y accompany by X\r\n" + 
				"X respond to Y	Y prescribe for X\r\n" + 
				"X need for Y	Y treat with X\r\n" + 
				"X control by Y	X control with Y\r\n" + 
				"X order for Y	X be in Y\r\n" + 
				"X occur in Y	Y have X\r\n" + 
				"X derive from Y	X synthesize from Y\r\n" + 
				"X carry by Y	X spread by Y\r\n" + 
				"X develop Y	Y affect X\r\n" + 
				"X associate with Y	X attend with Y\r\n" + 
				"X provide by Y	Y provide X\r\n" + 
				"X control with Y	Y eradicate X\r\n" + 
				"X afford by Y	X provide by Y\r\n" + 
				"X develop from Y	X result from Y\r\n" + 
				"X relieve by Y	X require Y\r\n" + 
				"X convert to Y	X reduce to Y\r\n" + 
				"X use for Y	X require for Y\r\n" + 
				"X destroy Y	X be against Y\r\n" + 
				"X cause Y	X mean Y\r\n" + 
				"X unite with Y	Y meet X\r\n" + 
				"X affect by Y	Y involve X\r\n" + 
				"X base on Y	X prepare from Y\r\n" + 
				"X respond to Y	Y alleviate X\r\n" + 
				"X give Y	X cause Y\r\n" + 
				"X transmit Y	Y spread by X\r\n" + 
				"X convert Y	X break Y\r\n" + 
				"X control by Y	X relieve by Y\r\n" + 
				"X develop into Y	X progress to Y\r\n" + 
				"X prevent Y	X protect from Y\r\n" + 
				"X obtain from Y	X derive from Y\r\n" + 
				"X prescribe for Y	X provide from Y\r\n" + 
				"X accompany Y	X associate with Y\r\n" + 
				"X precede Y	Y characterize by X\r\n" + 
				"X attribute to Y	Y associate with X\r\n" + 
				"X be in Y	Y cause X\r\n" + 
				"X cause by Y	Y aggravate X\r\n" + 
				"X mask Y	Y control with X\r\n" + 
				"X attribute to Y	X be in Y\r\n" + 
				"X convert into Y	X reduce to Y\r\n" + 
				"X affect Y	Y be with X\r\n" + 
				"X use in Y	X need for Y\r\n" + 
				"X occur from Y	Y produce X\r\n" + 
				"X compose of Y	X prepare from Y\r\n" + 
				"X reduce Y	Y relieve by X\r\n" + 
				"X make from Y	X base on Y\r\n" + 
				"X prevent by Y	Y protect against X\r\n" + 
				"X isolate from Y	X produce by Y\r\n" + 
				"X produce Y	Y relate to X\r\n" + 
				"X need for Y	X relieve Y\r\n" + 
				"X prevent Y	X wipe Y\r\n" + 
				"X take for Y	X give for Y\r\n" + 
				"X cure Y	Y cure by X\r\n" + 
				"X require Y	Y prescribe for X\r\n" + 
				"X use for Y	X take for Y\r\n" + 
				"X relieve with Y	Y take for X\r\n" + 
				"X occur with Y	Y lead to X\r\n" + 
				"X need for Y	X prescribe for Y\r\n" + 
				"X take for Y	X help Y\r\n" + 
				"X produce Y	Y occur with X\r\n" + 
				"X destroy Y	X kill Y\r\n" + 
				"X control with Y	Y be against X\r\n" + 
				"X lead to Y	X contribute to Y\r\n" + 
				"X occur in Y	Y contain X\r\n" + 
				"X require for Y	X be in Y\r\n" + 
				"X follow by Y	Y be after X\r\n" + 
				"X cure by Y	X require Y\r\n" + 
				"X develop into Y	X complicate by Y\r\n" + 
				"X kill Y	X use against Y\r\n" + 
				"X control by Y	Y order for X\r\n" + 
				"X encounter Y	X meet Y\r\n" + 
				"X introduce by Y	Y introduce X\r\n" + 
				"X give to Y	X use for Y\r\n" + 
				"X use for Y	X give Y\r\n" + 
				"X complicate Y	X occur with Y\r\n" + 
				"X be in Y	X reduce Y\r\n" + 
				"X penetrate Y	X fertilize Y\r\n" + 
				"X occur during Y	X be during Y\r\n" + 
				"X correct Y	X use for Y\r\n" + 
				"X relieve by Y	Y alleviate X\r\n" + 
				"X require for Y	X relieve Y\r\n" + 
				"X help Y	X decrease Y\r\n" + 
				"X give Y	Y occur in X\r\n" + 
				"X relieve by Y	X treat by Y\r\n" + 
				"X accompany by Y	X occur with Y\r\n" + 
				"X overcome by Y	X treat by Y\r\n" + 
				"X confuse with Y	X confound with Y\r\n" + 
				"X produce Y	Y cause by X\r\n" + 
				"X relieve Y	Y require X\r\n" + 
				"X cause by Y	X occur in Y\r\n" + 
				"X provide Y	X give Y\r\n" + 
				"X contain Y	Y be in X\r\n" + 
				"X be in Y	X administer for Y\r\n" + 
				"X cause by Y	X occur with Y\r\n" + 
				"X treat with Y	X take Y\r\n" + 
				"X recommend for Y	X prescribe for Y\r\n" + 
				"X follow by Y	X produce Y\r\n" + 
				"X relieve Y	X use for Y\r\n" + 
				"X combine with Y	Y join with X\r\n" + 
				"X be in Y	X order for Y\r\n" + 
				"X combine with Y	X use with Y\r\n" + 
				"X provide by Y	Y recommend for X\r\n" + 
				"X develop into Y	X associate with Y\r\n" + 
				"X accompany by Y	X come with Y\r\n" + 
				"X occur in Y	Y suffer from X\r\n" + 
				"X characterize by Y	X lead to Y\r\n" + 
				"X enter Y	X be in Y\r\n" + 
				"X treat by Y	Y eradicate X\r\n" + 
				"X affect Y	X involve Y\r\n" + 
				"X prevent Y	X use against Y\r\n" + 
				"X relieve with Y	Y give for X\r\n" + 
				"X produce Y	X develop Y\r\n" + 
				"X help Y	X reduce Y\r\n" + 
				"X provide Y	Y afford by X\r\n" + 
				"X make from Y	X prepare from Y\r\n" + 
				"X reduce Y	X need for Y\r\n" + 
				"X produce Y	X release Y\r\n" + 
				"X cause Y	Y be from X\r\n" + 
				"X infect with Y	Y infect X\r\n" + 
				"X decrease Y	X reduce Y\r\n" + 
				"X afford Y	Y obtain with X\r\n" + 
				"X suggest Y	Y be in X\r\n" + 
				"X carry Y	X spread Y\r\n" + 
				"X appear as Y	X characterize by Y\r\n" + 
				"X lead to Y	X develop into Y\r\n" + 
				"X provide Y	X contain Y\r\n" + 
				"X occur in Y	X affect Y\r\n" + 
				"X meet Y	Y unite with X\r\n" + 
				"X inactivate Y	Y inactivate by X\r\n" + 
				"X kill Y	Y die from X\r\n" + 
				"X achieve with Y	Y offer X\r\n" + 
				"X indicate for Y	X afford Y\r\n" + 
				"X affect by Y	Y affect X\r\n" + 
				"X use against Y	Y control with X\r\n" + 
				"X suggest Y	Y present with X\r\n" + 
				"X cause by Y	Y progress to X\r\n" + 
				"X prevent by Y	X treat by Y\r\n" + 
				"X indicate Y	Y result in X\r\n" + 
				"X cause by Y	Y manifest as X\r\n" + 
				"X control with Y	Y eliminate X\r\n" + 
				"X treat with Y	Y be against X\r\n" + 
				"X use for Y	X give for Y\r\n" + 
				"X manifest as Y	Y associate with X\r\n" + 
				"X be with Y	Y be in X\r\n" + 
				"X treat by Y	Y need for X\r\n" + 
				"X characterize by Y	X result from Y\r\n" + 
				"X contract Y	X get Y\r\n" + 
				"X increase Y	X cause Y\r\n" + 
				"X manage with Y	Y prescribe for X\r\n" + 
				"X need for Y	Y treat by X\r\n" + 
				"X be against Y	Y control by X\r\n" + 
				"X reduce Y	Y relieve with X\r\n" + 
				"X occur in Y	Y occur with X\r\n" + 
				"X give Y	X bring Y\r\n" + 
				"X infect with Y	Y occur in X\r\n" + 
				"X define as Y	Y associate with X\r\n" + 
				"X associate with Y	Y find in X\r\n" + 
				"X make Y	Y produce by X\r\n" + 
				"X obtain with Y	Y offer X\r\n" + 
				"X prescribe for Y	X control Y\r\n" + 
				"X occur with Y	Y result in X\r\n" + 
				"X progress to Y	X associate with Y\r\n" + 
				"X need for Y	X control Y\r\n" + 
				"X correct by Y	X treat with Y\r\n" + 
				"X correct by Y	X correct with Y\r\n" + 
				"X consist of Y	Y use as X\r\n" + 
				"X alleviate Y	Y respond to X\r\n" + 
				"X associate with Y	Y refer to X\r\n" + 
				"X eradicate Y	X eliminate Y\r\n" + 
				"X relieve with Y	Y mask X\r\n" + 
				"X have Y	X use as Y\r\n" + 
				"X secrete by Y	X produce by Y\r\n" + 
				"X begin as Y	Y complicate X\r\n" + 
				"X be in Y	X cause by Y\r\n" + 
				"X present with Y	Y suggest X\r\n" + 
				"X eliminate Y	X prevent Y\r\n" + 
				"X reduce Y	X indicate for Y\r\n" + 
				"X treat with Y	Y order for X\r\n" + 
				"X be after Y	X follow Y\r\n" + 
				"X reduce Y	X relieve Y\r\n" + 
				"X alleviate Y	X decrease Y\r\n" + 
				"X reduce Y	Y control by X\r\n" + 
				"X characterize by Y	Y result from X\r\n" + 
				"X follow Y	X characterize by Y\r\n" + 
				"X produce Y	X secrete Y\r\n" + 
				"X affect by Y	Y eradicate X\r\n" + 
				"X derive from Y	Y use as X\r\n" + 
				"X characterize by Y	Y cause X\r\n" + 
				"X start with Y	X characterize by Y\r\n" + 
				"X enter Y	X inject into Y\r\n" + 
				"X prevent Y	X prescribe for Y\r\n" + 
				"X associate with Y	X produce by Y\r\n" + 
				"X treat Y	Y treat with X\r\n" + 
				"X fertilize Y	X enter Y\r\n" + 
				"X make from Y	X consist of Y\r\n" + 
				"X follow by Y	Y develop after X\r\n" + 
				"X suggest Y	Y lead to X\r\n" + 
				"X relieve by Y	Y relieve X\r\n" + 
				"X administer to Y	X use in Y\r\n" + 
				"X combine with Y	X meet Y\r\n" + 
				"X manifest by Y	Y cause by X\r\n" + 
				"X treat with Y	X correct with Y\r\n" + 
				"X release Y	Y produce by X\r\n" + 
				"X complicate with Y	Y complicate X\r\n" + 
				"X penetrate Y	X inject into Y\r\n" + 
				"X order for Y	Y control by X\r\n" + 
				"X contribute to Y	Y cause by X\r\n" + 
				"X differ from Y	Y differ from X\r\n" + 
				"X associate with Y	Y manifest as X\r\n" + 
				"X see in Y	Y characterize by X\r\n" + 
				"X need for Y	X give for Y\r\n" + 
				"X precipitate by Y	Y cause X\r\n" + 
				"X result from Y	X produce by Y\r\n" + 
				"X describe as Y	X be in Y\r\n" + 
				"X treat with Y	X correct by Y\r\n" + 
				"X contribute to Y	X lead to Y\r\n" + 
				"X characterize by Y	X manifest by Y\r\n" + 
				"X control Y	Y cure with X\r\n" + 
				"X require for Y	Y relieve by X\r\n" + 
				"X contain Y	Y occur in X\r\n" + 
				"X accompany by Y	X attend with Y\r\n" + 
				"X order for Y	Y treat with X\r\n" + 
				"X present as Y	Y be in X\r\n" + 
				"X give Y	Y use in X\r\n" + 
				"X develop from Y	X associate with Y\r\n" + 
				"X be from Y	Y cause X\r\n" + 
				"X derive from Y	X produce by Y\r\n" + 
				"X produce by Y	Y manufacture X\r\n" + 
				"X have Y	X receive Y\r\n" + 
				"X carry Y	Y spread by X\r\n" + 
				"X give before Y	X start before Y\r\n" + 
				"X be in Y	X alleviate Y\r\n" + 
				"X occur with Y	Y associate with X\r\n" + 
				"X define as Y	X refer to Y\r\n" + 
				"X treat Y	Y treat by X\r\n" + 
				"X characterize by Y	X include Y\r\n" + 
				"X provide from Y	Y treat with X\r\n" + 
				"X result from Y	Y cause X\r\n" + 
				"X occur from Y	X cause by Y\r\n" + 
				"X indicate for Y	X provide Y\r\n" + 
				"X combine with Y	Y include X\r\n" + 
				"X affect in Y	Y involve X\r\n" + 
				"X observe in Y	X occur in Y\r\n" + 
				"X release Y	X make Y\r\n" + 
				"X mask Y	Y relieve by X\r\n" + 
				"X attend with Y	X accompany by Y\r\n" + 
				"X carry by Y	Y transmit X\r\n" + 
				"X relieve Y	X prescribe for Y\r\n" + 
				"X produce Y	X bring Y\r\n" + 
				"X use for Y	Y relieve by X\r\n" + 
				"X associate with Y	X occur in Y\r\n" + 
				"X prepare from Y	X base on Y\r\n" + 
				"X confuse with Y	X distinguish from Y\r\n" + 
				"X decrease Y	X control Y\r\n" + 
				"X control with Y	X relieve with Y\r\n" + 
				"X prevent Y	X treat Y\r\n" + 
				"X suggest Y	X result in Y\r\n" + 
				"X give for Y	X be for Y\r\n" + 
				"X reduce Y	X suppress Y\r\n" + 
				"X relieve with Y	Y control X\r\n" + 
				"X need for Y	X be in Y\r\n" + 
				"X carry Y	Y find in X\r\n" + 
				"X produce Y	X lead to Y\r\n" + 
				"X accompany Y	X follow Y\r\n" + 
				"X succeed to Y	Y follow by X\r\n" + 
				"X present with Y	Y indicate X\r\n" + 
				"X decrease Y	Y control with X\r\n" + 
				"X stimulate Y	X produce Y\r\n" + 
				"X convert to Y	Y form from X\r\n" + 
				"X metabolize to Y	X reduce to Y\r\n" + 
				"X convert by Y	X metabolize by Y\r\n" + 
				"X induce Y	X provide Y\r\n" + 
				"X mask Y	X control Y\r\n" + 
				"X complicate Y	Y complicate by X\r\n" + 
				"X follow Y	X suggest Y\r\n" + 
				"X control Y	X decrease Y\r\n" + 
				"X relieve by Y	Y take for X\r\n" + 
				"X afford Y	X offer Y\r\n" + 
				"X recommend for Y	X indicate for Y\r\n" + 
				"X indicate in Y	X indicate for Y\r\n" + 
				"X control by Y	Y need for X\r\n" + 
				"X be for Y	X recommend for Y\r\n" + 
				"X require Y	X treat by Y\r\n" + 
				"X use as Y	Y consist of X\r\n" + 
				"X associate with Y	X result in Y\r\n" + 
				"X associate with Y	X see with Y\r\n" + 
				"X join with Y	X unite with Y\r\n" + 
				"X give Y	Y associate with X\r\n" + 
				"X relieve Y	Y relieve with X\r\n" + 
				"X cause by Y	Y occur in X\r\n" + 
				"X indicate for Y	X recommend for Y\r\n" + 
				"X inject into Y	Y fertilize by X\r\n" + 
				"X order for Y	Y treat by X\r\n" + 
				"X alternate with Y	X give to Y\r\n" + 
				"X refer to Y	X cause by Y\r\n" + 
				"X correct by Y	Y correct X\r\n" + 
				"X mask Y	Y control by X\r\n" + 
				"X require for Y	X need for Y\r\n" + 
				"X cause Y	X develop with Y\r\n" + 
				"X make from Y	X compose of Y\r\n" + 
				"X spread through Y	X transmit by Y\r\n" + 
				"X isolate from Y	X derive from Y\r\n" + 
				"X give for Y	X prescribe for Y\r\n" + 
				"X affect Y	X develop in Y\r\n" + 
				"X manage with Y	Y mask X\r\n" + 
				"X predispose to Y	X lead to Y\r\n" + 
				"X transmit Y	Y transmit by X\r\n" + 
				"X transmit by Y	X find in Y\r\n" + 
				"X occur with Y	Y cause X\r\n" + 
				"X infect with Y	X have Y\r\n" + 
				"X indicate for Y	Y achieve with X\r\n" + 
				"X associate with Y	Y accompany with X\r\n" + 
				"X use for Y	X administer for Y\r\n" + 
				"X relate to Y	Y result in X\r\n" + 
				"X develop with Y	Y accompany X\r\n" + 
				"X relieve Y	Y treat with X\r\n" + 
				"X be in Y	Y present as X\r\n" + 
				"X provide Y	X recommend for Y\r\n" + 
				"X use in Y	X give to Y\r\n" + 
				"X enter Y	Y penetrate by X\r\n" + 
				"X use Y	X have Y\r\n" + 
				"X control by Y	X overcome by Y\r\n" + 
				"X produce Y	X find Y\r\n" + 
				"X afford Y	X prescribe for Y\r\n" + 
				"X use for Y	X order for Y\r\n" + 
				"X use for Y	Y correct with X\r\n" + 
				"X treat with Y	Y provide from X\r\n" + 
				"X convert by Y	Y convert X\r\n" + 
				"X present with Y	Y associate with X\r\n" + 
				"X find in Y	X appear in Y\r\n" + 
				"X afford by Y	Y offer X\r\n" + 
				"X follow by Y	X complicate by Y\r\n" + 
				"X produce Y	X precede by Y\r\n" + 
				"X begin with Y	X present with Y\r\n" + 
				"X require Y	X correct by Y\r\n" + 
				"X carry by Y	Y infect with X\r\n" + 
				"X correct with Y	X correct by Y\r\n" + 
				"X transmit by Y	X spread by Y\r\n" + 
				"X lead to Y	X present as Y\r\n" + 
				"X overcome by Y	X control by Y\r\n" + 
				"X cause Y	X trigger Y\r\n" + 
				"X achieve with Y	Y prescribe for X\r\n" + 
				"X cause Y	Y mean X\r\n" + 
				"X unite with Y	X encounter Y\r\n" + 
				"X occur in Y	X suggest Y\r\n" + 
				"X cause by Y	X define as Y\r\n" + 
				"X contract Y	X develop Y\r\n" + 
				"X treat with Y	X cure with Y\r\n" + 
				"X achieve with Y	Y give X\r\n" + 
				"X destroy Y	X attack Y\r\n" + 
				"X produce in Y	X produce by Y\r\n" + 
				"X be during Y	X be in Y\r\n" + 
				"X prescribe for Y	Y relieve with X\r\n" + 
				"X be in Y	Y relieve by X\r\n" + 
				"X alleviate Y	Y relieve by X\r\n" + 
				"X affect by Y	X treat with Y\r\n" + 
				"X be in Y	X develop in Y\r\n" + 
				"X complicate Y	Y begin as X\r\n" + 
				"X reduce Y	X give for Y\r\n" + 
				"X treat by Y	X relieve by Y\r\n" + 
				"X give Y	X treat with Y\r\n" + 
				"X come with Y	X associate with Y\r\n" + 
				"X suggest Y	X see in Y\r\n" + 
				"X relieve Y	X need for Y\r\n" + 
				"X suggest Y	Y follow X\r\n" + 
				"X correct Y	Y relieve by X\r\n" + 
				"X accompany Y	Y accompany X\r\n" + 
				"X contain Y	X compose of Y\r\n" + 
				"X involve Y	X associate with Y\r\n" + 
				"X promote Y	X stimulate Y\r\n" + 
				"X relieve Y	X reduce Y\r\n" + 
				"X associate with Y	X develop Y\r\n" + 
				"X occur in Y	Y cause X\r\n" + 
				"X complicate Y	X result from Y\r\n" + 
				"X afford Y	X provide Y\r\n" + 
				"X cure Y	X control Y\r\n" + 
				"X reduce to Y	X metabolize to Y\r\n" + 
				"X break Y	X convert Y\r\n" + 
				"X fertilize Y	X penetrate Y\r\n" + 
				"X follow Y	Y associate with X\r\n" + 
				"X relieve Y	X help Y\r\n" + 
				"X treat Y	Y control with X\r\n" + 
				"X kill Y	Y affect by X\r\n" + 
				"X include Y	Y use for X\r\n" + 
				"X eliminate Y	Y treat by X\r\n" + 
				"X obtain with Y	Y give X\r\n" + 
				"X produce Y	X induce Y\r\n" + 
				"X occur with Y	Y follow X\r\n" + 
				"X treat with Y	Y prescribe for X\r\n" + 
				"X relieve by Y	Y need for X\r\n" + 
				"X produce Y	X introduce Y\r\n" + 
				"X decrease Y	Y relieve by X\r\n" + 
				"X present with Y	Y see in X\r\n" + 
				"X use for Y	Y manage with X\r\n" + 
				"X relate to Y	Y produce X\r\n" + 
				"X enter Y	X release into Y\r\n" + 
				"X make of Y	X make from Y\r\n" + 
				"X prescribe for Y	X give for Y\r\n" + 
				"X be against Y	Y treat by X\r\n" + 
				"X relieve Y	X provide from Y\r\n" + 
				"X relieve by Y	X relieve with Y\r\n" + 
				"X manage with Y	X treat by Y\r\n" + 
				"X cause Y	Y characterize by X\r\n" + 
				"X eradicate Y	X use against Y\r\n" + 
				"X be for Y	X provide Y\r\n" + 
				"X mistake for Y	Y confound with X\r\n" + 
				"X administer for Y	X be for Y\r\n" + 
				"X kill by Y	Y be against X\r\n" + 
				"X associate with Y	X give to Y\r\n" + 
				"X control with Y	Y cure X\r\n" + 
				"X give Y	Y obtain with X\r\n" + 
				"X occur after Y	Y result in X\r\n" + 
				"X help Y	X use for Y\r\n" + 
				"X occur after Y	X result from Y\r\n" + 
				"X mask Y	X help Y\r\n" + 
				"X develop from Y	Y produce X\r\n" + 
				"X cause by Y	X result from Y\r\n" + 
				"X develop from Y	Y result in X\r\n" + 
				"X progress to Y	X develop into Y\r\n" + 
				"X occur during Y	X occur in Y\r\n" + 
				"X give for Y	X relieve Y\r\n" + 
				"X occur in Y	Y produce X\r\n" + 
				"X prepare from Y	X make from Y\r\n" + 
				"X need for Y	X order for Y\r\n" + 
				"X be in Y	X administer to Y\r\n" + 
				"X occur after Y	Y lead to X\r\n" + 
				"X respond to Y	Y reduce X\r\n" + 
				"X cure Y	Y cure with X\r\n" + 
				"X be in Y	Y relieve with X\r\n" + 
				"X decrease Y	X use for Y\r\n" + 
				"X develop after Y	Y follow by X\r\n" + 
				"X control Y	X reduce Y\r\n" + 
				"X give Y	X recommend for Y\r\n" + 
				"X carry Y	Y occur in X\r\n" + 
				"X suggest Y	Y manifest by X\r\n" + 
				"X unite with Y	Y encounter X\r\n" + 
				"X give Y	Y cause by X\r\n" + 
				"X eliminate Y	Y cure by X\r\n" + 
				"X help Y	Y relieve with X\r\n" + 
				"X confuse with Y	X differentiate from Y\r\n" + 
				"X relieve by Y	Y be in X\r\n" + 
				"X provide Y	X be for Y\r\n" + 
				"X produce by Y	Y cause X\r\n" + 
				"X use for Y	X give with Y\r\n" + 
				"X cause by Y	X accompany by Y\r\n" + 
				"X kill Y	X reduce Y\r\n" + 
				"X give for Y	X reduce Y\r\n" + 
				"X associate with Y	Y attribute to X\r\n" + 
				"X control with Y	Y kill X\r\n" + 
				"X penetrate Y	Y fertilize by X\r\n" + 
				"X affect Y	X infect Y\r\n" + 
				"X give to Y	X follow by Y\r\n" + 
				"X manage with Y	Y reduce X\r\n" + 
				"X treat with Y	X treat by Y\r\n" + 
				"X obtain from Y	X produce by Y\r\n" + 
				"X bring Y	X be for Y\r\n" + 
				"X cure with Y	Y cure X\r\n" + 
				"X relieve Y	Y relieve by X\r\n" + 
				"X metabolize in Y	X metabolize by Y\r\n" + 
				"X inject into Y	X enter Y\r\n" + 
				"X require for Y	X recommend for Y\r\n" + 
				"X cause Y	X start with Y\r\n" + 
				"X help Y	X mask Y\r\n" + 
				"X give for Y	X need for Y\r\n" + 
				"X manifest by Y	X characterize by Y\r\n" + 
				"X complicate Y	X precede by Y\r\n" + 
				"X characterize by Y	X be in Y\r\n" + 
				"X result from Y	Y characterize by X\r\n" + 
				"X turn into Y	X develop into Y\r\n" + 
				"X treat by Y	X correct with Y\r\n" + 
				"X suggest Y	X associate with Y\r\n" + 
				"X manifest by Y	Y see in X\r\n" + 
				"X obtain from Y	X isolate from Y\r\n" + 
				"X characterize by Y	X start with Y\r\n" + 
				"X occur in Y	X observe in Y\r\n" + 
				"X present with Y	Y cause by X\r\n" + 
				"X obtain with Y	X achieve with Y\r\n" + 
				"X introduce Y	X make Y\r\n" + 
				"X accompany Y	X cause by Y\r\n" + 
				"X find in Y	Y characterize by X\r\n" + 
				"X produce by Y	X isolate from Y\r\n" + 
				"X prescribe for Y	Y relieve by X\r\n" + 
				"X suggest Y	Y occur in X\r\n" + 
				"X result in Y	X present as Y\r\n" + 
				"X follow by Y	X associate with Y\r\n" + 
				"X include Y	Y produce by X\r\n" + 
				"X introduce Y	X produce Y\r\n" + 
				"X involve Y	Y associate with X\r\n" + 
				"X use in Y	X administer for Y\r\n" + 
				"X occur with Y	X associate with Y\r\n" + 
				"X begin with Y	Y indicate X\r\n" + 
				"X occur in Y	Y associate with X\r\n" + 
				"X recommend for Y	Y achieve with X\r\n" + 
				"X present as Y	X present with Y\r\n" + 
				"X present with Y	X begin with Y\r\n" + 
				"X encounter Y	Y join with X\r\n" + 
				"X provide from Y	Y control by X\r\n" + 
				"X secrete by Y	Y secrete X\r\n" + 
				"X see in Y	Y associate with X\r\n" + 
				"X control Y	Y relieve with X\r\n" + 
				"X cause Y	Y occur in X\r\n" + 
				"X protect from Y	X protect against Y\r\n" + 
				"X reduce Y	X decrease Y\r\n" + 
				"X mean Y	Y associate with X\r\n" + 
				"X lead to Y	X turn into Y\r\n" + 
				"X accompany by Y	X mean Y\r\n" + 
				"X recommend for Y	Y provide by X\r\n" + 
				"X terminate in Y	Y follow X\r\n" + 
				"X spread by Y	Y carry X\r\n" + 
				"X transmit Y	X carry Y\r\n" + 
				"X be for Y	X require for Y\r\n" + 
				"X suffer from Y	X attack with Y\r\n" + 
				"X be in Y	X require for Y\r\n" + 
				"X destroy Y	Y affect by X\r\n" + 
				"X need for Y	X use for Y\r\n" + 
				"X accompany by Y	Y follow by X\r\n" + 
				"X control by Y	Y eradicate X\r\n" + 
				"X provide by Y	Y offer X\r\n" + 
				"X treat by Y	Y prescribe for X\r\n" + 
				"X prevent Y	Y respond to X\r\n" + 
				"X include Y	Y refer to X\r\n" + 
				"X cause by Y	Y result in X\r\n" + 
				"X have Y	Y find in X\r\n" + 
				"X lead to Y	X produce Y\r\n" + 
				"X give with Y	X use for Y\r\n" + 
				"X provide from Y	X control Y\r\n" + 
				"X characterize by Y	X present with Y\r\n" + 
				"X lead to Y	Y develop from X\r\n" + 
				"X control Y	X cure Y\r\n" + 
				"X increase Y	X produce Y\r\n" + 
				"X develop Y	X suffer from Y\r\n" + 
				"X accompany Y	Y mean X\r\n" + 
				"X find in Y	Y infect with X\r\n" + 
				"X follow by Y	X alternate with Y\r\n" + 
				"X present with Y	X cause Y\r\n" + 
				"X use for Y	X provide from Y\r\n" + 
				"X prepare from Y	X compose of Y\r\n" + 
				"X confound with Y	X mistake for Y\r\n" + 
				"X control by Y	X prevent by Y\r\n" + 
				"X relieve by Y	X treat with Y\r\n" + 
				"X treat with Y	Y eradicate X\r\n" + 
				"X cause Y	X suggest Y\r\n" + 
				"X control Y	X help Y\r\n" + 
				"X cause by Y	Y occur with X\r\n" + 
				"X lead to Y	X progress to Y\r\n" + 
				"X affect Y	X be in Y\r\n" + 
				"X be in Y	Y be with X\r\n" + 
				"X produce Y	X manifest as Y\r\n" + 
				"X have Y	X get Y\r\n" + 
				"X recommend for Y	Y treat with X\r\n" + 
				"X administer to Y	X give to Y\r\n" + 
				"X find in Y	X see in Y\r\n" + 
				"X result from Y	X be in Y\r\n" + 
				"X develop into Y	Y associate with X\r\n" + 
				"X control with Y	X treat with Y\r\n" + 
				"X give for Y	X require for Y\r\n" + 
				"X inject into Y	X get into Y\r\n" + 
				"X confound with Y	X resemble Y\r\n" + 
				"X control by Y	Y alleviate X\r\n" + 
				"X complicate by Y	Y complicate X\r\n" + 
				"X prevent by Y	X control by Y\r\n" + 
				"X prescribe for Y	X order for Y\r\n" + 
				"X control with Y	Y prescribe for X\r\n" + 
				"X use for Y	Y provide by X\r\n" + 
				"X occur in Y	X be during Y\r\n" + 
				"X occur after Y	X associate with Y\r\n" + 
				"X treat by Y	Y order for X\r\n" + 
				"X suggest Y	Y present as X\r\n" + 
				"X require for Y	X use for Y\r\n" + 
				"X carry Y	X contain Y\r\n" + 
				"X cause by Y	X mean Y\r\n" + 
				"X confuse with Y	Y distinguish from X\r\n" + 
				"X order for Y	Y control with X\r\n" + 
				"X associate with Y	Y come with X\r\n" + 
				"X achieve with Y	X obtain with Y\r\n" + 
				"X relieve by Y	X correct by Y\r\n" + 
				"X imply Y	X mean Y\r\n" + 
				"X wipe Y	X be against Y\r\n" + 
				"X lead to Y	Y prevent by X\r\n" + 
				"X lead to Y	Y relate to X\r\n" + 
				"X provide from Y	Y control with X\r\n" + 
				"X develop into Y	X suggest Y\r\n" + 
				"X manifest by Y	X result in Y\r\n" + 
				"X find in Y	Y contain X\r\n" + 
				"X result from Y	Y complicate by X\r\n" + 
				"X use Y	Y use for X\r\n" + 
				"X use for Y	X give to Y\r\n" + 
				"X use in Y	X recommend for Y\r\n" + 
				"X relieve Y	Y correct by X\r\n" + 
				"X prescribe for Y	Y obtain with X\r\n" + 
				"X prescribe for Y	X administer for Y\r\n" + 
				"X cure with Y	X treat with Y\r\n" + 
				"X occur with Y	Y occur in X\r\n" + 
				"X occur from Y	X associate with Y\r\n" + 
				"X follow by Y	X cause Y\r\n" + 
				"X treat by Y	X cure with Y\r\n" + 
				"X be in Y	X need for Y\r\n" + 
				"X administer for Y	X use for Y\r\n" + 
				"X afford Y	X be for Y\r\n" + 
				"X use for Y	Y afford by X\r\n" + 
				"X refer to Y	Y include X\r\n" + 
				"X alleviate Y	X give for Y\r\n" + 
				"X order for Y	X relieve Y\r\n" + 
				"X use as Y	X give as Y\r\n" + 
				"X contain Y	X provide Y\r\n" + 
				"X give Y	X use for Y\r\n" + 
				"X associate with Y	X accompany with Y\r\n" + 
				"X prevent Y	Y correct by X\r\n" + 
				"X respond to Y	Y help X\r\n" + 
				"X suggest Y	Y cause by X\r\n" + 
				"X cause Y	Y develop after X\r\n" + 
				"X occur during Y	X be in Y\r\n" + 
				"X control Y	Y control by X\r\n" + 
				"X respond to Y	Y relieve X\r\n" + 
				"X enter Y	X fertilize Y\r\n" + 
				"X control by Y	X manage with Y\r\n" + 
				"X use in Y	Y be with X\r\n" + 
				"X reduce Y	Y control with X\r\n" + 
				"X produce Y	Y obtain with X\r\n" + 
				"X hydrolyze Y	X digest Y\r\n" + 
				"X correct by Y	Y prevent X\r\n" + 
				"X obtain from Y	Y provide X\r\n" + 
				"X infect Y	X affect Y\r\n" + 
				"X take for Y	X relieve Y\r\n" + 
				"X occur in Y	Y precede by X\r\n" + 
				"X be for Y	X license for Y\r\n" + 
				"X attack Y	Y affect by X\r\n" + 
				"X order for Y	X be for Y\r\n" + 
				"X convert to Y	X metabolize to Y\r\n" + 
				"X result from Y	X occur from Y\r\n" + 
				"X die of Y	X have Y\r\n" + 
				"X accompany Y	X accompany by Y\r\n" + 
				"X control with Y	Y control X\r\n" + 
				"X lead to Y	Y cause by X\r\n" + 
				"X characterize by Y	Y indicate X\r\n" + 
				"X relate to Y	X suggest Y\r\n" + 
				"X develop in Y	X occur in Y\r\n" + 
				"X result from Y	X cause by Y\r\n" + 
				"X use for Y	X alleviate Y\r\n" + 
				"X give to Y	X alternate with Y\r\n" + 
				"X precede by Y	Y occur in X\r\n" + 
				"X see with Y	X associate with Y\r\n" + 
				"X refer to Y	X involve Y\r\n" + 
				"X present Y	Y find in X\r\n" + 
				"X increase Y	X associate with Y\r\n" + 
				"X require for Y	Y treat with X\r\n" + 
				"X be for Y	X administer for Y\r\n" + 
				"X know as Y	X include Y\r\n" + 
				"X contain Y	Y obtain from X\r\n" + 
				"X prescribe for Y	X alleviate Y\r\n" + 
				"X see in Y	X associate with Y\r\n" + 
				"X break Y	X hydrolyze Y\r\n" + 
				"X occur in Y	X associate with Y\r\n" + 
				"X develop Y	X transmit Y\r\n" + 
				"X accompany by Y	Y occur with X\r\n" + 
				"X use during Y	X give during Y\r\n" + 
				"X invent Y	X discover Y\r\n" + 
				"X decrease Y	X relieve Y\r\n" + 
				"X transmit Y	Y find in X\r\n" + 
				"X cure with Y	X control by Y\r\n" + 
				"X treat with Y	Y reduce X\r\n" + 
				"X receive Y	Y be in X\r\n" + 
				"X relieve with Y	X manage with Y\r\n" + 
				"X respond to Y	X relieve with Y\r\n" + 
				"X provide from Y	X give for Y\r\n" + 
				"X fertilize by Y	X penetrate by Y\r\n" + 
				"X appear in Y	X find in Y\r\n" + 
				"X fertilize Y	X inject into Y\r\n" + 
				"X kill Y	X be for Y\r\n" + 
				"X treat by Y	X control by Y\r\n" + 
				"X respond to Y	Y be against X\r\n" + 
				"X precede Y	Y complicate X\r\n" + 
				"X cause Y	X precipitate Y\r\n" + 
				"X produce Y	X contribute to Y\r\n" + 
				"X indicate Y	Y occur with X\r\n" + 
				"X lead to Y	Y result from X\r\n" + 
				"X start with Y	Y suggest X\r\n" + 
				"X associate with Y	X involve Y\r\n" + 
				"X control with Y	Y indicate for X\r\n" + 
				"X digest Y	X break Y\r\n" + 
				"X be against Y	X wipe Y\r\n" + 
				"X use during Y	X use in Y\r\n" + 
				"X accompany Y	X cause Y\r\n" + 
				"X precipitate Y	X lead to Y\r\n" + 
				"X treat Y	Y respond to X\r\n" + 
				"X lead to Y	Y occur with X\r\n" + 
				"X require for Y	X order for Y\r\n" + 
				"X use for Y	Y respond to X\r\n" + 
				"X encounter Y	Y meet X\r\n" + 
				"X license for Y	X use in Y\r\n" + 
				"X present as Y	X associate with Y\r\n" + 
				"X mean Y	Y cause X\r\n" + 
				"X control by Y	Y prevent X\r\n" + 
				"X differ from Y	Y distinguish from X\r\n" + 
				"X require for Y	X administer for Y\r\n" + 
				"X need for Y	X administer for Y\r\n" + 
				"X develop for Y	X recommend for Y\r\n" + 
				"X characterize by Y	X occur in Y\r\n" + 
				"X transmit Y	Y be in X\r\n" + 
				"X give for Y	Y relieve with X\r\n" + 
				"X infect with Y	Y transmit by X\r\n" + 
				"X associate with Y	Y begin as X\r\n" + 
				"X treat with Y	Y need for X\r\n" + 
				"X die of Y	Y kill X\r\n" + 
				"X produce by Y	X secrete by Y\r\n" + 
				"X kill Y	Y control with X\r\n" + 
				"X unite with Y	Y join with X\r\n" + 
				"X precipitate Y	X trigger Y\r\n" + 
				"X prescribe for Y	Y respond to X\r\n" + 
				"X use for Y	X license for Y\r\n" + 
				"X occur after Y	Y follow by X\r\n" + 
				"X produce Y	Y occur from X\r\n" + 
				"X induce Y	X cause Y\r\n" + 
				"X transmit Y	X have Y\r\n" + 
				"X involve Y	X cause by Y\r\n" + 
				"X eliminate Y	X kill Y\r\n" + 
				"X precede Y	Y follow X\r\n" + 
				"X encounter Y	X combine with Y\r\n" + 
				"X recommend for Y	X use in Y\r\n" + 
				"X prevent by Y	X treat with Y\r\n" + 
				"X contain Y	Y use in X\r\n" + 
				"X have Y	X carry Y\r\n" + 
				"X manage with Y	X treat with Y\r\n" + 
				"X make Y	Y introduce by X\r\n" + 
				"X form from Y	Y convert to X\r\n" + 
				"X treat with Y	Y treat X\r\n" + 
				"X meet Y	Y meet X\r\n" + 
				"X correct with Y	Y correct X\r\n" + 
				"X differ from Y	X distinguish from Y\r\n" + 
				"X give to Y	X characterize by Y\r\n" + 
				"X associate with Y	Y involve X\r\n" + 
				"X occur with Y	Y follow by X\r\n" + 
				"X produce Y	X ejaculate Y\r\n" + 
				"X treat Y	X control Y\r\n" + 
				"X cause by Y	Y manifest by X\r\n" + 
				"X need for Y	Y manage with X\r\n" + 
				"X be against Y	X eradicate Y\r\n" + 
				"X be during Y	X occur in Y\r\n" + 
				"X find Y	X invent Y\r\n" + 
				"X need for Y	Y relieve by X\r\n" + 
				"X characterize by Y	X present as Y\r\n" + 
				"X attribute to Y	X follow Y\r\n" + 
				"X administer for Y	X prescribe for Y\r\n" + 
				"X be in Y	X recommend for Y\r\n" + 
				"X offer Y	Y afford by X\r\n" + 
				"X lead to Y	X result in Y\r\n" + 
				"X suggest Y	X result from Y\r\n" + 
				"X accompany by Y	X cause by Y\r\n" + 
				"X cause Y	X bring Y\r\n" + 
				"X produce Y	Y occur after X\r\n" + 
				"X be in Y	Y affect by X\r\n" + 
				"X introduce by Y	Y make X\r\n" + 
				"X cure by Y	Y be in X\r\n" + 
				"X manage with Y	X relieve by Y\r\n" + 
				"X involve Y	Y affect by X\r\n" + 
				"X be for Y	X prescribe for Y\r\n" + 
				"X die from Y	Y kill X\r\n" + 
				"X associate with Y	X indicate Y\r\n" + 
				"X be in Y	X find in Y\r\n" + 
				"X convert Y	X hydrolyze Y\r\n" + 
				"X prevent by Y	Y prevent X\r\n" + 
				"X help Y	X control Y\r\n" + 
				"X eliminate Y	Y treat with X\r\n" + 
				"X produce Y	X create Y\r\n" + 
				"X produce Y	Y occur in X\r\n" + 
				"X treat by Y	Y reserve for X\r\n" + 
				"X find in Y	Y include X\r\n" + 
				"X present with Y	X produce Y\r\n" + 
				"X produce Y	Y isolate from X\r\n" + 
				"X give for Y	Y relieve by X\r\n" + 
				"X administer for Y	X help Y\r\n" + 
				"X result from Y	Y lead to X\r\n" + 
				"X afford Y	Y achieve with X\r\n" + 
				"X manifest by Y	X cause Y\r\n" + 
				"X be for Y	X afford Y\r\n" + 
				"X affect by Y	X respond to Y\r\n" + 
				"X result from Y	X indicate Y\r\n" + 
				"X lead to Y	Y suggest X\r\n" + 
				"X see in Y	X suggest Y\r\n" + 
				"X wipe Y	Y control with X\r\n" + 
				"X cause Y	X contribute to Y\r\n" + 
				"X occur in Y	Y cause by X\r\n" + 
				"X control with Y	X cure with Y\r\n" + 
				"X provide Y	Y provide by X\r\n" + 
				"X reduce Y	X alleviate Y\r\n" + 
				"X follow Y	X occur after Y\r\n" + 
				"X substitute for Y	Y substitute for X\r\n" + 
				"X offer Y	X be for Y\r\n" + 
				"X form from Y	Y convert into X\r\n" + 
				"X characterize by Y	Y cause by X\r\n" + 
				"X consist of Y	X compose of Y\r\n" + 
				"X require Y	Y cure X\r\n" + 
				"X be with Y	X begin with Y\r\n" + 
				"X relieve with Y	Y be in X\r\n" + 
				"X associate with Y	X present with Y\r\n" + 
				"X treat with Y	Y give to X\r\n" + 
				"X release Y	X produce Y\r\n" + 
				"X treat by Y	X require Y\r\n" + 
				"X distinguish from Y	Y distinguish from X\r\n" + 
				"X suggest Y	Y involve X\r\n" + 
				"X cause Y	X result from Y\r\n" + 
				"X use in Y	Y require X\r\n" + 
				"X encounter Y	X unite with Y\r\n" + 
				"X obtain from Y	Y contain X\r\n" + 
				"X give Y	Y afford by X\r\n" + 
				"X give Y	X afford Y\r\n" + 
				"X recommend for Y	X give Y\r\n" + 
				"X control with Y	Y need for X\r\n" + 
				"X present as Y	X lead to Y\r\n" + 
				"X begin with Y	Y cause by X\r\n" + 
				"X follow by Y	Y occur after X\r\n" + 
				"X come with Y	Y associate with X\r\n" + 
				"X indicate for Y	Y control by X\r\n" + 
				"X cure by Y	Y eliminate X\r\n" + 
				"X associate with Y	X accompany Y\r\n" + 
				"X promote Y	X produce Y\r\n" + 
				"X accompany by Y	X follow by Y\r\n" + 
				"X characterize by Y	Y relate to X\r\n" + 
				"X bring Y	X produce Y\r\n" + 
				"X have Y	X contain Y\r\n" + 
				"X offer Y	X give Y\r\n" + 
				"X need for Y	Y require X\r\n" + 
				"X respond to Y	Y give for X\r\n" + 
				"X develop into Y	X turn to Y\r\n" + 
				"X follow Y	X accompany by Y\r\n" + 
				"X follow Y	Y terminate in X\r\n" + 
				"X affect Y	X transmit to Y\r\n" + 
				"X associate with Y	Y occur after X\r\n" + 
				"X associate with Y	X contribute to Y\r\n" + 
				"X treat by Y	X cure by Y\r\n" + 
				"X cause by Y	Y develop X\r\n" + 
				"X begin with Y	X consist of Y\r\n" + 
				"X require Y	Y use in X\r\n" + 
				"X give Y	X provide Y\r\n" + 
				"X be in Y	X attribute to Y\r\n" + 
				"X help Y	X be in Y\r\n" + 
				"X reduce Y	X be in Y\r\n" + 
				"X prevent Y	Y affect by X\r\n" + 
				"X administer for Y	X use in Y\r\n" + 
				"X characterize by Y	X result in Y\r\n" + 
				"X use for Y	X administer to Y\r\n" + 
				"X occur with Y	Y indicate X\r\n" + 
				"X afford by Y	Y use for X\r\n" + 
				"X manifest by Y	Y suggest X\r\n" + 
				"X occur in Y	Y affect with X\r\n" + 
				"X manage with Y	Y be in X\r\n" + 
				"X follow Y	Y accompany by X\r\n" + 
				"X respond to Y	Y need for X\r\n" + 
				"X suggest Y	X follow Y\r\n" + 
				"X observe in Y	X be in Y\r\n" + 
				"X correct Y	Y correct by X\r\n" + 
				"X order for Y	X administer for Y\r\n" + 
				"X occur in Y	Y show X\r\n" + 
				"X relieve Y	X give for Y\r\n" + 
				"X use against Y	X eradicate Y\r\n" + 
				"X mistake for Y	X distinguish from Y\r\n" + 
				"X fertilize by Y	Y enter X\r\n" + 
				"X affect by Y	Y eliminate X\r\n" + 
				"X obtain with Y	Y be for X\r\n" + 
				"X associate with Y	Y develop with X\r\n" + 
				"X result in Y	Y follow X\r\n" + 
				"X result from Y	X develop from Y\r\n" + 
				"X accompany Y	Y produce X\r\n" + 
				"X see in Y	X be in Y\r\n" + 
				"X transmit Y	Y transmit through X\r\n" + 
				"X develop into Y	X turn into Y\r\n" + 
				"X give to Y	Y treat with X\r\n" + 
				"X control by Y	X relieve with Y\r\n" + 
				"X provide by Y	Y be for X\r\n" + 
				"X follow Y	Y present as X\r\n" + 
				"X indicate for Y	X license for Y\r\n" + 
				"X require for Y	X prescribe for Y\r\n" + 
				"X relieve by Y	X manage with Y\r\n" + 
				"X hydrolyze Y	X convert Y\r\n" + 
				"X contain Y	X receive Y\r\n" + 
				"X respond to Y	Y decrease X\r\n" + 
				"X combine with Y	X join with Y\r\n" + 
				"X require Y	X treat with Y\r\n" + 
				"X precede by Y	X produce Y\r\n" + 
				"X accompany by Y	X associate with Y\r\n" + 
				"X associate with Y	Y complicate with X\r\n" + 
				"X contain Y	X make from Y\r\n" + 
				"X begin with Y	X be with Y\r\n" + 
				"X control with Y	Y wipe X\r\n" + 
				"X present with Y	Y be in X\r\n" + 
				"X lead to Y	X suggest Y\r\n" + 
				"X mean Y	X involve Y\r\n" + 
				"X lead to Y	X cause Y\r\n" + 
				"X form from Y	X derive from Y\r\n" + 
				"X reduce Y	X prescribe for Y\r\n" + 
				"X begin as Y	X associate with Y\r\n" + 
				"X accompany Y	Y associate with X\r\n" + 
				"X indicate Y	X suggest Y\r\n" + 
				"X afford Y	X give Y\r\n" + 
				"X order for Y	X require for Y\r\n" + 
				"X indicate Y	Y present with X\r\n" + 
				"X occur with Y	Y cause by X\r\n" + 
				"X spread by Y	X transmit by Y\r\n" + 
				"X be for Y	Y achieve with X\r\n" + 
				"X be in Y	Y receive X\r\n" + 
				"X produce by Y	Y produce X\r\n" + 
				"X manufacture Y	X release Y\r\n" + 
				"X characterize by Y	Y find in X\r\n" + 
				"X give for Y	Y respond to X\r\n" + 
				"X produce Y	Y induce by X\r\n" + 
				"X alleviate Y	X help Y\r\n" + 
				"X present with Y	X characterize by Y\r\n" + 
				"X prescribe for Y	Y treat with X\r\n" + 
				"X treat by Y	X prevent by Y\r\n" + 
				"X develop Y	X contract Y\r\n" + 
				"X result in Y	X manifest by Y\r\n" + 
				"X inject into Y	X penetrate Y\r\n" + 
				"X give Y	X lead to Y\r\n" + 
				"X take Y	Y give to X\r\n" + 
				"X respond to Y	Y kill X\r\n" + 
				"X take for Y	X use for Y\r\n" + 
				"X cause Y	Y follow X\r\n" + 
				"X be against Y	Y treat with X\r\n" + 
				"X prescribe for Y	Y control with X\r\n" + 
				"X occur after Y	Y cause X\r\n" + 
				"X relieve by Y	X control by Y\r\n" + 
				"X spread Y	X carry Y\r\n" + 
				"X manage with Y	Y need for X\r\n" + 
				"X contribute to Y	X associate with Y\r\n" + 
				"X associate with Y	Y present with X\r\n" + 
				"X cause Y	Y produce by X\r\n" + 
				"X meet Y	X combine with Y\r\n" + 
				"X induce by Y	Y lead to X\r\n" + 
				"X recommend for Y	X be in Y\r\n" + 
				"X be against Y	Y kill by X\r\n" + 
				"X require Y	Y use for X\r\n" + 
				"X mean Y	X imply Y\r\n" + 
				"X produce Y	Y derive from X\r\n" + 
				"X correct by Y	Y relieve X\r\n" + 
				"X accompany Y	Y cause X\r\n" + 
				"X relieve with Y	Y alleviate X\r\n" + 
				"X control Y	X relieve Y\r\n" + 
				"X administer during Y	X use during Y\r\n" + 
				"X treat by Y	X treat with Y\r\n" + 
				"X develop from Y	Y cause X\r\n" + 
				"X respond to Y	Y treat X\r\n" + 
				"X confound with Y	Y confuse with X\r\n" + 
				"X occur in Y	X be in Y\r\n" + 
				"X mask Y	Y respond to X\r\n" + 
				"X be for Y	X take for Y\r\n" + 
				"X indicate Y	X occur in Y\r\n" + 
				"X attribute to Y	Y produce X\r\n" + 
				"X manage with Y	X relieve with Y\r\n" + 
				"X develop in Y	Y affect with X\r\n" + 
				"X be in Y	Y use X\r\n" + 
				"X cause Y	Y occur during X\r\n" + 
				"X treat by Y	Y require for X\r\n" + 
				"X treat with Y	Y relieve X\r\n" + 
				"X provide from Y	X prescribe for Y\r\n" + 
				"X result from Y	Y produce X\r\n" + 
				"X use Y	Y be in X\r\n" + 
				"X require Y	Y require for X\r\n" + 
				"X accompany with Y	Y accompany X\r\n" + 
				"X follow by Y	Y complicate X\r\n" + 
				"X cause Y	X imply Y\r\n" + 
				"X produce Y	Y produce in X\r\n" + 
				"X associate with Y	Y accompany X\r\n" + 
				"X use in Y	X develop for Y\r\n" + 
				"X isolate from Y	X find in Y\r\n" + 
				"X recommend for Y	X require for Y\r\n" + 
				"X cause Y	X aggravate Y\r\n" + 
				"X complicate by Y	X lead to Y\r\n" + 
				"X be with Y	Y characterize by X\r\n" + 
				"X prescribe for Y	X require for Y\r\n" + 
				"X characterize by Y	X begin with Y\r\n" + 
				"X associate with Y	X progress to Y\r\n" + 
				"X characterize by Y	X give to Y\r\n" + 
				"X require Y	Y treat X\r\n" + 
				"X accompany by Y	Y accompany X\r\n" + 
				"X indicate for Y	X develop for Y\r\n" + 
				"X turn into Y	X lead to Y\r\n" + 
				"X contain Y	X combine with Y\r\n" + 
				"X follow Y	Y complicate by X\r\n" + 
				"X involve Y	X characterize by Y\r\n" + 
				"X correct by Y	Y use for X\r\n" + 
				"X transmit through Y	X transmit by Y\r\n" + 
				"X occur with Y	X occur in Y\r\n" + 
				"X find in Y	Y carry X\r\n" + 
				"X present with Y	Y occur with X\r\n" + 
				"X decrease Y	Y respond to X\r\n" + 
				"X derive from Y	Y convert into X\r\n" + 
				"X accompany by Y	X develop with Y\r\n" + 
				"X be in Y	Y suggest X\r\n" + 
				"X convert Y	X digest Y\r\n" + 
				"X produce Y	Y suggest X\r\n" + 
				"X confuse with Y	X mistake for Y\r\n" + 
				"X achieve with Y	Y be for X\r\n" + 
				"X require Y	X control with Y\r\n" + 
				"X use as Y	X use for Y\r\n" + 
				"X produce by Y	Y secrete X\r\n" + 
				"X indicate for Y	X control Y\r\n" + 
				"X offer Y	X use for Y\r\n" + 
				"X contain Y	Y use as X\r\n" + 
				"X affect by Y	Y kill X\r\n" + 
				"X prevent by Y	X cure by Y\r\n" + 
				"X control with Y	Y decrease X\r\n" + 
				"X convert into Y	X convert to Y\r\n" + 
				"X give Y	X prescribe for Y\r\n" + 
				"X cause Y	X give to Y\r\n" + 
				"X result from Y	X attribute to Y\r\n" + 
				"X produce Y	X suggest Y\r\n" + 
				"X attribute to Y	Y cause X\r\n" + 
				"X develop after Y	X follow Y\r\n" + 
				"X transmit by Y	X transmit through Y\r\n" + 
				"X prescribe for Y	X help Y\r\n" + 
				"X confound with Y	X differ from Y\r\n" + 
				"X cure Y	Y control with X\r\n" + 
				"X order for Y	X indicate for Y\r\n" + 
				"X eliminate Y	Y control with X\r\n" + 
				"X cover Y	X be against Y\r\n" + 
				"X cause by Y	X relate to Y\r\n" + 
				"X produce Y	X make Y\r\n" + 
				"X reduce Y	X prevent Y\r\n" + 
				"X suggest Y	Y begin with X\r\n" + 
				"X associate with Y	X develop into Y\r\n" + 
				"X mistake for Y	X confuse with Y\r\n" + 
				"X accompany by Y	X present with Y\r\n" + 
				"X result in Y	Y occur from X\r\n" + 
				"X control Y	Y treat with X\r\n" + 
				"X manifest as Y	X associate with Y\r\n" + 
				"X cause Y	X induce Y\r\n" + 
				"X suggest Y	X cause by Y\r\n" + 
				"X respond with Y	X treat with Y\r\n" + 
				"X license for Y	X recommend for Y\r\n" + 
				"X indicate Y	X cause by Y\r\n" + 
				"X reserve for Y	Y require X\r\n" + 
				"X lead to Y	X precipitate Y\r\n" + 
				"X give to Y	Y give X\r\n" + 
				"X eliminate Y	X eradicate Y\r\n" + 
				"X present as Y	Y indicate X\r\n" + 
				"X suggest Y	Y occur with X\r\n" + 
				"X isolate from Y	X obtain from Y\r\n" + 
				"X result in Y	Y cause X\r\n" + 
				"X complicate with Y	X associate with Y\r\n" + 
				"X contain Y	X prepare from Y\r\n" + 
				"X produce Y	X aggravate Y\r\n" + 
				"X require for Y	Y manage with X\r\n" + 
				"X alleviate Y	Y manage with X\r\n" + 
				"X complicate Y	X accompany Y\r\n" + 
				"X die of Y	X expose to Y\r\n" + 
				"X treat with Y	Y decrease X\r\n" + 
				"X relieve by Y	Y control X\r\n" + 
				"X cure by Y	X treat with Y\r\n" + 
				"X suggest Y	X lead to Y\r\n" + 
				"X eradicate Y	Y control with X\r\n" + 
				"X confuse with Y	X differ from Y\r\n" + 
				"X combine with Y	X unite with Y\r\n" + 
				"X contribute to Y	X result in Y\r\n" + 
				"X indicate Y	Y manifest by X\r\n" + 
				"X be in Y	Y cure by X\r\n" + 
				"X control Y	X order for Y\r\n" + 
				"X involve Y	X confine to Y\r\n" + 
				"X kill Y	X eliminate Y\r\n" + 
				"X cause Y	X accompany by Y\r\n" + 
				"X meet Y	Y encounter X\r\n" + 
				"X meet Y	X encounter Y\r\n" + 
				"X occur with Y	X complicate Y\r\n" + 
				"X result from Y	X occur after Y\r\n" + 
				"X follow by Y	Y succeed to X\r\n" + 
				"X accompany Y	Y follow by X\r\n" + 
				"X make Y	X release Y\r\n" + 
				"X complicate Y	Y lead to X\r\n" + 
				"X result in Y	Y accompany X\r\n" + 
				"X take for Y	Y relieve by X\r\n" + 
				"X reserve for Y	Y treat with X\r\n" + 
				"X follow Y	X complicate by Y\r\n" + 
				"X transmit to Y	X affect Y\r\n" + 
				"X result in Y	X give to Y\r\n" + 
				"X present as Y	Y associate with X\r\n" + 
				"X relate to Y	Y lead to X\r\n" + 
				"X affect with Y	Y occur in X\r\n" + 
				"X associate with Y	X accompany by Y\r\n" + 
				"X administer to Y	X be in Y\r\n" + 
				"X use for Y	X use during Y\r\n" + 
				"X include Y	X contain Y\r\n" + 
				"X be for Y	X use in Y\r\n" + 
				"X see in Y	Y cause X\r\n" + 
				"X treat with Y	X give Y\r\n" + 
				"X associate with Y	X occur with Y\r\n" + 
				"X compose of Y	X base on Y\r\n" + 
				"X eliminate Y	Y prevent by X\r\n" + 
				"X prescribe for Y	X be for Y\r\n" + 
				"X license for Y	X indicate for Y\r\n" + 
				"X use in Y	X use as Y\r\n" + 
				"X cause by Y	Y characterize by X\r\n" + 
				"X cause by Y	X indicate Y\r\n" + 
				"X lead to Y	X follow by Y\r\n" + 
				"X be in Y	Y consist of X\r\n" + 
				"X indicate Y	X associate with Y\r\n" + 
				"X achieve with Y	Y require for X\r\n" + 
				"X infect with Y	X carry Y\r\n" + 
				"X invent Y	X find Y\r\n" + 
				"X correct with Y	X treat with Y\r\n" + 
				"X associate with Y	Y cause by X\r\n" + 
				"X control Y	X treat Y\r\n" + 
				"X require for Y	Y achieve with X\r\n" + 
				"X imply Y	X cause Y\r\n" + 
				"X alternate with Y	Y follow by X\r\n" + 
				"X see in Y	Y present with X\r\n" + 
				"X accompany with Y	X associate with Y\r\n" + 
				"X produce Y	Y see in X\r\n" + 
				"X be in Y	Y transmit X\r\n" + 
				"X manifest by Y	X present with Y\r\n" + 
				"X alleviate Y	X use for Y\r\n" + 
				"X control by Y	Y provide from X\r\n" + 
				"X correct Y	Y treat with X\r\n" + 
				"X give to Y	Y follow X\r\n" + 
				"X associate with Y	Y occur with X\r\n" + 
				"X mean Y	X refer to Y\r\n" + 
				"X develop from Y	Y lead to X\r\n" + 
				"X include Y	X characterize by Y\r\n" + 
				"X precipitate Y	X cause Y\r\n" + 
				"X characterize by Y	X involve Y\r\n" + 
				"X relieve with Y	Y prescribe for X\r\n" + 
				"X suffer from Y	X have Y\r\n" + 
				"X derive from Y	X form from Y\r\n" + 
				"X confine to Y	X involve Y\r\n" + 
				"X administer during Y	X use in Y\r\n" + 
				"X prescribe for Y	X afford Y\r\n" + 
				"X relieve with Y	X control with Y\r\n" + 
				"X control Y	X provide from Y\r\n" + 
				"X indicate for Y	X reduce Y\r\n" + 
				"X relieve with Y	X respond to Y\r\n" + 
				"X give Y	Y provide by X\r\n" + 
				"X join with Y	X meet Y\r\n" + 
				"X provide Y	X indicate for Y\r\n" + 
				"X cause by Y	Y contribute to X\r\n" + 
				"X associate with Y	X present as Y\r\n" + 
				"X discharge Y	X produce Y\r\n" + 
				"X be for Y	X help Y\r\n" + 
				"X respond to Y	Y be for X\r\n" + 
				"X use Y	Y recommend for X\r\n" + 
				"X die of Y	X get Y\r\n" + 
				"X contain Y	X carry Y\r\n" + 
				"X affect Y	X occur in Y\r\n" + 
				"X lead to Y	Y complicate X\r\n" + 
				"X lead to Y	Y occur from X\r\n" + 
				"X unite with Y	X meet Y\r\n" + 
				"X control with Y	Y be in X\r\n" + 
				"X result from Y	Y present as X\r\n" + 
				"X be against Y	X destroy Y\r\n" + 
				"X characterize by Y	X appear as Y\r\n" + 
				"X achieve with Y	Y be in X\r\n" + 
				"X provide from Y	X reduce Y\r\n" + 
				"X transmit by Y	X be in Y\r\n" + 
				"X compose of Y	X consist of Y\r\n" + 
				"X present as Y	X characterize by Y\r\n" + 
				"X manage with Y	X respond to Y\r\n" + 
				"X give to Y	X cause Y\r\n" + 
				"X cause Y	X accompany with Y\r\n" + 
				"X employ as Y	X use as Y\r\n" + 
				"X correct by Y	X require Y\r\n" + 
				"X follow Y	X be in Y\r\n" + 
				"X prescribe for Y	X use for Y\r\n" + 
				"X involve Y	Y suggest X\r\n" + 
				"X occur from Y	X follow Y\r\n" + 
				"X give to Y	Y result from X\r\n" + 
				"X be with Y	Y be for X\r\n" + 
				"X protect from Y	X prevent Y\r\n" + 
				"X use as Y	Y compose of X\r\n" + 
				"X give for Y	X order for Y\r\n" + 
				"X relieve Y	X order for Y\r\n" + 
				"X affect Y	Y have X\r\n" + 
				"X occur with Y	X present with Y\r\n" + 
				"X alleviate Y	X control Y\r\n" + 
				"X treat Y	X prevent Y\r\n" + 
				"X eradicate Y	Y affect by X\r\n" + 
				"X give for Y	Y manage with X\r\n" + 
				"X cause by Y	X occur from Y\r\n" + 
				"X be for Y	Y obtain with X\r\n" + 
				"X result in Y	Y occur after X\r\n" + 
				"X convert Y	Y convert by X\r\n" + 
				"X indicate Y	Y begin with X\r\n" + 
				"X result in Y	Y relate to X\r\n" + 
				"X enter Y	X penetrate Y\r\n" + 
				"X suggest Y	X relate to Y\r\n" + 
				"X relieve Y	X prevent Y\r\n" + 
				"X help Y	Y control with X\r\n" + 
				"X use for Y	Y be with X\r\n" + 
				"X isolate from Y	Y produce X\r\n" + 
				"X cure Y	X prescribe for Y\r\n" + 
				"X prescribe for Y	X relieve Y\r\n" + 
				"X cause Y	X precede by Y\r\n" + 
				"X treat by Y	X control with Y\r\n" + 
				"X mean Y	X accompany by Y\r\n" + 
				"X relieve by Y	Y correct X\r\n" + 
				"X occur with Y	Y suggest X\r\n" + 
				"X be in Y	X observe in Y\r\n" + 
				"X give for Y	X use for Y\r\n" + 
				"X prescribe for Y	Y treat by X\r\n" + 
				"X be against Y	Y control with X\r\n" + 
				"X take Y	X treat with Y\r\n" + 
				"X cause by Y	Y suggest X\r\n" + 
				"X associate with Y	X manifest as Y\r\n" + 
				"X induce by Y	Y cause X\r\n" + 
				"X mean Y	X cause by Y\r\n" + 
				"X release Y	X manufacture Y\r\n" + 
				"X produce Y	Y develop from X\r\n" + 
				"X respond to Y	Y prevent X\r\n" + 
				"X order for Y	X control Y\r\n" + 
				"X contribute to Y	X characterize by Y\r\n" + 
				"X enter Y	Y fertilize by X\r\n" + 
				"X unite with Y	Y unite with X\r\n" + 
				"X complicate by Y	X follow Y\r\n" + 
				"X encounter Y	Y unite with X\r\n" + 
				"X prepare from Y	Y use as X\r\n" + 
				"X relate to Y	Y contribute to X\r\n" + 
				"X secrete into Y	X release into Y\r\n" + 
				"X produce Y	Y attribute to X\r\n" + 
				"X find in Y	X transmit by Y\r\n" + 
				"X digest Y	X hydrolyze Y\r\n" + 
				"X mean Y	X define as Y\r\n" + 
				"X transmit by Y	X carry by Y\r\n" + 
				"X prescribe for Y	X use in Y\r\n" + 
				"X result from Y	Y manifest as X\r\n" + 
				"X control Y	X need for Y\r\n" + 
				"X cover Y	X kill Y\r\n" + 
				"X relieve by Y	Y help X\r\n" + 
				"X develop into Y	Y cause by X\r\n" + 
				"X attack with Y	X suffer from Y\r\n" + 
				"X characterize by Y	X consist of Y\r\n" + 
				"X cause Y	X begin as Y\r\n" + 
				"X relieve by Y	Y give for X\r\n" + 
				"X affect by Y	Y prevent X\r\n" + 
				"X affect Y	Y affect by X\r\n" + 
				"X provide from Y	X use for Y\r\n" + 
				"X be in Y	X enter Y\r\n" + 
				"X use with Y	X combine with Y\r\n" + 
				"X complicate with Y	Y associate with X\r\n" + 
				"X relieve with Y	Y be for X\r\n" + 
				"X mean Y	Y accompany X\r\n" + 
				"X treat Y	Y require X\r\n" + 
				"X relieve by Y	Y improve X\r\n" + 
				"X manufacture Y	X produce Y\r\n" + 
				"X produce in Y	Y make X\r\n" + 
				"X eliminate Y	Y respond to X\r\n" + 
				"X metabolize by Y	X convert by Y\r\n" + 
				"X attack Y	X affect Y\r\n" + 
				"X suggest Y	Y cause X\r\n" + 
				"X develop with Y	X associate with Y\r\n" + 
				"X be in Y	X follow Y\r\n" + 
				"X affect in Y	Y affect X\r\n" + 
				"X join with Y	X combine with Y\r\n" + 
				"X define as Y	X involve Y\r\n" + 
				"X involve Y	Y affect in X\r\n" + 
				"X follow by Y	Y give to X\r\n" + 
				"X use in Y	X be against Y\r\n" + 
				"X develop with Y	Y associate with X\r\n" + 
				"X provide Y	X afford Y\r\n" + 
				"X have Y	X characterize by Y\r\n" + 
				"X recommend for Y	X license for Y\r\n" + 
				"X need for Y	X reduce Y\r\n" + 
				"X achieve with Y	Y afford X\r\n" + 
				"X digest Y	X convert Y\r\n" + 
				"X occur in Y	X occur during Y\r\n" + 
				"X cause Y	Y result in X\r\n" + 
				"X relieve Y	X alleviate Y\r\n" + 
				"X treat with Y	Y help X\r\n" + 
				"X produce by Y	X derive from Y\r\n" + 
				"X characterize by Y	Y see in X\r\n" + 
				"X follow Y	Y follow by X\r\n" + 
				"X produce Y	Y achieve with X\r\n" + 
				"X infect Y	Y infect with X\r\n" + 
				"X manifest as Y	Y cause by X\r\n" + 
				"X result from Y	X cause Y\r\n" + 
				"X follow Y	Y result in X\r\n" + 
				"X respond to Y	X affect by Y\r\n" + 
				"X administer for Y	X be in Y\r\n" + 
				"X afford Y	X indicate for Y\r\n" + 
				"X help Y	X give for Y\r\n" + 
				"X include Y	Y combine with X\r\n" + 
				"X indicate for Y	X order for Y\r\n" + 
				"X correct Y	Y correct with X\r\n" + 
				"X transmit through Y	X spread by Y\r\n" + 
				"X confound with Y	Y mistake for X\r\n" + 
				"X use against Y	X kill Y\r\n" + 
				"X cause by Y	X suggest Y\r\n" + 
				"X occur with Y	X produce Y\r\n" + 
				"X control with Y	Y relieve X\r\n" + 
				"X relate to Y	Y associate with X\r\n" + 
				"X respond to Y	X cure by Y\r\n" + 
				"X aggravate Y	Y cause by X\r\n" + 
				"X use for Y	X have Y\r\n" + 
				"X be against Y	X use against Y\r\n" + 
				"X produce by Y	X result from Y\r\n" + 
				"X characterize by Y	Y precede X\r\n" + 
				"X occur in Y	Y give X\r\n" + 
				"X ejaculate Y	X produce Y\r\n" + 
				"X confound with Y	Y confound with X\r\n" + 
				"X give for Y	X alleviate Y\r\n" + 
				"X lead to Y	Y occur after X\r\n" + 
				"X bring Y	X provide Y\r\n" + 
				"X cause by Y	Y produce X\r\n" + 
				"X produce Y	X offer Y\r\n" + 
				"X use for Y	Y require X\r\n" + 
				"X follow by Y	Y follow by X\r\n" + 
				"X occur from Y	X result from Y\r\n" + 
				"X find in Y	X enter Y\r\n" + 
				"X prevent by Y	Y lead to X\r\n" + 
				"X be in Y	Y develop X\r\n" + 
				"X obtain from Y	Y produce X\r\n" + 
				"X accompany by Y	X accompany Y\r\n" + 
				"X infect with Y	Y carry by X\r\n" + 
				"X relieve with Y	Y use for X\r\n" + 
				"X induce Y	Y cause by X\r\n" + 
				"X synthesize from Y	X derive from Y\r\n" + 
				"X follow Y	X be after Y\r\n" + 
				"X control with Y	Y alleviate X\r\n" + 
				"X reduce Y	X be for Y\r\n" + 
				"X relieve Y	Y cure by X\r\n" + 
				"X give before Y	X administer before Y\r\n" + 
				"X prescribe for Y	Y manage with X\r\n" + 
				"X manifest as Y	X produce Y\r\n" + 
				"X carry Y	X have Y\r\n" + 
				"X correct by Y	X relieve by Y\r\n" + 
				"X produce Y	Y be in X\r\n" + 
				"X infect with Y	Y be in X\r\n" + 
				"X hydrolyze Y	Y hydrolyze by X\r\n" + 
				"X die from Y	X get Y\r\n" + 
				"X require Y	Y reserve for X\r\n" + 
				"X produce Y	X promote Y\r\n" + 
				"X use during Y	X administer during Y\r\n" + 
				"X associate with Y	Y present as X\r\n" + 
				"X associate with Y	X increase Y\r\n" + 
				"X produce Y	X present with Y\r\n" + 
				"X alleviate Y	X be in Y\r\n" + 
				"X require Y	Y correct X\r\n" + 
				"X occur from Y	Y cause X\r\n" + 
				"X complicate Y	Y follow X\r\n" + 
				"X bring Y	X use for Y\r\n" + 
				"X manage with Y	Y help X\r\n" + 
				"X cause by Y	X be in Y\r\n" + 
				"X transmit Y	X develop Y\r\n" + 
				"X be against Y	X cover Y\r\n" + 
				"X transmit through Y	X transmit via Y\r\n" + 
				"X produce Y	Y follow X\r\n" + 
				"X control with Y	Y use against X\r\n" + 
				"X occur with Y	X cause by Y\r\n" + 
				"X kill by Y	X affect by Y\r\n" + 
				"X use during Y	X use for Y\r\n" + 
				"X enter Y	X get into Y\r\n" + 
				"X associate with Y	Y follow X\r\n" + 
				"X be in Y	X be during Y\r\n" + 
				"X prescribe for Y	Y control by X\r\n" + 
				"X cause Y	Y cause by X\r\n" + 
				"X follow Y	Y cause X\r\n" + 
				"X treat with Y	X prevent by Y\r\n" + 
				"X prevent Y	X eliminate Y\r\n" + 
				"X control Y	Y control with X\r\n" + 
				"X accompany with Y	X accompany by Y\r\n" + 
				"X associate Y	X cause Y\r\n" + 
				"X develop Y	X associate with Y\r\n" + 
				"X combine with Y	Y encounter X\r\n" + 
				"X carry by Y	X transmit by Y\r\n" + 
				"X secrete by Y	Y produce X\r\n" + 
				"X attribute to Y	X result from Y\r\n" + 
				"X affect by Y	Y attack X\r\n" + 
				"X inject into Y	Y penetrate by X\r\n" + 
				"X present as Y	X result in Y\r\n" + 
				"X accompany by Y	Y produce X\r\n" + 
				"X present with Y	X associate with Y\r\n" + 
				"X be against Y	Y affect by X\r\n" + 
				"X associate with Y	Y imply X\r\n" + 
				"X relieve with Y	Y reduce X\r\n" + 
				"X mask Y	Y treat with X\r\n" + 
				"X produce Y	Y obtain from X\r\n" + 
				"X alternate with Y	Y alternate with X\r\n" + 
				"X imply Y	X define as Y\r\n" + 
				"X need for Y	X require for Y\r\n" + 
				"X secrete Y	Y secrete by X\r\n" + 
				"X make Y	X produce Y\r\n" + 
				"X use in Y	X be for Y\r\n" + 
				"X penetrate Y	Y penetrate by X\r\n" + 
				"X receive Y	X have Y\r\n" + 
				"X accompany Y	Y accompany with X\r\n" + 
				"X order for Y	X give for Y\r\n" + 
				"X mistake for Y	X confound with Y\r\n" + 
				"X mask Y	X alleviate Y\r\n" + 
				"X consist of Y	X have Y\r\n" + 
				"X kill Y	X cover Y\r\n" + 
				"X treat with Y	Y correct X\r\n" + 
				"X manufacture Y	X make Y\r\n" + 
				"X prevent Y	Y prevent by X\r\n" + 
				"X develop with Y	X accompany by Y\r\n" + 
				"X treat with Y	X relieve by Y\r\n" + 
				"X relieve with Y	Y relieve X\r\n" + 
				"X relieve Y	X improve Y\r\n" + 
				"X see in Y	X indicate Y\r\n" + 
				"X have Y	X consist of Y\r\n" + 
				"X carry Y	Y carry by X\r\n" + 
				"X correct with Y	Y use for X\r\n" + 
				"X relate to Y	X result from Y\r\n" + 
				"X see in Y	X occur in Y\r\n" + 
				"X administer to Y	X use for Y\r\n" + 
				"X take for Y	Y relieve with X\r\n" + 
				"X follow Y	Y follow X\r\n" + 
				"X predispose to Y	X cause Y\r\n" + 
				"X follow by Y	Y accompany by X\r\n" + 
				"X carry Y	Y transmit by X\r\n" + 
				"X contribute to Y	X cause Y\r\n" + 
				"X accompany Y	Y result in X\r\n" + 
				"X produce Y	Y make by X\r\n" + 
				"X progress to Y	Y cause by X\r\n" + 
				"X develop from Y	X cause by Y\r\n" + 
				"X be in Y	Y have X\r\n" + 
				"X complicate by Y	Y cause by X\r\n" + 
				"X use in Y	X reduce Y\r\n" + 
				"X occur in Y	X obtain from Y\r\n" + 
				"X accompany Y	X occur in Y\r\n" + 
				"X associate with Y	X begin as Y\r\n" + 
				"X require for Y	Y require X\r\n" + 
				"X afford Y	X recommend for Y\r\n" + 
				"X recommend for Y	X be for Y\r\n" + 
				"X lead to Y	Y indicate X\r\n" + 
				"X complicate by Y	Y result from X\r\n" + 
				"X occur in Y	X cause by Y\r\n" + 
				"X cause Y	Y accompany X\r\n" + 
				"X treat by Y	Y mask X\r\n" + 
				"X use Y	Y use in X\r\n" + 
				"X complicate by Y	Y follow X\r\n" + 
				"X provide from Y	X relieve Y\r\n" + 
				"X help Y	X be for Y\r\n" + 
				"X be in Y	X occur during Y\r\n" + 
				"X suggest Y	Y result from X\r\n" + 
				"X kill Y	X destroy Y\r\n" + 
				"X characterize by Y	Y occur during X\r\n" + 
				"X reduce Y	X mask Y\r\n" + 
				"X relieve Y	Y respond to X\r\n" + 
				"X manifest by Y	Y result from X\r\n" + 
				"X recommend for Y	X develop for Y\r\n" + 
				"X make from Y	X make of Y\r\n" + 
				"X suggest Y	X be in Y\r\n" + 
				"X develop Y	X cause Y\r\n" + 
				"X attend with Y	X associate with Y\r\n" + 
				"X require Y	Y administer for X\r\n" + 
				"X develop for Y	X be for Y\r\n" + 
				"X lead to Y	X give Y\r\n" + 
				"X manage with Y	Y give for X\r\n" + 
				"X accompany Y	Y occur with X\r\n" + 
				"X present with Y	X result in Y\r\n" + 
				"X confine to Y	X affect Y\r\n" + 
				"X use as Y	X use in Y\r\n" + 
				"X follow Y	Y occur with X\r\n" + 
				"X occur with Y	Y accompany X\r\n" + 
				"X cure with Y	X treat by Y\r\n" + 
				"X trigger Y	X cause Y\r\n" + 
				"X have Y	X transmit Y\r\n" + 
				"X accompany by Y	Y see in X\r\n" + 
				"X afford Y	X use for Y\r\n" + 
				"X indicate for Y	Y control with X\r\n" + 
				"X associate with Y	X develop with Y\r\n" + 
				"X be in Y	Y carry X\r\n" + 
				"X precede Y	X follow by Y\r\n" + 
				"X indicate for Y	X give Y\r\n" + 
				"X produce Y	Y result from X\r\n" + 
				"X be for Y	X kill Y\r\n" + 
				"X obtain from Y	X find in Y\r\n" + 
				"X contain Y	X include Y\r\n" + 
				"X present as Y	X produce Y\r\n" + 
				"X prescribe for Y	X need for Y\r\n" + 
				"X produce Y	X accompany Y\r\n" + 
				"X treat with Y	Y reserve for X\r\n" + 
				"X relieve by Y	X respond to Y\r\n" + 
				"X give Y	X offer Y\r\n" + 
				"X confound with Y	Y distinguish from X\r\n" + 
				"X cause Y	X result in Y\r\n" + 
				"X treat with Y	Y cure X\r\n" + 
				"X cause Y	Y result from X\r\n" + 
				"X complicate Y	Y precede X\r\n" + 
				"X mean Y	X associate with Y\r\n" + 
				"X manage with Y	Y require for X\r\n" + 
				"X be after Y	Y follow by X\r\n" + 
				"X be for Y	X bring Y\r\n" + 
				"X occur during Y	Y cause X\r\n" + 
				"X associate with Y	Y relate to X\r\n" + 
				"X need for Y	X help Y\r\n" + 
				"X treat by Y	Y treat X\r\n" + 
				"X be in Y	X transmit by Y\r\n" + 
				"X correct Y	Y treat by X\r\n" + 
				"X result from Y	Y result in X\r\n" + 
				"X occur in Y	X indicate Y\r\n" + 
				"X treat by Y	X manage with Y\r\n" + 
				"X use for Y	X afford Y\r\n" + 
				"X convert to Y	X convert into Y\r\n" + 
				"X join with Y	Y meet X\r\n" + 
				"X use against Y	X prevent Y\r\n" + 
				"X hydrolyze Y	X break Y\r\n" + 
				"X control Y	X mask Y\r\n" + 
				"X reduce Y	X protect from Y\r\n" + 
				"X alleviate Y	X reduce Y\r\n" + 
				"X help Y	Y control by X\r\n" + 
				"X relieve by Y	Y decrease X\r\n" + 
				"X relate to Y	X attribute to Y\r\n" + 
				"X produce by Y	X cause by Y\r\n" + 
				"X relieve by Y	Y be for X\r\n" + 
				"X result in Y	Y suggest X\r\n" + 
				"X combine with Y	X use as Y\r\n" + 
				"X use for Y	Y relieve with X\r\n" + 
				"X find in Y	Y present X\r\n" + 
				"X lead to Y	Y follow X\r\n" + 
				"X provide Y	Y achieve with X\r\n" + 
				"X attack Y	X occur in Y\r\n" + 
				"X derive from Y	X make from Y\r\n" + 
				"X find in Y	Y transmit X\r\n" + 
				"X mask Y	X use for Y\r\n" + 
				"X treat by Y	Y be against X\r\n" + 
				"X respond to Y	Y use in X\r\n" + 
				"X inhibit by Y	Y inhibit X\r\n" + 
				"X be in Y	X control Y\r\n" + 
				"X treat with Y	X overcome by Y\r\n" + 
				"X give for Y	X recommend for Y\r\n" + 
				"X occur during Y	X associate with Y\r\n" + 
				"X indicate Y	Y characterize by X\r\n" + 
				"X act as Y	Y include X\r\n" + 
				"X derive from Y	Y produce X\r\n" + 
				"X bring Y	X afford Y\r\n" + 
				"X alternate with Y	X follow by Y\r\n" + 
				"X follow by Y	Y occur with X\r\n" + 
				"X treat with Y	Y cover X\r\n" + 
				"X cause Y	X follow by Y\r\n" + 
				"X refer to Y	X imply Y\r\n" + 
				"X find in Y	X associate with Y\r\n" + 
				"X pass through Y	X enter Y\r\n" + 
				"X use as Y	X combine with Y\r\n" + 
				"X be for Y	Y relieve by X\r\n" + 
				"X control Y	X alleviate Y\r\n" + 
				"X resemble Y	X simulate Y\r\n" + 
				"X indicate Y	X be in Y\r\n" + 
				"X get into Y	X inject into Y\r\n" + 
				"X accompany Y	Y accompany by X\r\n" + 
				"X prescribe for Y	X take for Y\r\n" + 
				"X be in Y	X result from Y\r\n" + 
				"X result from Y	Y manifest by X\r\n" + 
				"X accompany Y	X suggest Y\r\n" + 
				"X include Y	Y find in X\r\n" + 
				"X treat by Y	Y control X\r\n" + 
				"X result from Y	X mean Y\r\n" + 
				"X involve Y	X refer to Y\r\n" + 
				"X have Y	X develop Y\r\n" + 
				"X treat by Y	Y eliminate X\r\n" + 
				"X develop after Y	X cause by Y\r\n" + 
				"X administer with Y	X combine with Y\r\n" + 
				"X occur in Y	Y present as X\r\n" + 
				"X manifest by Y	Y associate with X\r\n" + 
				"X use for Y	X prescribe for Y\r\n" + 
				"X control with Y	X require Y\r\n" + 
				"X ejaculate Y	X have Y\r\n" + 
				"X help Y	X relieve Y\r\n" + 
				"X associate with Y	X mean Y\r\n" + 
				"X control Y	Y relieve by X\r\n" + 
				"X eradicate Y	Y treat with X\r\n" + 
				"X kill by Y	Y destroy X\r\n" + 
				"X respond to Y	Y administer for X\r\n" + 
				"X use in Y	Y contain X\r\n" + 
				"X combine with Y	Y meet X\r\n" + 
				"X associate with Y	Y occur from X\r\n" + 
				"X decrease Y	X help Y\r\n" + 
				"X control with Y	Y reduce X\r\n" + 
				"X lead to Y	Y produce by X\r\n" + 
				"X produce by Y	X associate with Y\r\n" + 
				"X cure by Y	X respond to Y\r\n" + 
				"X kill by Y	Y kill X\r\n" + 
				"X require Y	X correct with Y\r\n" + 
				"X correct Y	Y require X\r\n" + 
				"X convert into Y	X metabolize to Y\r\n" + 
				"X receive Y	X contain Y\r\n" + 
				"X alleviate Y	X mask Y\r\n" + 
				"X imply Y	X refer to Y\r\n" + 
				"X follow by Y	Y follow X\r\n" + 
				"X associate with Y	X attribute to Y\r\n" + 
				"X occur after Y	Y produce X\r\n" + 
				"X lead to Y	Y induce by X\r\n" + 
				"X meet Y	Y join with X\r\n" + 
				"X obtain from Y	Y have X\r\n" + 
				"X distinguish from Y	Y differ from X\r\n" + 
				"X compose of Y	X make from Y\r\n" + 
				"X carry by Y	Y carry X\r\n" + 
				"X confound with Y	X simulate Y\r\n" + 
				"X treat by Y	X correct by Y\r\n" + 
				"X die of Y	X die from Y\r\n" + 
				"X require for Y	Y treat by X\r\n" + 
				"X lead to Y	Y precede by X\r\n" + 
				"X prescribe for Y	X bring Y\r\n" + 
				"X mean Y	X result from Y\r\n" + 
				"X use for Y	Y use X\r\n" + 
				"X penetrate by Y	Y penetrate X\r\n" + 
				"X penetrate by Y	Y enter X\r\n" + 
				"X control by Y	Y indicate for X\r\n" + 
				"X achieve with Y	Y use for X\r\n" + 
				"X produce Y	Y produce by X\r\n" + 
				"X use for Y	X reserve for Y\r\n" + 
				"X treat by Y	X overcome by Y\r\n" + 
				"X relieve Y	X mask Y\r\n" + 
				"X alternate with Y	Y give to X\r\n" + 
				"X relieve Y	X help with Y\r\n" + 
				"X administer for Y	Y respond to X\r\n" + 
				"X administer during Y	X use for Y\r\n" + 
				"X bring Y	X prescribe for Y\r\n" + 
				"X be against Y	X use for Y\r\n" + 
				"X eradicate Y	X kill Y\r\n" + 
				"X cause Y	X attend with Y\r\n" + 
				"X follow Y	X attribute to Y\r\n" + 
				"X offer Y	Y achieve with X\r\n" + 
				"X require Y	Y need for X\r\n" + 
				"X cause by Y	Y develop into X\r\n" + 
				"X cause Y	X develop Y\r\n" + 
				"X treat with Y	Y suppress X\r\n" + 
				"X control with Y	Y take for X\r\n" + 
				"X combine with Y	X encounter Y\r\n" + 
				"X overcome by Y	Y relieve X\r\n" + 
				"X result in Y	Y occur with X\r\n" + 
				"X control by Y	Y prescribe for X\r\n" + 
				"X manage with Y	Y alleviate X\r\n" + 
				"X occur with Y	Y present with X\r\n" + 
				"X start before Y	X give before Y\r\n" + 
				"X obtain with Y	Y provide X\r\n" + 
				"X contract Y	X have Y\r\n" + 
				"X suggest Y	Y result in X\r\n" + 
				"X provide Y	X induce Y\r\n" + 
				"X associate with Y	X find in Y\r\n" + 
				"X attack with Y	X have Y\r\n" + 
				"X define as Y	X imply Y\r\n" + 
				"X give for Y	X administer for Y\r\n" + 
				"X give for Y	Y correct by X\r\n" + 
				"X contribute to Y	X produce Y\r\n" + 
				"X eliminate Y	Y affect by X\r\n" + 
				"X control Y	X prescribe for Y\r\n" + 
				"X prescribe for Y	X provide Y\r\n" + 
				"X produce by Y	Y include X\r\n" + 
				"X characterize by Y	X contribute to Y\r\n" + 
				"X refer to Y	X define as Y\r\n" + 
				"X order for Y	Y relieve by X\r\n" + 
				"X require Y	Y relieve X\r\n" + 
				"X relieve Y	X require for Y\r\n" + 
				"X be of Y	X have Y\r\n" + 
				"X produce by Y	Y give X\r\n" + 
				"X mask Y	X reduce Y\r\n" + 
				"X present with Y	X occur with Y\r\n" + 
				"X get Y	X have Y\r\n" + 
				"X occur in Y	Y develop X\r\n" + 
				"X contribute to Y	Y result from X\r\n" + 
				"X complicate Y	X cause Y\r\n" + 
				"X progress to Y	X lead to Y\r\n" + 
				"X associate with Y	X manifest by Y\r\n" + 
				"X administer for Y	Y require X\r\n" + 
				"X be against Y	X eliminate Y\r\n" + 
				"X suggest Y	Y characterize by X\r\n" + 
				"X prevent Y	Y control with X\r\n" + 
				"X transmit Y	X infect with Y\r\n" + 
				"X be in Y	X indicate Y\r\n" + 
				"X see in Y	X find in Y\r\n" + 
				"X compose of Y	X contain Y\r\n" + 
				"X prescribe for Y	X give Y\r\n" + 
				"X need for Y	X use in Y\r\n" + 
				"X help Y	X take for Y\r\n" + 
				"X offer Y	Y provide by X\r\n" + 
				"X provide by Y	Y use for X\r\n" + 
				"X be with Y	Y use in X\r\n" + 
				"X cure by Y	X prevent by Y\r\n" + 
				"X cause by Y	X follow Y\r\n" + 
				"X follow by Y	X precede Y\r\n" + 
				"X be for Y	Y relieve with X\r\n" + 
				"X offer Y	Y obtain with X\r\n" + 
				"X use in Y	Y respond to X\r\n" + 
				"X provide Y	Y obtain from X\r\n" + 
				"X occur in Y	Y suggest X\r\n" + 
				"X occur in Y	Y infect with X\r\n" + 
				"X give Y	Y achieve with X\r\n" + 
				"X indicate for Y	Y provide by X\r\n" + 
				"X give to Y	Y follow by X\r\n" + 
				"X use for Y	X bring Y\r\n" + 
				"X cause by Y	X refer to Y\r\n" + 
				"X expose to Y	X give Y\r\n" + 
				"X reserve for Y	X use for Y\r\n" + 
				"X use in Y	Y give X\r\n" + 
				"X kill Y	Y get X\r\n" + 
				"X alleviate Y	Y treat with X\r\n" + 
				"X be for Y	X order for Y\r\n" + 
				"X use as Y	Y make from X\r\n" + 
				"X produce Y	X result in Y\r\n" + 
				"X precede by Y	Y lead to X\r\n" + 
				"X have Y	Y be in X\r\n" + 
				"X predispose to Y	Y cause by X\r\n" + 
				"X spread by Y	X transmit through Y\r\n" + 
				"X occur after Y	X follow Y\r\n" + 
				"X relieve Y	Y treat by X\r\n" + 
				"X affect Y	Y affect in X\r\n" + 
				"X attribute to Y	X relate to Y\r\n" + 
				"X carry Y	Y be in X\r\n" + 
				"X give to Y	Y alternate with X\r\n" + 
				"X treat with Y	X cure by Y\r\n" + 
				"X develop into Y	X lead to Y\r\n" + 
				"X cause by Y	X produce by Y\r\n" + 
				"X derive from Y	X prepare from Y\r\n" + 
				"X alleviate Y	Y treat by X\r\n" + 
				"X have Y	X ejaculate Y\r\n" + 
				"X need for Y	Y respond to X\r\n" + 
				"X use against Y	X be against Y\r\n" + 
				"X control with Y	Y help X\r\n" + 
				"X produce in Y	Y produce X\r\n" + 
				"X use as Y	Y contain X\r\n" + 
				"X find Y	X produce Y\r\n" + 
				"X affect Y	Y develop X\r\n" + 
				"X relieve Y	Y control with X\r\n" + 
				"X contain Y	X consist of Y\r\n" + 
				"X use for Y	X offer Y\r\n" + 
				"X follow Y	Y accompany X\r\n" + 
				"X refer to Y	Y associate with X\r\n" + 
				"X come from Y	Y produce X\r\n" + 
				"X give to Y	X result in Y\r\n" + 
				"X produce by Y	Y make X\r\n" + 
				"X treat by Y	Y correct X\r\n" + 
				"X prepare from Y	X contain Y\r\n" + 
				"X prescribe for Y	X recommend for Y\r\n" + 
				"X treat with Y	X manage with Y\r\n" + 
				"X be for Y	X offer Y\r\n" + 
				"X result from Y	X relate to Y\r\n" + 
				"X cause by Y	Y begin with X\r\n" + 
				"X administer for Y	X recommend for Y\r\n" + 
				"X decrease Y	X alleviate Y\r\n" + 
				"X contribute to Y	Y associate with X\r\n" + 
				"X meet Y	X unite with Y\r\n" + 
				"X indicate Y	Y associate with X\r\n" + 
				"X relieve Y	X administer for Y\r\n" + 
				"X cure by Y	X treat by Y\r\n" + 
				"X occur in Y	Y carry X\r\n" + 
				"X cause Y	X accompany Y\r\n" + 
				"X complicate Y	X cause by Y\r\n" + 
				"X present with Y	X manifest by Y\r\n" + 
				"X get into Y	X enter Y\r\n" + 
				"X characterize by Y	X have Y\r\n" + 
				"X indicate for Y	X prescribe for Y\r\n" + 
				"X suggest Y	Y produce X\r\n" + 
				"X come with Y	X accompany by Y\r\n" + 
				"X take Y	Y use in X\r\n" + 
				"X distinguish from Y	X differ from Y\r\n" + 
				"X complicate with Y	Y be in X\r\n" + 
				"X result from Y	X follow Y\r\n" + 
				"X present as Y	Y result from X\r\n" + 
				"X indicate for Y	X be for Y\r\n" + 
				"X bring Y	X give Y\r\n" + 
				"X respond to Y	Y mask X\r\n" + 
				"X fertilize Y	Y fertilize by X\r\n" + 
				"X make from Y	Y use as X\r\n" + 
				"X replace Y	X substitute for Y\r\n" + 
				"X begin as Y	Y associate with X\r\n" + 
				"X prevent Y	X fight Y\r\n" + 
				"X eliminate Y	X use against Y\r\n" + 
				"X use for Y	X administer during Y\r\n" + 
				"X use for Y	X be for Y\r\n" + 
				"X have Y	Y occur in X\r\n" + 
				"X present with Y	X accompany by Y\r\n" + 
				"X mistake for Y	X differ from Y\r\n" + 
				"X associate with Y	X suggest Y\r\n" + 
				"X spread by Y	X carry by Y\r\n" + 
				"X afford by Y	Y give X\r\n" + 
				"X see in Y	Y manifest by X\r\n" + 
				"X reduce Y	X kill Y\r\n" + 
				"X obtain with Y	Y afford X\r\n" + 
				"X affect with Y	Y affect X\r\n" + 
				"X join with Y	Y combine with X\r\n" + 
				"X enter Y	X pass through Y\r\n" + 
				"X treat with Y	X control by Y\r\n" + 
				"X be for Y	X give Y\r\n" + 
				"X trigger Y	X precipitate Y\r\n" + 
				"X use in Y	X administer during Y\r\n" + 
				"X manage with Y	X control with Y\r\n" + 
				"X cause Y	X produce Y\r\n" + 
				"X obtain with Y	Y use for X\r\n" + 
				"X associate with Y	Y accompany by X\r\n" + 
				"X have Y	X be of Y\r\n" + 
				"X make Y	X introduce Y\r\n" + 
				"X hydrolyze by Y	Y hydrolyze X\r\n" + 
				"X control by Y	Y cure X\r\n" + 
				"X give to Y	X present with Y\r\n" + 
				"X be for Y	X need for Y\r\n" + 
				"X contribute to Y	Y relate to X\r\n" + 
				"X follow Y	X associate with Y\r\n" + 
				"X enter Y	X find in Y\r\n" + 
				"X achieve with Y	Y produce X\r\n" + 
				"X provide by Y	X afford by Y\r\n" + 
				"X cure Y	Y treat with X\r\n" + 
				"X relate to Y	Y characterize by X\r\n" + 
				"X control Y	Y require X\r\n" + 
				"X result in Y	X follow by Y\r\n" + 
				"X associate with Y	Y contribute to X\r\n" + 
				"X associate with Y	Y occur in X\r\n" + 
				"X consist of Y	X contain Y\r\n" + 
				"X occur in Y	X accompany Y\r\n" + 
				"X control by Y	X require Y\r\n" + 
				"X combine with Y	Y unite with X\r\n" + 
				"X result from Y	X complicate Y\r\n" + 
				"X associate with Y	X relate to Y\r\n" + 
				"X require Y	Y control X\r\n" + 
				"X protect against Y	Y prevent by X\r\n" + 
				"X make by Y	Y produce X\r\n" + 
				"X combine with Y	Y use with X\r\n" + 
				"X produce Y	X stimulate Y\r\n" + 
				"X derive from Y	X obtain from Y\r\n" + 
				"X have Y	Y obtain from X\r\n" + 
				"X destroy Y	Y kill by X\r\n" + 
				"X produce Y	X develop with Y\r\n" + 
				"X develop for Y	X indicate for Y\r\n" + 
				"X treat with Y	Y eliminate X\r\n" + 
				"X penetrate by Y	Y fertilize X\r\n" + 
				"X have Y	Y affect X\r\n" + 
				"X result from Y	X see in Y\r\n" + 
				"X occur with Y	X accompany Y\r\n" + 
				"X cause Y	X give Y\r\n" + 
				"X present with Y	X present as Y\r\n" + 
				"X occur in Y	X see in Y\r\n" + 
				"X need for Y	X be for Y\r\n" + 
				"X accompany Y	X produce Y\r\n" + 
				"X confound with Y	Y simulate X\r\n" + 
				"X use in Y	X use during Y\r\n" + 
				"X treat with Y	Y take for X\r\n" + 
				"X substitute for Y	X replace Y\r\n" + 
				"X consist of Y	X prepare from Y\r\n" + 
				"X reduce Y	X provide from Y\r\n" + 
				"X use for Y	X provide Y\r\n" + 
				"X cause by Y	X attribute to Y\r\n" + 
				"X follow by Y	X result in Y\r\n" + 
				"X confuse with Y	Y differ from X\r\n" + 
				"X relieve by Y	Y mask X\r\n" + 
				"X include Y	Y act as X\r\n" + 
				"X offer Y	X afford Y\r\n" + 
				"X suffer from Y	Y occur in X\r\n" + 
				"X treat with Y	X require Y\r\n" + 
				"X be for Y	X relieve Y\r\n" + 
				"X fertilize by Y	Y fertilize X\r\n" + 
				"X mask Y	Y treat by X\r\n" + 
				"X involve Y	X mean Y\r\n" + 
				"X unite with Y	X join with Y\r\n" + 
				"X be with Y	Y use for X\r\n" + 
				"X suppress Y	Y treat with X\r\n" + 
				"X use for Y	Y obtain with X\r\n" + 
				"X begin with Y	Y suggest X\r\n" + 
				"X prepare from Y	X derive from Y\r\n" + 
				"X relieve by Y	Y cure X\r\n" + 
				"X prescribe for Y	X prevent Y\r\n" + 
				"X indicate Y	Y lead to X\r\n" + 
				"X decrease Y	Y treat with X\r\n" + 
				"X give Y	Y produce by X\r\n" + 
				"X control with Y	X relieve by Y\r\n" + 
				"X prevent Y	X control Y\r\n" + 
				"X reduce Y	X use for Y\r\n" + 
				"X reduce Y	Y respond to X\r\n" + 
				"X manifest as Y	X cause Y\r\n" + 
				"X follow by Y	Y accompany X\r\n" + 
				"X reduce to Y	X convert into Y\r\n" + 
				"X require for Y	X give for Y\r\n" + 
				"X relieve Y	X take for Y\r\n" + 
				"X cause Y	Y relate to X\r\n" + 
				"X occur with Y	X accompany by Y\r\n" + 
				"X mask Y	Y relieve with X\r\n" + 
				"X die of Y	X suffer from Y\r\n" + 
				"X meet Y	X join with Y\r\n" + 
				"X relieve Y	X be for Y\r\n" + 
				"X follow by Y	X accompany by Y\r\n" + 
				"X result from Y	X characterize by Y\r\n" + 
				"X start with Y	X cause Y\r\n" + 
				"X cause by Y	Y complicate by X\r\n" + 
				"X manage with Y	Y control X\r\n" + 
				"X secrete Y	Y produce by X\r\n" + 
				"X indicate Y	X result from Y\r\n" + 
				"X provide Y	X produce Y\r\n" + 
				"X manage with Y	Y use for X\r\n" + 
				"X accompany by Y	X follow Y\r\n" + 
				"X result in Y	X associate with Y\r\n" + 
				"X occur in Y	X find in Y\r\n" + 
				"X produce Y	Y introduce by X\r\n" + 
				"X cause Y	Y occur from X\r\n" + 
				"X cause by Y	X occur after Y\r\n" + 
				"X use in Y	X prescribe for Y\r\n" + 
				"X administer for Y	X reduce Y\r\n" + 
				"X be for Y	X indicate for Y\r\n" + 
				"X find in Y	X occur in Y\r\n" + 
				"X associate with Y	X complicate with Y\r\n" + 
				"X kill Y	X be against Y\r\n" + 
				"X occur with Y	X cause Y\r\n" + 
				"X control by Y	Y reduce X\r\n" + 
				"X provide by Y	Y indicate for X\r\n" + 
				"X kill Y	X eradicate Y\r\n" + 
				"X be against Y	Y respond to X\r\n" + 
				"X have Y	X infect with Y\r\n" + 
				"X find in Y	X obtain from Y\r\n" + 
				"X be for Y	X develop for Y\r\n" + 
				"X be in Y	X see in Y\r\n" + 
				"X administer for Y	X require for Y\r\n" + 
				"X characterize by Y	Y occur in X\r\n" + 
				"X associate with Y	Y see in X\r\n" + 
				"X show Y	Y occur in X\r\n" + 
				"X be for Y	X reduce Y\r\n" + 
				"X use in Y	X license for Y\r\n" + 
				"X obtain with Y	Y produce X\r\n" + 
				"X attack Y	X involve Y\r\n" + 
				"X refer to Y	X mean Y\r\n" + 
				"X be for Y	Y respond to X\r\n" + 
				"X mask Y	X relieve Y\r\n" + 
				"X take for Y	Y control with X\r\n" + 
				"X correct with Y	X require Y\r\n" + 
				"X associate with Y	X occur from Y\r\n" + 
				"X follow Y	X develop from Y\r\n" + 
				"X form from Y	Y metabolize to X\r\n" + 
				"X result in Y	Y develop from X\r\n" + 
				"X alleviate Y	Y control with X\r\n" + 
				"X relieve Y	X decrease Y\r\n" + 
				"X be in Y	X characterize by Y\r\n" + 
				"X need for Y	Y control by X\r\n" + 
				"X find in Y	Y have X\r\n" + 
				"X associate with Y	Y manifest by X\r\n" + 
				"X administer for Y	X relieve Y\r\n" + 
				"X transmit Y	Y carry by X\r\n" + 
				"X cause Y	X present with Y\r\n" + 
				"X suggest Y	X develop into Y\r\n" + 
				"X follow Y	Y suggest X\r\n" + 
				"X treat with Y	Y require for X\r\n" + 
				"X protect against Y	X prevent Y\r\n" + 
				"X give Y	Y obtain from X\r\n" + 
				"X associate with Y	Y define as X\r\n" + 
				"X occur with Y	Y produce X\r\n" + 
				"X aggravate Y	X produce Y\r\n" + 
				"X control with Y	Y order for X\r\n" + 
				"X cause Y	Y see in X\r\n" + 
				"X protect from Y	X reduce Y\r\n" + 
				"X result in Y	X lead to Y\r\n" + 
				"X define as Y	X mean Y\r\n" + 
				"X afford Y	X produce Y\r\n" + 
				"X result in Y	X present with Y\r\n" + 
				"X manage with Y	Y relieve X\r\n" + 
				"X suggest Y	X produce Y\r\n" + 
				"X carry Y	X infect with Y\r\n" + 
				"X use for Y	X mask Y\r\n" + 
				"X give for Y	X help Y\r\n" + 
				"X be for Y	X be against Y\r\n" + 
				"X be in Y	Y infect with X\r\n" + 
				"X alleviate Y	X relieve Y\r\n" + 
				"X give to Y	X administer to Y\r\n" + 
				"X administer for Y	X need for Y\r\n" + 
				"X compose of Y	Y use as X\r\n" + 
				"X precede by Y	X herald by Y\r\n" + 
				"X occur during Y	Y characterize by X\r\n" + 
				"X be in Y	X occur in Y\r\n" + 
				"X present with Y	X occur in Y\r\n" + 
				"X prescribe for Y	X reduce Y\r\n" + 
				"X reduce Y	X control Y\r\n" + 
				"X accompany by Y	Y associate with X\r\n" + 
				"X use for Y	X use in Y\r\n" + 
				"X cause by Y	Y cause X\r\n" + 
				"X manage with Y	Y be for X\r\n" + 
				"X cause Y	X complicate Y\r\n" + 
				"X be in Y	X affect Y\r\n" + 
				"X make Y	Y produce in X\r\n" + 
				"X follow by Y	X give to Y\r\n" + 
				"X improve Y	X relieve Y\r\n" + 
				"X give for Y	X provide from Y\r\n" + 
				"X follow Y	X occur from Y\r\n" + 
				"X associate with Y	Y mean X\r\n" + 
				"X treat with Y	Y control X\r\n" + 
				"X cause by Y	Y induce X\r\n" + 
				"X use in Y	X require for Y\r\n" + 
				"X take for Y	X be for Y\r\n" + 
				"X unite with Y	X combine with Y\r\n" + 
				"X manufacture Y	Y produce by X\r\n" + 
				"X be in Y	Y contain X\r\n" + 
				"X accompany with Y	Y associate with X\r\n" + 
				"X imply Y	Y associate with X\r\n" + 
				"X prevent Y	X reduce Y\r\n" + 
				"X present with Y	X give to Y\r\n" + 
				"X give Y	Y give to X\r\n" + 
				"X produce Y	X occur with Y\r\n" + 
				"X be with Y	Y indicate for X\r\n" + 
				"X achieve with Y	Y provide X\r\n" + 
				"X occur in Y	X characterize by Y\r\n" + 
				"X cure by Y	Y cure X\r\n" + 
				"X mean Y	X characterize by Y\r\n" + 
				"X cause Y	Y develop from X\r\n" + 
				"X cause Y	X associate Y\r\n" + 
				"X control by Y	X cure with Y\r\n" + 
				"X characterize by Y	X be with Y\r\n" + 
				"X mistake for Y	Y mistake for X\r\n" + 
				"X base on Y	X contain Y\r\n" + 
				"X consist of Y	Y be in X\r\n" + 
				"X result in Y	X cause Y\r\n" + 
				"X occur after Y	Y associate with X\r\n" + 
				"X predispose to Y	Y associate with X\r\n" + 
				"X result from Y	Y give to X\r\n" + 
				"X control by Y	Y control X\r\n" + 
				"X accompany Y	X occur with Y\r\n" + 
				"X give with Y	Y treat with X\r\n" + 
				"X cause by Y	Y present with X\r\n" + 
				"X associate with Y	X see in Y\r\n" + 
				"X treat Y	X use for Y\r\n" + 
				"X metabolize to Y	Y form from X\r\n" + 
				"X aggravate Y	X cause Y\r\n" + 
				"X be for Y	X give for Y\r\n" + 
				"X overcome by Y	X relieve by Y\r\n" + 
				"X be in Y	Y control by X\r\n" + 
				"X take for Y	Y treat with X\r\n" + 
				"X help Y	X administer for Y\r\n" + 
				"X encounter Y	Y combine with X\r\n" + 
				"X produce Y	X cause Y\r\n" + 
				"X indicate Y	X see in Y\r\n" + 
				"X help Y	Y respond to X\r\n" + 
				"X combine with Y	X administer with Y\r\n" + 
				"X help Y	Y treat with X\r\n" + 
				"X reduce Y	X use in Y\r\n" + 
				"X relate to Y	X associate with Y\r\n" + 
				"X herald by Y	X associate with Y\r\n" + 
				"X administer for Y	X order for Y\r\n" + 
				"X provide Y	X offer Y\r\n" + 
				"X control with Y	X manage with Y\r\n" + 
				"X occur in Y	X occur with Y\r\n" + 
				"X prescribe for Y	Y achieve with X\r\n" + 
				"X complicate by Y	X progress to Y\r\n" + 
				"X give during Y	X use during Y\r\n" + 
				"X affect with Y	Y develop in X\r\n" + 
				"X help with Y	X relieve Y\r\n" + 
				"X use for Y	X reduce Y\r\n" + 
				"X treat with Y	X control with Y\r\n" + 
				"X occur in Y	X develop in Y\r\n" + 
				"X respond to Y	Y require for X\r\n" + 
				"X derive from Y	X isolate from Y\r\n" + 
				"X follow Y	X cause by Y\r\n" + 
				"X inject into Y	X fertilize Y\r\n" + 
				"X eradicate Y	Y treat by X\r\n" + 
				"X cause Y	Y occur with X\r\n" + 
				"X complicate Y	Y follow by X\r\n" + 
				"X be for Y	Y manage with X\r\n" + 
				"X be for Y	Y provide by X\r\n" + 
				"X produce Y	Y accompany X\r\n" + 
				"X respond to Y	X relieve by Y\r\n" + 
				"X produce Y	X accompany by Y\r\n" + 
				"X see in Y	X result from Y\r\n" + 
				"X follow by Y	Y associate with X\r\n" + 
				"X cause by Y	X develop after Y\r\n" + 
				"X penetrate Y	X enter Y\r\n" + 
				"X use for Y	X decrease Y\r\n" + 
				"X associate with Y	X come with Y\r\n" + 
				"X develop in Y	X be in Y\r\n" + 
				"X develop for Y	X use in Y\r\n" + 
				"X cause Y	X predispose to Y\r\n" + 
				"X cause by Y	X accompany Y\r\n" + 
				"X control Y	Y treat by X\r\n" + 
				"X result from Y	Y suggest X\r\n" + 
				"X cause by Y	X develop from Y\r\n" + 
				"X reduce Y	X administer for Y\r\n" + 
				"X prevent Y	Y control by X\r\n" + 
				"X use for Y	Y achieve with X\r\n" + 
				"X protect against Y	X protect from Y\r\n" + 
				"X provide Y	X use for Y\r\n" + 
				"X cause by Y	Y follow by X\r\n" + 
				"X expose to Y	X develop Y\r\n" + 
				"X recommend for Y	X give for Y\r\n" + 
				"X define as Y	X cause by Y\r\n" + 
				"X relieve by Y	Y require for X\r\n" + 
				"X confuse with Y	Y resemble X\r\n" + 
				"X develop for Y	X use for Y\r\n" + 
				"X respond to Y	Y use for X\r\n" + 
				"X obtain from Y	Y give X\r\n" + 
				"X associate with Y	Y give to X\r\n" + 
				"X accompany by Y	X accompany with Y\r\n" + 
				"X be against Y	X be for Y\r\n" + 
				"X develop Y	X produce Y\r\n" + 
				"X metabolize by Y	X metabolize in Y\r\n" + 
				"X treat with Y	X relieve with Y\r\n" + 
				"X obtain with Y	Y bring X\r\n" + 
				"X produce Y	X present as Y\r\n" + 
				"X penetrate by Y	Y inject into X\r\n" + 
				"X relieve with Y	X control by Y\r\n" + 
				"X consist of Y	X make from Y\r\n" + 
				"X relieve Y	X be in Y\r\n" + 
				"X have Y	X provide Y\r\n" + 
				"X suggest Y	Y consist of X\r\n" + 
				"X join with Y	Y encounter X\r\n" + 
				"X suggest Y	Y associate with X\r\n" + 
				"X prepare from Y	X consist of Y\r\n" + 
				"X result in Y	X characterize by Y\r\n" + 
				"X prevent Y	X protect against Y\r\n" + 
				"X cause Y	X manifest as Y\r\n" + 
				"X treat by Y	Y relieve X\r\n" + 
				"X control with Y	Y mask X\r\n" + 
				"X associate with Y	Y result from X\r\n" + 
				"X use as Y	Y prepare from X\r\n" + 
				"X inhibit Y	Y inhibit by X\r\n" + 
				"X kill Y	Y respond to X\r\n" + 
				"X prevent Y	X relieve Y\r\n" + 
				"X relieve by Y	Y administer for X\r\n" + 
				"X introduce Y	Y introduce by X\r\n" + 
				"X follow by Y	X lead to Y\r\n" + 
				"X produce by Y	X obtain from Y\r\n" + 
				"X produce by Y	X produce in Y\r\n" + 
				"X relieve with Y	X relieve by Y\r\n" + 
				"X achieve with Y	Y bring X\r\n" + 
				"X metabolize to Y	X convert into Y\r\n" + 
				"X release into Y	X enter Y\r\n" + 
				"X use with Y	Y combine with X\r\n" + 
				"X offer Y	X produce Y\r\n" + 
				"X secrete Y	X produce Y\r\n" + 
				"X consist of Y	X characterize by Y\r\n" + 
				"X metabolize to Y	X convert to Y\r\n" + 
				"X be in Y	X help Y\r\n" + 
				"X inhibit Y	X prevent Y\r\n" + 
				"X lead to Y	X predispose to Y\r\n" + 
				"X occur in Y	X attack Y\r\n" + 
				"X manage with Y	X control by Y\r\n" + 
				"X discover Y	X find Y\r\n" + 
				"X control by Y	X treat with Y\r\n" + 
				"X transmit through Y	Y transmit X\r\n" + 
				"X characterize by Y	Y be with X\r\n" + 
				"X occur with Y	Y accompany by X\r\n" + 
				"X reach Y	X enter Y\r\n" + 
				"X be for Y	X use for Y\r\n" + 
				"X be for Y	Y be with X\r\n" + 
				"X suffer from Y	Y be in X\r\n" + 
				"X manifest by Y	X lead to Y\r\n" + 
				"X relieve by Y	Y use for X\r\n" + 
				"X take for Y	X reduce Y\r\n" + 
				"X reduce Y	X take for Y\r\n" + 
				"X afford by Y	Y provide X\r\n" + 
				"X cause Y	Y be in X\r\n" + 
				"X simulate Y	X resemble Y\r\n" + 
				"X transmit by Y	Y carry X\r\n" + 
				"X suggest Y	Y have X\r\n" + 
				"X be in Y	Y complicate with X\r\n" + 
				"X produce Y	X manufacture Y\r\n" + 
				"X fight Y	X prevent Y\r\n" + 
				"X produce by Y	Y lead to X\r\n" + 
				"X use for Y	X need for Y\r\n" + 
				"X associate with Y	X occur during Y\r\n" + 
				"X order for Y	X prescribe for Y\r\n" + 
				"X treat with Y	Y alleviate X\r\n" + 
				"X manifest by Y	X produce Y\r\n" + 
				"X associate with Y	X herald by Y\r\n" + 
				"X prescribe for Y	Y require X\r\n" + 
				"X use for Y	Y correct by X\r\n" + 
				"X indicate for Y	X indicate in Y\r\n" + 
				"X give Y	X be for Y\r\n" + 
				"X herald by Y	X precede by Y\r\n" + 
				"X combine with Y	X contain Y\r\n" + 
				"X produce Y	X follow by Y\r\n" + 
				"X cause Y	Y induce by X\r\n" + 
				"X use in Y	X give for Y\r\n" + 
				"X provide by Y	Y give X\r\n" + 
				"X affect by Y	Y be against X\r\n" + 
				"X prevent by Y	Y eliminate X\r\n" + 
				"X relieve Y	Y control by X\r\n" + 
				"X complicate by Y	X follow by Y\r\n" + 
				"X use as Y	X have Y\r\n" + 
				"X recommend for Y	X use for Y\r\n" + 
				"X transmit by Y	Y infect with X\r\n" + 
				"X accompany by Y	Y cause by X\r\n" + 
				"X use for Y	X correct Y\r\n" + 
				"X find in Y	Y develop X\r\n" + 
				"X mistake for Y	Y distinguish from X\r\n" + 
				"X result from Y	X suggest Y\r\n" + 
				"X help Y	X need for Y\r\n" + 
				"X reduce Y	Y manage with X\r\n" + 
				"X relate to Y	X cause by Y\r\n" + 
				"X treat with Y	Y give with X\r\n" + 
				"X control by Y	Y help X\r\n" + 
				"X relieve by Y	X overcome by Y\r\n" + 
				"X cause Y	X manifest by Y\r\n" + 
				"X use for Y	X relieve Y\r\n" + 
				"X die of Y	X develop Y\r\n" + 
				"X improve Y	Y treat with X\r\n" + 
				"X result in Y	Y cause by X\r\n" + 
				"X require Y	X control by Y\r\n" + 
				"X control by Y	Y be against X\r\n" + 
				"X result in Y	X suggest Y\r\n" + 
				"X present as Y	Y occur in X\r\n" + 
				"X suggest Y	X accompany Y\r\n" + 
				"X follow Y	Y lead to X\r\n" + 
				"X result in Y	Y result from X\r\n" + 
				"X cause Y	X affect Y\r\n" + 
				"X result in Y	Y indicate X\r\n" + 
				"X prescribe for Y	X indicate for Y\r\n" + 
				"X mask Y	Y manage with X\r\n" + 
				"X recommend for Y	X provide Y\r\n" + 
				"X help Y	Y relieve by X\r\n" + 
				"X relieve with Y	X treat with Y\r\n" + 
				"X cause by Y	X complicate Y\r\n" + 
				"X cause Y	Y occur after X\r\n" + 
				"X be in Y	Y achieve with X\r\n" + 
				"X relieve by Y	Y order for X\r\n" + 
				"X indicate Y	Y accompany by X\r\n" + 
				"X induce Y	X produce Y\r\n" + 
				"X relieve by Y	Y reduce X\r\n" + 
				"X occur from Y	Y lead to X\r\n" + 
				"X use for Y	X use as Y\r\n" + 
				"X use for Y	X develop for Y\r\n" + 
				"X occur in Y	X involve Y\r\n" + 
				"X develop Y	Y find in X\r\n" + 
				"X correct by Y	Y give for X\r\n" + 
				"X suggest Y	X indicate Y\r\n" + 
				"X use against Y	X eliminate Y\r\n" + 
				"X respond to Y	X require Y\r\n" + 
				"X contain Y	X base on Y\r\n" + 
				"X recommend for Y	X afford Y\r\n" + 
				"X accompany by Y	Y accompany by X\r\n" + 
				"X give to Y	X associate with Y\r\n" + 
				"X transmit by Y	Y transmit X\r\n" + 
				"X follow Y	Y give to X\r\n" + 
				"X result from Y	Y contribute to X\r\n" + 
				"X develop in Y	X affect Y\r\n" + 
				"X involve Y	X attack Y\r\n" + 
				"X complicate Y	Y complicate with X\r\n" + 
				"X cure with Y	Y control X\r\n" + 
				"X correct by Y	X treat by Y\r\n" + 
				"X administer before Y	X give before Y\r\n" + 
				"X use as Y	X employ as Y\r\n" + 
				"X develop after Y	Y cause X\r\n" + 
				"X require Y	X cure by Y\r\n" + 
				"X be against Y	X kill Y\r\n" + 
				"X spread by Y	Y transmit X\r\n" + 
				"X provide Y	X bring Y\r\n" + 
				"X control with Y	Y provide from X\r\n" + 
				"X cause Y	X involve Y\r\n" + 
				"X reduce Y	Y treat with X\r\n" + 
				"X manifest by Y	Y indicate X\r\n" + 
				"X cure Y	Y treat by X\r\n" + 
				"X control with Y	Y treat X\r\n" + 
				"X require Y	X relieve by Y\r\n" + 
				"X control Y	X indicate for Y\r\n" + 
				"X prescribe for Y	X cure Y\r\n" + 
				"X obtain from Y	X occur in Y\r\n" + 
				"X cause by Y	X characterize by Y\r\n" + 
				"X use in Y	Y use X\r\n" + 
				"X relieve Y	X control Y\r\n" + 
				"X find in Y	X isolate from Y\r\n" + 
				"X be in Y	Y manage with X\r\n" + 
				"X treat with Y	Y improve X\r\n" + 
				"X be in Y	X suggest Y\r\n" + 
				"X alleviate Y	Y control by X\r\n" + 
				"X accompany by Y	Y follow X\r\n" + 
				"X cause Y	Y attribute to X\r\n" + 
				"X control by Y	Y be in X\r\n" + 
				"X give to Y	Y associate with X\r\n" + 
				"X turn to Y	X develop into Y\r\n" + 
				"X combine with Y	Y combine with X\r\n" + 
				"X transmit via Y	X transmit through Y\r\n" + 
				"X suggest Y	Y start with X\r\n" + 
				"X reduce Y	X help Y\r\n" + 
				"X lead to Y	X manifest by Y\r\n" + 
				"X control Y	X prevent Y\r\n" + 
				"X cause Y	X exacerbate Y\r\n" + 
				"X occur in Y	Y characterize by X\r\n" + 
				"X bring Y	Y obtain with X\r\n" + 
				"X indicate Y	Y present as X\r\n" + 
				"X begin as Y	X cause Y\r\n" + 
				"X provide Y	X have Y\r\n" + 
				"X obtain with Y	Y prescribe for X\r\n" + 
				"X produce Y	X afford Y\r\n" + 
				"X manifest as Y	Y result from X\r\n" + 
				"X make Y	X manufacture Y\r\n" + 
				"X carry Y	X transmit Y\r\n" + 
				"X relieve with Y	Y help X\r\n" + 
				"X occur after Y	X cause by Y\r\n" + 
				"X suppress Y	X reduce Y\r\n" + 
				"X produce Y	Y accompany by X\r\n" + 
				"X achieve with Y	Y recommend for X\r\n" + 
				"X begin with Y	X characterize by Y\r\n" + 
				"X transmit from Y	Y have X\r\n" + 
				"X suggest Y	X cause Y\r\n" + 
				"X cure by Y	Y relieve X\r\n" + 
				"X convert into Y	Y derive from X\r\n" + 
				"X afford Y	X bring Y\r\n" + 
				"X manifest by Y	X associate with Y\r\n" + 
				"X reduce Y	X order for Y\r\n" + 
				"X give for Y	Y require X\r\n" + 
				"X cure with Y	X control with Y\r\n" + 
				"X indicate for Y	Y be with X\r\n" + 
				"X control Y	Y manage with X\r\n" + 
				"X control by Y	X treat by Y\r\n" + 
				"X require Y	Y prevent X\r\n" + 
				"X complicate by Y	X develop into Y\r\n" + 
				"X alleviate Y	Y relieve with X\r\n" + 
				"X use as Y	Y use as X\r\n" + 
				"X use in Y	X administer to Y\r\n" + 
				"X affect by Y	Y be in X\r\n" + 
				"X be with Y	Y affect X\r\n" + 
				"X require Y	Y give for X\r\n" + 
				"X respond to Y	X manage with Y\r\n" + 
				"X result in Y	Y associate with X\r\n" + 
				"X fertilize Y	Y penetrate by X\r\n" + 
				"X fertilize by Y	Y penetrate X\r\n" + 
				"X cause Y	X occur with Y\r\n" + 
				"X produce Y	Y come from X\r\n" + 
				"X need for Y	Y control with X\r\n" + 
				"X cause by Y	Y lead to X\r\n" + 
				"X make from Y	X contain Y\r\n" + 
				"X control by Y	Y relieve X\r\n" + 
				"X produce Y	X discharge Y\r\n" + 
				"X relieve Y	Y overcome by X\r\n" + 
				"X attribute to Y	X associate with Y\r\n" + 
				"X follow Y	Y produce X\r\n" + 
				"X order for Y	X need for Y\r\n" + 
				"X involve Y	X affect Y\r\n" + 
				"X follow Y	X accompany Y\r\n" + 
				"X produce Y	X provide Y\r\n" + 
				"X affect by Y	X kill by Y\r\n" + 
				"X order for Y	X use for Y\r\n" + 
				"X provide Y	X prescribe for Y\r\n" + 
				"X use for Y	Y include X\r\n" + 
				"X cure Y	Y require X\r\n" + 
				"X control by Y	Y mask X\r\n" + 
				"X follow Y	X result from Y\r\n" + 
				"X follow Y	Y complicate X\r\n" + 
				"X prevent Y	X inhibit Y\r\n" + 
				"X produce Y	X manifest by Y\r\n" + 
				"X include Y	X know as Y\r\n" + 
				"X be in Y	Y suffer from X\r\n" + 
				"X recommend for Y	Y use X\r\n" + 
				"X base on Y	X compose of Y\r\n" + 
				"X lead to Y	X characterize by Y\r\n" + 
				"X bring Y	Y achieve with X\r\n" + 
				"X occur from Y	Y associate with X\r\n" + 
				"X cover Y	Y treat with X\r\n" + 
				"X produce by Y	Y release X\r\n" + 
				"X occur from Y	Y result in X\r\n" + 
				"X associate with Y	X follow by Y\r\n" + 
				"X eliminate Y	X be against Y\r\n" + 
				"X require for Y	X be for Y\r\n" + 
				"X develop Y	Y cause by X\r\n" + 
				"X correct with Y	X treat by Y\r\n" + 
				"X result in Y	X contribute to Y\r\n" + 
				"X relieve Y	Y manage with X\r\n" + 
				"X cause by Y	Y accompany by X\r\n" + 
				"X cause by Y	X see in Y\r\n" + 
				"X cause by Y	Y predispose to X\r\n" + 
				"X meet Y	Y combine with X\r\n" + 
				"X affect Y	Y affect with X\r\n" + 
				"X respond to Y	Y eliminate X\r\n" + 
				"X cure Y	Y relieve by X\r\n" + 
				"X kill Y	Y die of X\r\n" + 
				"X develop from Y	X follow Y\r\n" + 
				"X progress to Y	X complicate by Y\r\n" + 
				"X mean Y	X cause Y\r\n" + 
				"X overcome by Y	X treat with Y\r\n" + 
				"X accompany by Y	Y complicate X\r\n" + 
				"X transmit by Y	X spread through Y\r\n" + 
				"X introduce by Y	Y produce X\r\n" + 
				"X achieve with Y	Y indicate for X\r\n" + 
				"X break Y	X digest Y\r\n" + 
				"X be during Y	X occur during Y\r\n" + 
				"X be with Y	X characterize by Y\r\n" + 
				"X penetrate by Y	X fertilize by Y\r\n" + 
				"X prevent Y	Y require X\r\n" + 
				"X relieve by Y	X control with Y\r\n" + 
				"X treat by Y	Y alleviate X\r\n" + 
				"X cure Y	Y control by X\r\n" + 
				"X treat by Y	Y cure X\r\n" + 
				"X help Y	X alleviate Y\r\n" + 
				"X develop Y	X expose to Y\r\n" + 
				"X infect with Y	X transmit Y\r\n" + 
				"X characterize by Y	X cause by Y\r\n" + 
				"X accompany Y	Y follow X\r\n" + 
				"X attribute to Y	X cause by Y\r\n" + 
				"X occur in Y	Y transmit X\r\n" + 
				"X follow by Y	Y cause by X\r\n" + 
				"X exacerbate Y	X cause Y\r\n" + 
				"X treat with Y	Y recommend for X\r\n" + 
				"X involve Y	X cause Y\r\n" + 
				"X develop Y	X have Y\r\n" + 
				"X convert into Y	Y form from X\r\n" + 
				"X reduce to Y	X convert to Y\r\n" + 
				"X discover Y	X invent Y\r\n" + 
				"X suggest Y	X occur in Y\r\n" + 
				"X see in Y	Y produce X\r\n" + 
				"X follow by Y	Y alternate with X\r\n" + 
				"X be in Y	X relieve Y\r\n" + 
				"X require Y	X respond to Y\r\n" + 
				"X infect with Y	Y find in X\r\n" + 
				"X transmit Y	Y occur in X\r\n" + 
				"X control with Y	X control by Y\r\n" + 
				"X use for Y	X treat Y\r\n" + 
				"X cause by Y	Y associate with X\r\n" + 
				"X follow Y	Y precede X\r\n" + 
				"X unite with Y	Y combine with X\r\n";
		private String testSetNeg = "X expose to Y	X protect from Y\r\n" + 
				"X differ from Y	Y resemble X\r\n" + 
				"X distinguish from Y	Y associate with X\r\n" + 
				"X know as Y	X resemble Y\r\n" + 
				"X protect from Y	X expose to Y\r\n" + 
				"X associate with Y	Y occur X\r\n" + 
				"X characterize by Y	Y distinguish from X\r\n" + 
				"X distinguish from Y	Y have X\r\n" + 
				"X derive from Y	X kill Y\r\n" + 
				"X include Y	Y differ from X\r\n" + 
				"X have Y	Y distinguish from X\r\n" + 
				"X produce by Y	X destroy Y\r\n" + 
				"X derive from Y	X convert to Y\r\n" + 
				"X be in Y	X contain Y\r\n" + 
				"X be in Y	X have Y\r\n" + 
				"X derive from Y	X be against Y\r\n" + 
				"X contain Y	X be in Y\r\n" + 
				"X include Y	Y substitute for X\r\n" + 
				"X resemble Y	Y differentiate from X\r\n" + 
				"X simulate Y	X confound with Y\r\n" + 
				"X convert to Y	X derive from Y\r\n" + 
				"X produce Y	Y kill X\r\n" + 
				"X treat by Y	Y produce X\r\n" + 
				"X be against Y	X derive from Y\r\n" + 
				"X resemble Y	Y confuse with X\r\n" + 
				"X distinguish from Y	Y confound with X\r\n" + 
				"X associate with Y	Y distinguish from X\r\n" + 
				"X be in Y	Y occur in X\r\n" + 
				"X avoid in Y	X use in Y\r\n" + 
				"X prevent Y	X produce Y\r\n" + 
				"X differentiate from Y	Y resemble X\r\n" + 
				"X resemble Y	X confound with Y\r\n" + 
				"X kill Y	X derive from Y\r\n" + 
				"X get Y	Y kill X\r\n" + 
				"X contract Y	X die of Y\r\n" + 
				"X include Y	X confuse with Y\r\n" + 
				"X differ from Y	Y include X\r\n" + 
				"X substitute for Y	Y include X\r\n" + 
				"X be against Y	Y produce X\r\n" + 
				"X produce Y	Y use in X\r\n" + 
				"X produce by Y	X be against Y\r\n" + 
				"X simulate Y	X differ from Y\r\n" + 
				"X mimic Y	Y differentiate from X\r\n" + 
				"X characterize by Y	Y include X\r\n" + 
				"X correct Y	X indicate for Y\r\n" + 
				"X expose to Y	X die of Y\r\n" + 
				"X resemble Y	X know as Y\r\n" + 
				"X occur Y	Y associate with X\r\n" + 
				"X resemble Y	X distinguish from Y\r\n" + 
				"X confuse with Y	X include Y\r\n" + 
				"X produce by Y	X kill Y\r\n" + 
				"X produce Y	Y give to X\r\n" + 
				"X contract Y	X die from Y\r\n" + 
				"X be in Y	Y find in X\r\n" + 
				"X distinguish from Y	X characterize by Y\r\n" + 
				"X produce Y	Y be against X\r\n" + 
				"X resemble Y	Y distinguish from X\r\n" + 
				"X use in Y	Y produce X\r\n" + 
				"X have Y	X be in Y\r\n" + 
				"X suffer from Y	X die of Y\r\n" + 
				"X treat with Y	X produce Y\r\n" + 
				"X destroy Y	X produce by Y\r\n" + 
				"X simulate Y	Y confound with X\r\n" + 
				"X distinguish from Y	X associate with Y\r\n" + 
				"X give to Y	Y produce X\r\n" + 
				"X distinguish from Y	X confound with Y\r\n" + 
				"X develop Y	X die of Y\r\n" + 
				"X differ from Y	X confound with Y\r\n" + 
				"X characterize by Y	X distinguish from Y\r\n" + 
				"X produce Y	X prevent Y\r\n" + 
				"X resemble Y	Y differ from X\r\n" + 
				"X indicate for Y	X correct Y\r\n" + 
				"X distinguish from Y	X resemble Y\r\n" + 
				"X have Y	X die of Y\r\n" + 
				"X destroy Y	X derive from Y\r\n" + 
				"X kill Y	X produce by Y\r\n" + 
				"X associate with Y	X distinguish from Y\r\n" + 
				"X occur in Y	Y be in X\r\n" + 
				"X contract Y	Y kill X\r\n" + 
				"X resemble Y	X differ from Y\r\n" + 
				"X produce Y	Y destroy X\r\n" + 
				"X constrict Y	X dilate Y\r\n" + 
				"X produce Y	X treat with Y\r\n" + 
				"X be against Y	X produce by Y\r\n" + 
				"X dilate Y	X constrict Y\r\n" + 
				"X use in Y	X avoid in Y\r\n" + 
				"X get Y	X die of Y\r\n" + 
				"X produce Y	Y treat by X\r\n" + 
				"X simulate Y	Y distinguish from X\r\n" + 
				"X kill Y	Y produce X\r\n" + 
				"X derive from Y	X destroy Y\r\n" + 
				"X distinguish from Y	Y characterize by X\r\n" + 
				"X resemble Y	X confuse with Y\r\n" + 
				"X find in Y	Y be in X\r\n" + 
				"X destroy Y	Y produce X\r\n" + 
				"X convert to Y	Y convert to X\r\n" + 
				"X have Y	Y kill X\r\n" + 
				"X differentiate from Y	Y mimic X\r\n" + 
				"X get Y	X die from Y\r\n";
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			testSet = new HashMap<String, List<String>>();
//			List<String> pos = Files.readAllLines(Paths.get("D:\\DSP3\\positive-preds.txt"));
//			List<String> neg = Files.readAllLines(Paths.get("D:\\DSP3\\negative-preds.txt"));
			String[] pos = testSetPos.split("\r\n");
			String[] neg = testSetNeg.split("\r\n");
			addToTestSet(pos);
			addToTestSet(neg);
		}

		private void addToTestSet(String[] fileLines) {
			for (String line : fileLines) {
				String[] parts = line.split("\t");
				String path1 = parts[0].substring(2, parts[0].lastIndexOf(' '));
				String path2 = parts[1].substring(2, parts[1].lastIndexOf(' '));
//				String path1 = parts[0].charAt(0) + parts[0].substring(2, parts[0].length() - 2)
//						+ parts[0].charAt(parts[0].length() - 1);
//				String path2 = parts[1].charAt(0) + parts[1].substring(2, parts[1].length() - 2)
//						+ parts[1].charAt(parts[1].length() - 1);

				path1 = runStemmer.englishStem(path1);
				path2 = runStemmer.englishStem(path2);

				if (!testSet.containsKey(path1)) {
					List<String> connectedPaths = new ArrayList<String>();
					connectedPaths.add(path2);
					testSet.put(path1, connectedPaths);
				} else {
					if (!testSet.get(path1).contains(path2))
						testSet.get(path1).add(path2);
				}

				if (!testSet.containsKey(path2)) {
					List<String> connectedPaths = new ArrayList<String>();
					connectedPaths.add(path1);
					testSet.put(path2, connectedPaths);
				} else {
					if (!testSet.get(path2).contains(path1))
						testSet.get(path2).add(path1);
				}
			}
		}

		public void reduce(Triple key, Iterable<PairDouble> values, Context context)
				throws IOException, InterruptedException {
		
			if (key.getFirst().toString().equals("*")) {
				for (PairDouble val : values) {
					String path = new String(val.getFirst().toString());
					if (testSet.containsKey(path)) {
						String p1 = "", p2 = "";
						for (String p : testSet.get(path)) {
							if (path.compareTo(p) < 0) {
								p1 = new String(path);
								p2 = new String(p);
							} else if (path.compareTo(p) > 0) {
								p1 = new String(p);
								p2 = new String(path);
							} else { // ==
								p1 = new String(path);
								p2 = new String(p);
								context.write(new Triple(new Text(p1), new Text(p2), key.getThird()), new PairDouble(key.getSecond(), val.getSecond()));
							}
							context.write(new Triple(new Text(p1), new Text(p2), key.getThird()), new PairDouble(key.getSecond(), val.getSecond()));
						}
					}
				}
			} else {
				Double MI = new Double(0);
				String path = new String(key.getFirst().toString());
				for (PairDouble val : values) {
					MI = new Double(MI + val.getSecond().get());
				}
				if (testSet.containsKey(path)) {
					String p1 = "", p2 = "";
					for (String p : testSet.get(path)) {
						if (path.compareTo(p) <= 0) {
							p1 = new String(path);
							p2 = new String(p);
						} else {
							p1 = new String(p);
							p2 = new String(path);
						}
						context.write(new Triple(new Text(p1), new Text(p2), new Text("*")), new PairDouble(key.getSecond(), new DoubleWritable(MI)));
					}
				}
			}
		}
		
		
		
		
		/*public void reduce(Triple key, Iterable<PairDouble> values, Context context)
				throws IOException, InterruptedException {
			if (key.getFirst().toString().equals("*")) {
				for (PairDouble val : values) {
					String path = new String(val.getFirst().toString());
					if (testSet.containsKey("X" + path + "Y")) {
						for (String p : testSet.get("X" + path + "Y")) {
							String p1 = "", p2 = "";
							p = p.substring(1, p.length() - 1);
							if (path.compareTo(p) < 0) {
								p1 = new String(path);
								p2 = new String(p);
							} else if (path.compareTo(p) > 0) {
								p1 = new String(p);
								p2 = new String(path);
							} else { // ==
								p1 = new String(path);
								p2 = new String(p);
								context.write(new Triple(new Text(p1), new Text(p2), new Text(key.getThird())),
										new PairDouble(new Text(key.getSecond()),
												new DoubleWritable(val.getSecond().get())));
							}
							context.write(new Triple(new Text(p1), new Text(p2), new Text(key.getThird())),
									new PairDouble(new Text(key.getSecond()),
											new DoubleWritable(val.getSecond().get())));
						}
					} else if (testSet.containsKey("Y" + path + " X")) {
						for (String p : testSet.get("Y" + path + "X")) {
							String p1 = "", p2 = "";
							p = p.substring(1, p.length() - 1);
							String slot = key.getSecond().toString().equals("x") ? "y" : "x";
							if (path.compareTo(p) < 0) {
								p1 = new String(path);
								p2 = new String(p);
							} else if (path.compareTo(p) > 0) {
								p1 = new String(p);
								p2 = new String(path);
							} else { // ==
								p1 = new String(path);
								p2 = new String(p);
								context.write(new Triple(new Text(p1), new Text(p2), new Text(key.getThird())),
										new PairDouble(new Text(slot), new DoubleWritable(val.getSecond().get())));
							}
							context.write(new Triple(new Text(p1), new Text(p2), new Text(key.getThird())),
									new PairDouble(new Text(slot), new DoubleWritable(val.getSecond().get())));
						}
					}
				}
			} else {
				Double MI = new Double(0);
				String path = new String(key.getFirst().toString());
				for (PairDouble val : values) {
					MI = new Double(MI + val.getSecond().get());
				}
				if (testSet.containsKey("X" + path + "Y")) {
					for (String p : testSet.get("X" + path + "Y")) {
						p = p.substring(1, p.length() - 1);
						context.write(new Triple(new Text(key.getFirst()), new Text(p), new Text("*")),
								new PairDouble(new Text(key.getSecond()), new DoubleWritable(MI)));
					}
				}
				if (testSet.containsKey("Y" + path + "X")) {
					String slot = key.getSecond().toString().equals("x") ? "y" : "x";
					for (String p : testSet.get("Y" + path + "X")) {
						p = p.substring(1, p.length() - 1);
						context.write(new Triple(new Text(key.getFirst()), new Text(p), new Text("*")),
								new PairDouble(new Text(slot), new DoubleWritable(MI)));
					}
				}
			}
		}*/
	}
}