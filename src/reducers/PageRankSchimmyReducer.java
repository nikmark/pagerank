package reducers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import utils.Data;

public class PageRankSchimmyReducer extends Reducer<IntWritable, Data, Text, Text> {

	private Double total_loss = new Double(0.0);
	private final Double ALPHA = new Double(0.15);
	private Integer cardinality = new Integer(0);
	private String myTaskNumber = new String();

	protected void setup(Reducer<IntWritable, Data, Text, Text>.Context context)
			throws IOException, InterruptedException {

		cardinality = context.getConfiguration().getInt("cardinality", 1);

		FileSystem fs = FileSystem.get(context.getConfiguration());
		RemoteIterator<LocatedFileStatus> list = fs.listFiles(new Path(
				"loss-tmp"), true);

		while (list.hasNext()) {

			FSDataInputStream out = fs.open(list.next().getPath());
			BufferedReader bw = new BufferedReader(new InputStreamReader(out));

			total_loss += Double.parseDouble(bw.readLine());
			bw.close();
		}

		myTaskNumber = context.getTaskAttemptID().toString();

	};

	@Override
	protected void reduce(IntWritable key, Iterable<Data> values, Context context)
			throws IOException, InterruptedException {

		// myTaskNumber = context.getTaskAttemptID().getTaskID().getId();

//		 context.write(key, new Text(" reduce number: "+myTaskNumber));
		Double sum = new Double(0);
		Data n = new Data();

		for (Data val : values) {

			if (val.getPageRankOld() != -1) {
				n = new Data(val);
			}
			sum += val.getPageRank();
		}

		sum = sum + (total_loss / cardinality);

		Double p = (ALPHA / cardinality) + (1 - ALPHA) * sum;

		if (Math.abs(p - n.getPageRankOld()) > (0.1 / cardinality)) {
			FileSystem fs = FileSystem.get(context.getConfiguration());
			try {
				fs.createNewFile(new Path("convergence"));
			} catch (IOException ioe) {
			}
		}

//		System.out.println("key = " + key.toString() + " val= " + p.toString());
//		context.write(key, new Text("c"));;
		context.write(new Text(key.toString()), new Text(p.toString()));

	}

}
