package sequencefile.reducers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import sequencefile.utils.Node;

/**
 * Reducer per il calcolo del PageRank con lo Schimmy design pattern. Tutti i calcoli di convergenza ecc, vengono eseguiti nel cleanup().
 * 
 * @author Nicolò Marchi, Fabio Pettenuzzo
 *
 */	
public class PageRankSchimmyReducer extends Reducer<LongWritable, DoubleWritable, LongWritable, Node> {

	private Double total_loss;
	private final Double ALPHA = new Double(0.85);
	private Integer cardinality;
	private Integer myTaskNumber;
	
	private Double percentage;

	private HashMap<LongWritable, Double> output;

	@Override
	protected void setup(Reducer<LongWritable, DoubleWritable, LongWritable, Node>.Context context) throws IOException, InterruptedException {

		cardinality = new Integer(context.getConfiguration().getInt("cardinality", 1));
		percentage = new Double(context.getConfiguration().getDouble("percentage", 0.1));
		total_loss = new Double(0.0);
		myTaskNumber = new Integer(0);
		output = new HashMap <LongWritable, Double>();
		
		FileSystem fs = FileSystem.get(context.getConfiguration());
		RemoteIterator<LocatedFileStatus> list = fs.listFiles(new Path("OUTPUT/loss-tmp"), true);

		while (list.hasNext()) {

			FSDataInputStream out = fs.open(list.next().getPath());
			BufferedReader bw = new BufferedReader(new InputStreamReader(out));

			total_loss += Double.parseDouble(bw.readLine());
			bw.close();
		}
	};

	@Override
	protected void reduce(LongWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

		myTaskNumber = context.getTaskAttemptID().getTaskID().getId();

		Double sum = new Double(0);

		for (DoubleWritable val : values) {
			sum += val.get();
		}

		sum = sum + (total_loss / cardinality.doubleValue());

		Double p = ((1 - ALPHA) / cardinality.doubleValue()) + ALPHA * sum;

		LongWritable storeKey = new LongWritable(key.get());
		output.put(storeKey, p);
	}
	
	
	protected void cleanup(org.apache.hadoop.mapreduce.Reducer<LongWritable,DoubleWritable,LongWritable,Node>.Context context) throws IOException ,InterruptedException {
		
		FileSystem fs = FileSystem.get(context.getConfiguration());
		int it = context.getConfiguration().getInt("iteration", 0);
		
		String number = "";
		
		for(int i =0; i< 5-myTaskNumber.toString().length(); i++) number+="0";
		
		number += myTaskNumber;

		Text tx = new Text();
		
		SequenceFile.Reader reader = new SequenceFile.Reader(context.getConfiguration(),Reader.file(new Path("OUTPUT/pr-"+it+".out/part-r-"+number)));
		
		LongWritable key = null;
		Node value = null;
		try {
			key = (LongWritable) reader.getKeyClass().newInstance();
			value = (Node) reader.getValueClass().newInstance();

		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		while (reader.next(key, value)){
			tx.clear();
			
			String[] rawOut = value.toString().split("\\t");
			
			StringBuilder s = new StringBuilder();
						
			Double p_old = Double.parseDouble(rawOut[0]);
			Double p_new = new Double(0.0);
			
			if(output.containsKey(key)){	
				p_new = output.get(key);
			}else{
				p_new = (((1-ALPHA) / cardinality) + (ALPHA) *(total_loss/cardinality.doubleValue()));
			}
			
			if (Math.abs(p_new - p_old) > (percentage / cardinality)) {
				try {
					fs.createNewFile(new Path("OUTPUT/convergence"));
				} catch (IOException ioe) {
				}
			}
			
			value.setPagerank(p_new);
			context.write(key, value);

		}
		reader.close();
		
	};

}
