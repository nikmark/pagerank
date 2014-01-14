package reducers;

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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer per il calcolo del PageRank con lo Schimmy design pattern. Tutti i calcoli di convergenza ecc, vengono eseguiti nel cleanup().
 * 
 * @author Nicolò Marchi, Fabio Pettenuzzo
 *
 */	
public class PageRankSchimmyReducer extends Reducer<LongWritable, DoubleWritable, Text, Text> {

	private Double total_loss;
	private final Double ALPHA = new Double(0.15);
	private Integer cardinality;
	private Integer myTaskNumber;
	
	private Double percentage;

	private HashMap<LongWritable, Double> output;

	@Override
	protected void setup(Reducer<LongWritable, DoubleWritable, Text, Text>.Context context) throws IOException, InterruptedException {

		cardinality = context.getConfiguration().getInt("cardinality", 1);
		percentage = context.getConfiguration().getDouble("percentage", 0.1);
		total_loss = 0.0;
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

		sum = sum + (total_loss / cardinality);

		Double p = (ALPHA / cardinality) + (1 - ALPHA) * sum;

		LongWritable storeKey = new LongWritable(key.get());
		output.put(storeKey, p);
	}
	
	
	protected void cleanup(org.apache.hadoop.mapreduce.Reducer<LongWritable,DoubleWritable,Text,Text>.Context context) throws IOException ,InterruptedException {
		
		FileSystem fs = FileSystem.get(context.getConfiguration());
		int it = context.getConfiguration().getInt("iteration", 0);
		
		String number = "";
		
		for(int i =0; i< 5-myTaskNumber.toString().length(); i++) number+="0";
		
		number += myTaskNumber;

		FSDataInputStream out = fs.open(new Path("OUTPUT/pr-"+it+".out/part-r-"+number));
		BufferedReader bw = new BufferedReader(new InputStreamReader(out));
		
		String line, tmp;

		while((tmp = bw.readLine())!=null){
			line=tmp;
			String[] rawOut = line.split("\\t");
			
			StringBuilder s = new StringBuilder();
						
			Double p_old = Double.parseDouble(rawOut[1]);
			Double p_new = new Double(0.0);

			if(output.containsKey(new LongWritable(Integer.parseInt(rawOut[0])))){				
				p_new = output.get(new LongWritable(Integer.parseInt(rawOut[0])));
			}else{
				p_new = ((ALPHA / cardinality) + (1 - ALPHA) *(total_loss/cardinality.doubleValue()));
			}
			
			if (Math.abs(p_new - p_old) > (percentage / cardinality)) {
				try {
					fs.createNewFile(new Path("OUTPUT/convergence"));
				} catch (IOException ioe) {
				}
			}
			
			s.append(p_new);
			
			if(rawOut.length > 2){
				s.append("\t");
				s.append(line.substring(rawOut[0].length()+rawOut[1].length()+2));
			}
			
			context.write(new Text(new Text(rawOut[0])), new Text(s.toString()));
		}
		
	};

}
