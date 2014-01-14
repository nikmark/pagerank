package reducers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import utils.Data;

public class PageRankSchimmyReducer extends Reducer<IntWritable, DoubleWritable, Text, Text> {

	private Double total_loss;
	private final Double ALPHA = new Double(0.15);
	private Integer cardinality;
	private Integer myTaskNumber = new Integer(0);
	
	private Double percentage;

	private ConcurrentHashMap <IntWritable, Double> output;

	protected void setup(Reducer<IntWritable, DoubleWritable, Text, Text>.Context context)
			throws IOException, InterruptedException {

		cardinality = context.getConfiguration().getInt("cardinality", 1);
		percentage = context.getConfiguration().getDouble("percentage", 0.1);
		total_loss = 0.0;
		output = new ConcurrentHashMap <IntWritable, Double>();
		
		FileSystem fs = FileSystem.get(context.getConfiguration());
		RemoteIterator<LocatedFileStatus> list = fs.listFiles(new Path("OUTPUT/loss-tmp"), true);

		while (list.hasNext()) {

			FSDataInputStream out = fs.open(list.next().getPath());
			BufferedReader bw = new BufferedReader(new InputStreamReader(out));

			total_loss += Double.parseDouble(bw.readLine());
			bw.close();
		}
//		System.out.println("perdita totale nel reducer: "+total_loss);

				
	};

	@Override
	protected void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {

		 myTaskNumber = context.getTaskAttemptID().getTaskID().getId();

		
		Double sum = new Double(0);

		for (DoubleWritable val : values) {

			sum += val.get();
		}

		sum = sum + (total_loss / cardinality);

		Double p = (ALPHA / cardinality) + (1 - ALPHA) * sum;

		IntWritable storeKey = new IntWritable(key.get());
		System.out.println("metodo reduce: "+key.get()+" "+p);
		output.put(storeKey, p);
		System.out.println("output dentro il reduce: "+output.toString());

	}
	
	
	protected void cleanup(org.apache.hadoop.mapreduce.Reducer<IntWritable,DoubleWritable,Text,Text>.Context context) throws IOException ,InterruptedException {
		
		FileSystem fs = FileSystem.get(context.getConfiguration());
		int it = context.getConfiguration().getInt("iteration", 0);
		
		String number = "";
		for(int i =0; i< 5-myTaskNumber.toString().length(); i++) number+="0";
		
		number += myTaskNumber;

		FSDataInputStream out = fs.open(new Path("OUTPUT/pr-"+it+".out/part-r-"+number));
		BufferedReader bw = new BufferedReader(new InputStreamReader(out));
		
		String line, tmp;
		System.out.println("reimission: "+(total_loss/cardinality));

		while((tmp = bw.readLine())!=null){
			line=tmp;
			String[] rawOut = line.split("\\t");
			
			StringBuilder s = new StringBuilder();
						
			Double p_old = Double.parseDouble(rawOut[1]);
			Double p_new = new Double(0.0);
			
			System.out.println("raw: "+rawOut[0]);
			System.out.println("output: "+output.toString());
			System.out.println("line: "+line);
			if(output.containsKey(new IntWritable(Integer.parseInt(rawOut[0])))){				
				p_new = output.get(new IntWritable(Integer.parseInt(rawOut[0])));
			}else{
				p_new = ((total_loss/cardinality.doubleValue()));
			}
			
			
			if (Math.abs(p_new - p_old) > (percentage / cardinality)) {
//				fs = FileSystem.get(context.getConfiguration());
				try {
					fs.createNewFile(new Path("OUTPUT/convergence"));
				} catch (IOException ioe) {
				}
			}
			
			System.out.println("red chiave:"+rawOut[0]+" valore:"+p_new);
			s.append(p_new);
			
			if(rawOut.length > 2){
				s.append("\t");
				s.append(line.substring(rawOut[0].length()+rawOut[1].length()+2));
			}
			
			context.write(new Text(new Text(rawOut[0])), new Text(s.toString()));
		}
		
	};

}
