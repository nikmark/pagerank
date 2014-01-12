package start;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.util.ToolRunner;

import drivers.CardinalityIMCRun;
import drivers.PageRankObjectRun;
import drivers.PageRankSchimmyRun;

enum MethodType {
    SCHIMMY, NORMAL
}

public class Main {
	
	static Configuration conf;
	static FileSystem fs;
	static String check = "";
	
	public static void main(String[] args) throws Exception {

		conf = new Configuration();
		fs = FileSystem.get(conf);
		fs.setWorkingDirectory(new Path("output/marchipettenuzzo"));
		
		if(args.length >= 5){
			check = args[4];
		}
		
		switch(parseMethod(check)){
		
			case SCHIMMY:
					sequenceSchimmy(args);
				break;
			
			case NORMAL:
					sequenceNormal(args);
				break;
				
			default: 
					sequenceNormal(args);
				break;
			
		}

	}

	public static void sequenceNormal(String[] args) throws Exception {
		
		////////////////////////////////////////	INIZIO	//////////////////////////////////////////////
		
		execCardinality(args);

		readCardinalityAndMax();

		execPageRank(args, false);
		
		////////////////////////////////////////	FINE	//////////////////////////////////////////////
		
		
	}

	public static void sequenceSchimmy(String[] args) throws Exception {
		
		execCardinality(args);
		
		////////////////////////////////////////	INIZIO	//////////////////////////////////////////////

		readCardinalityAndMax();
		
		// probabile job di sorting
		// discutiamo domani di questo
		
		//job di pagerank
		execPageRank(args, true);

		
	}

	public static void execCardinality(String[] args) throws Exception {
		String[] prepareOpts = { args[1], args[0]};
		ToolRunner.run(conf, new CardinalityIMCRun(), prepareOpts);
	}

	public static void execPageRank(String[] args, boolean schimmy) throws IOException, Exception {
		
		Path prev = null, curr = null;
		String[] opts = null;
		
		boolean converged = true;
		
		for(int i = 0; i < Integer.parseInt(args[3]) && converged; i++){
			
			converged = false;
	
			// Cancella temp files per la massa pers
			Path filenamePath = new Path("loss-data");
			if(fs.exists(filenamePath)){
				fs.delete(filenamePath, true);
			}
			
			
			prev = new Path("OUTPUT/"+"pr-" + i + ".out");
			curr = new Path("OUTPUT/" + "pr-" + (i+1) + ".out");
			
			opts = new String[]{ prev.toString(), curr.toString(), args[0]};
			
			if(schimmy){
				ToolRunner.run(conf, new PageRankSchimmyRun(), opts);
			}else{
				ToolRunner.run(conf, new PageRankObjectRun(), opts);
			}
	
//			fs.delete(prev, true);
	
			// Controlla se è a convergenza o no
			if(fs.exists(new Path("convergence"))){
				converged = true;
				fs.delete(new Path("convergence"), true);
			}
			
			fs.delete(new Path("loss-tmp"), true);
	
			System.out.println("Stampiamo la I: "+i);
	
		}
		
		fs.rename(curr, new Path(args[2]));
		
	}

	private static void readCardinalityAndMax() throws FileNotFoundException, IOException {
		
		RemoteIterator<LocatedFileStatus> list = fs.listFiles(new Path("cardinality"), true);
		Integer cardinality = new Integer(0);
		Integer max = new Integer(0);
		
		while (list.hasNext()) {
	
			FSDataInputStream out = fs.open(list.next().getPath());
			BufferedReader bw = new BufferedReader(new InputStreamReader(out));
	
			String[] s = bw.readLine().split("\\t");
			cardinality += Integer.parseInt(s[0]);
			if(max <= Integer.parseInt(s[1])){
				max = Integer.parseInt(s[1]);
			}
			bw.close();
			out.close();
		}
	
		
		conf.setInt("cardinality", cardinality);
		conf.setInt("max", max);
		
		fs.delete(new Path("cardinality"), true);
	
	}

	private static MethodType parseMethod(String string) {
		if(string.equalsIgnoreCase("schimmy")){
			return MethodType.SCHIMMY;
		}
		return MethodType.NORMAL;
	}

//	private static Configuration setIntervalsReds(Path file, Configuration conf, Integer d, int numReds) throws IOException {
//		
//		
//	    int elemForInterval = (int) Math.floor(d.intValue() / numReds);    
//
//	    String tmp;
//		FSDataInputStream in;
//		BufferedReader b;
//		
//		FileStatus[] status = FileSystem.get(conf).listStatus(file);
//		for (int i=0;i<status.length;i++){
//			in = FileSystem.get(conf).open(status[i].getPath());
//		    b = new BufferedReader(new InputStreamReader(in));
//		}
//
//		
//		int firstValue = 0;
//		int secondValue = 0;
//		int i = 0, k = 0 ;
//		while((tmp = b.readLine()) != null){
//			
//			i++;
//			secondValue = Integer.parseInt(tmp.split("\\t")[0]);
//			if(i == elemForInterval && k < numReds-1){
//	    		conf.set("interval-"+k, firstValue+"-"+secondValue);
//				firstValue = secondValue +1; 
//				k++;
//				i = 0;
//			}
//		
//		}
//		conf.set("interval-"+k, firstValue+"-"+secondValue);
//		
//		return conf;
//	}

}
