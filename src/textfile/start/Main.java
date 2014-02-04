package textfile.start;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.util.ToolRunner;

import textfile.drivers.GraphTransformationIMCRun;
import textfile.drivers.PageRankIMCRun;
import textfile.drivers.PageRankRun;
import textfile.drivers.PageRankSchimmyIMCRun;
import textfile.drivers.PageRankSchimmyRun;
import textfile.utils.StopWatch;

enum MethodType {
    SCHIMMY, NO_OPTIM, IMC, IMC_SCHIMMY
}

/**
 * Classe principale di bootstrap di tutte le attività. 
 * 
 * @author Nicolo' Marchi, Fabio Pettenuzzo
 *
 */
public class Main {
	
	static Configuration conf;
	static FileSystem fs;
	
	static StopWatch all = new StopWatch();
	static StopWatch work = new StopWatch();
	
	/**
	 * Metodo main
	 * 
	 * @param args gli argomenti passati da linea di comando
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		conf = new Configuration();
		fs = FileSystem.get(conf);
		fs.setWorkingDirectory(new Path("/output/MaPe"));
		
		conf.setBoolean("mapreduce.map.output.compress", true);
		
		conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.Lz4Codec");
		
		if(args.length < 5){
			System.out.println("");
			System.out.println("Welcome to the PageRank Hadoop Job.");
			System.out.println("");
			System.out.println("Uncorrect number of parameters.");
			System.out.println("Usage: PageRank_MaPe <num_reducers> <input_path> <output_path> <optimization> <percentage_convergence>");
			System.out.println("");

			System.exit(0);
		}
		
		try {
			String[] name = args[1].split("\\/");
		    System.setOut(new PrintStream(new File("Test-"+name[name.length-1]+"-"+args[3]+"-"+args[4]+".txt")));
		} catch (Exception e) {
		     e.printStackTrace();
		}
		
		System.out.println();
		System.out.println("Starting test with "+args[1]+" and "+args[3]+"-"+args[4]);
		System.out.println("-----------------------");
		System.out.println();
		
		all.start();
		
		conf.setDouble("percentage", Math.abs(Double.parseDouble(args[4])));
		
		work.start();
		execCardinality(args);
		work.stop();
		
		System.out.println("Time for graph transformation: "+work.getElapsedTimeSecs());
		System.out.println("-----------------------");

		readCardinalityAndMax();

		System.out.println();
		
		work.start();
		execPageRank(args, parseMethod(args[3]));
		work.stop();

		System.out.println("Time for total all iterations: "+work.getElapsedTimeSecs());
		System.out.println();
		
		all.stop();
		System.out.println("TotalTIME: " + all.getElapsedTimeSecs());
		System.out.println("-----------------------");
		System.out.println();
				
		System.out.flush();
		System.out.close();
		

			
	}

	
	/**
	 * Metodo per l'esecuzione del job per la trasformazione del grafo dalla sintassi dei file presenti su http://snap.stanford.edu/ alla sintassi utilizzata per il calcolo del PageRank.
	 * 
	 * @param args argomenti passati da linea di comando
	 * @throws Exception
	 */
	public static void execCardinality(String[] args) throws Exception {
		String[] prepareOpts = { args[1], args[0]};
		ToolRunner.run(conf, new GraphTransformationIMCRun(), prepareOpts);
	}

	/**
	 * Metodo per l'esecuzione del job di calcolo del PageRank.
	 * 
	 * @param args argomenti contenenti i path dei file di input e output
	 * @param optimizations ottimizzazioni scelte dall'utente
	 * @throws IOException se vi sono eventuali errori con l'utilizzo dei file
	 * @throws Exception
	 */
	public static void execPageRank(String[] args, MethodType optimizations) throws IOException, Exception {
		
		Path prev = null, curr = null;
		String[] opts = null;
		
		boolean converged = true;
		
		for(int i = 0; converged; i++){
			
			converged = false;
			
			conf.setInt("iteration", i);
			
			prev = new Path("OUTPUT/"+"pr-" + i + ".out");
			curr = new Path("OUTPUT/" + "pr-" + (i+1) + ".out");
			
			if(i < 1){
				setIntervalsReds(prev, Integer.parseInt(args[0]));
			}
			
			opts = new String[]{ prev.toString(), curr.toString(), args[0]};
			
			StopWatch tmp = new StopWatch();
			tmp.start();

			switch(optimizations) {
				case SCHIMMY :
					ToolRunner.run(conf, new PageRankSchimmyRun(), opts);
					break;
				case IMC :
					ToolRunner.run(conf, new PageRankIMCRun(), opts);
					break;
				case IMC_SCHIMMY :
					ToolRunner.run(conf, new PageRankSchimmyIMCRun(), opts);
					break;
				default :
					ToolRunner.run(conf, new PageRankRun(), opts);
			}
			
			tmp.stop();
			System.out.println("- Time for only the job without files num="+i+": "+tmp.getElapsedTimeSecs());
			System.out.println();

	
			fs.delete(prev, true);
	
			// Controlla se è a convergenza o no
			if(fs.exists(new Path("OUTPUT/convergence"))){
				converged = true;
				fs.delete(new Path("OUTPUT/convergence"), true);
			}
			
			fs.delete(new Path("OUTPUT/loss-tmp"), true);
		
		}
		
		fs.rename(curr, new Path(args[2]));
		
	}

	/**
	 * Metodo per la lettura del file contentente il valore di cardinalità e della chiave massima.
	 * 
	 * @throws FileNotFoundException se il file della cardinalità non è presente
	 * @throws IOException se vi sono errori con l'utilizzo del file
	 */
	public static void readCardinalityAndMax() throws FileNotFoundException, IOException {
		
		RemoteIterator<LocatedFileStatus> list = fs.listFiles(new Path("OUTPUT/cardinality"), true);
		Integer cardinality = new Integer(0);
		
		while (list.hasNext()) {
	
			FSDataInputStream out = fs.open(list.next().getPath());
			BufferedReader bw = new BufferedReader(new InputStreamReader(out));
	
			String[] s = bw.readLine().split("\\t");
			cardinality += Integer.parseInt(s[0]);
			
			bw.close();
			out.close();
		}
	
		conf.setInt("cardinality", cardinality);
		
		fs.delete(new Path("OUTPUT/cardinality"), true);
	
	}

	private static MethodType parseMethod(String string) {
		if(string.equalsIgnoreCase("schimmy"))				return MethodType.SCHIMMY;
		else if(string.equalsIgnoreCase("imc"))				return MethodType.IMC;
		else if(string.equalsIgnoreCase("imc-schimmy"))		return MethodType.IMC_SCHIMMY;
															return MethodType.NO_OPTIM;
	}

	/**
	 * Metodo per il calcolo delle chiavi limite di ogni partizione di output.
	 * 
	 * @param file cartella contenente i file di output
	 * @param numReds numero dei reducers
	 * @return configurazione
	 * @throws IOException se vi sono errori con l'utilizzo dei file
	 */
	public static Configuration setIntervalsReds(Path file, int numReds) throws IOException {
		
	    String tmp, firstKey, secondKey = null;
		FSDataInputStream in;
		BufferedReader b;
		
		FileStatus[] status = FileSystem.get(conf).listStatus(file);
		for (int i=0;i<status.length;i++){
			if(status[i].getLen() == 0){
				continue;
			}
			
			in = FileSystem.get(conf).open(status[i].getPath());
		    b = new BufferedReader(new InputStreamReader(in));
		    
		    firstKey= b.readLine();
		    while((tmp = b.readLine())!= null){
		    	secondKey = tmp;
		    }
    		conf.set("interval-"+(i-1), firstKey.split("\\t")[0]+"-"+secondKey.split("\\t")[0]);

		}

		return conf;
	}

}
