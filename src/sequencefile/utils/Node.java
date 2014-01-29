package sequencefile.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
/**
 * Struttura dati per il salvataggio del nodo del grafo, contenente i valori di PageRank presente e precedente, e tutta la lista d'adiacenza del nodo stesso.
 * 
 * @author Nicolò Marchi, Fabio Pettenuzzo
 *
 */
public class Node implements WritableComparable {
	
	Text name;
	Double pagerank;	
	Double pagerankOld;
	Boolean vertex;

	List<String> adjacencyList;
	
	public Node (Node old){
		this.name=old.getName();
		this.pagerank = old.getPagerank();
		this.pagerankOld = old.getPagerankOld();
		this.vertex=old.isVertex();
		this.adjacencyList = old.getAdjacencyList();
	}

	public Node() {
		name = new Text("");
		pagerank = new Double(-1);	
		pagerankOld = new Double(-1);
		vertex = false;
		
		adjacencyList = new ArrayList<String>();

	}
	
	public void set(Node n){
		this.name = n.getName();
		this.pagerank = n.getPagerank();
		this.pagerankOld = n.getPagerank();
		this.vertex = n.isVertex();
		this.adjacencyList = n.getAdjacencyList();		
	}
	
	public void clear(){
		name = new Text("");
		pagerank = new Double(-1);	
		pagerankOld = new Double(-1);
		vertex = false;
		
		adjacencyList = new ArrayList<String>();
	}

	public Boolean isVertex() {
		return vertex;
	}

	public void setVertex(Boolean vertex) {
		this.vertex = vertex;
	}

	public Text getName() {
		return name;
	}

	public void setName(Text name) {
		this.name = name;
	}

	public Double getPagerank() {
		return pagerank;
	}

	public void setPagerank(Double pagerank) {
		this.pagerank = pagerank;
	}

	public Double getPagerankOld() {
		return pagerankOld;
	}

	public void setPagerankOld(Double pagerankOld) {
		this.pagerankOld = pagerankOld;
	}

	public List<String> getAdjacencyList() {
		return adjacencyList;
	}

	public void setAdjacencyList(List<String> adjacencyList) {
		this.adjacencyList = adjacencyList;
	}

	public Boolean getVertex() {
		return vertex;
	}

	public Double outputPageRank(){
		return pagerank / adjacencyList.size() ;
	}
	
	public String toString(){
		StringBuilder sb = new StringBuilder();
		sb.append(pagerank);
		
		for(String s : adjacencyList){
			sb.append("\t").append(s);
		}
		
		return sb.toString();
	}
	

	@Override
	public void readFields(DataInput din) throws IOException {
		
		name.set(din.readUTF());
		pagerank = din.readDouble();
		pagerankOld = din.readDouble();
		vertex = din.readBoolean();
		
		int size = din.readInt();
		
		if(size == -1){
			adjacencyList = new ArrayList<String>();
		}else{
			adjacencyList = new ArrayList<String>();

		}
		for(int i = 0; i < size; i++){
			adjacencyList.add(din.readUTF());
		}
		
		
	}

	@Override
	public void write(DataOutput dou) throws IOException {

		dou.writeUTF(name.toString());
		dou.writeDouble(pagerank);
		dou.writeDouble(pagerankOld);
		dou.writeBoolean(vertex);
		
		if(adjacencyList.size() > 0){
			dou.writeInt(adjacencyList.size());
			for(String s : adjacencyList){
				dou.writeUTF(s);
			}
		}else{
			dou.writeInt(-1);

		}
		
	}
	
	public byte[] serialize() throws IOException {
	    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
	    DataOutputStream dataOut = new DataOutputStream(bytesOut);
	    write(dataOut);

	    return bytesOut.toByteArray();
	  }

	  public static Node create(DataInput in) throws IOException {
	    Node m = new Node();
	    m.readFields(in);

	    return m;
	  }

	  public static Node create(byte[] bytes) throws IOException {
	    return create(new DataInputStream(new ByteArrayInputStream(bytes)));
	  }

	@Override
	public int compareTo(Object o) {
		Integer i = Integer.parseInt(this.getName().toString());
		Integer j = Integer.parseInt(((Node)o).getName().toString());

		return i.compareTo(j);
	}

}
