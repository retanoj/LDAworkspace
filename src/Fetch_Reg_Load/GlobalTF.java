package Fetch_Reg_Load;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.nio.channels.WritableByteChannel;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;


public class GlobalTF {
	private Map<String, Integer> hashMap = new HashMap<String, Integer>();
	private Map<String, Integer> idMap = new HashMap<String, Integer>();
	private final String FILE = "cipin.txt";
	private final String STOPWORD = "stopword.txt";
	private final String DOCUMENTS = "documents_num.txt";
	private final String DOCUMENTS_TYPE = "NUM"; // STRING
	
	
	public void count(){
		Connection conn = ConnUtil.getConn();
		Statement stmt = null;
		int id = 0;
		try {
			stmt = conn.createStatement();
			String select_sql = String.format("select data_seg from t20140702_100w_seg;");
			ResultSet select_rs = stmt.executeQuery(select_sql);
			int process = 0;
			while(select_rs.next()){
				String content = select_rs.getString("data_seg");
				StringTokenizer st = new StringTokenizer(content, " ");
				process++;
				if(process % 10000 == 0)
					System.out.println(String.valueOf(process) + " records done.");
				while(st.hasMoreTokens()){
					String key = st.nextToken();
					if(hashMap.containsKey(key)){
						Integer freq = hashMap.get(key);
						freq++;
						hashMap.put(key, freq);
					}
					else{
						hashMap.put(key, new Integer(1));
						idMap.put(key, id);
						id++;
					}
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	public void save(){
		System.out.println("Saving start...");
		try {
			File file = new File(FILE);
			file.createNewFile();
			Writer writer = new OutputStreamWriter(new FileOutputStream(file), "UTF-8");
			Iterator it = hashMap.entrySet().iterator();
			//int id = 0;
			while(it.hasNext()){
				Map.Entry<String, Integer> entry = (Map.Entry<String, Integer>) it.next();
				String key = entry.getKey();
				Integer value = entry.getValue();
				int id = idMap.get(key);
				writer.write(String.valueOf(id) + " " + key + " " + String.valueOf(value) + "\n");
				//id++;
			}
			writer.close();
			
		} catch (IOException e) {
			return ;
		}
		System.out.println("Saving done.");
		System.out.println("Save to file " + FILE + ".");
	}
	public void load(){
		System.out.println("load start...");
		hashMap.clear();
		idMap.clear();
		File file = new File(FILE);
		try {
			Reader reader = new InputStreamReader(new FileInputStream(file), "UTF-8");
			BufferedReader bReader = new BufferedReader(reader);			
			int c = 0;
			while(true){
				String line = bReader.readLine();
				if(line == null)
					break;
				String[] parts = line.split(" ");
				if(parts.length != 3){
					continue;
				}				
				hashMap.put(parts[1], Integer.valueOf(parts[2]));
				idMap.put(parts[1], Integer.valueOf(parts[0]));
				c++;
				if(c % 1000 == 0)
					System.out.println(String.valueOf(c));
			}
			bReader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("load done.");
	}
	public void filter(){
		System.out.println("filter start....");
		File file = new File(STOPWORD);
		HashSet<String> stopword = new HashSet<String>();
		try {
			Reader reader = new InputStreamReader(new FileInputStream(file), "UTF-8");
			BufferedReader bReader = new BufferedReader(reader);
			String line = bReader.readLine();			
			while(line != null){
				stopword.add(line.trim());
				line = bReader.readLine();
			}
			bReader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		Iterator it = hashMap.entrySet().iterator();
		HashMap<String, Integer> tMap = new HashMap<>();
		idMap.clear();
		int id = 0;
		while(it.hasNext()){
			Map.Entry<String, Integer> entry = (Map.Entry<String, Integer>) it.next();
			String key = entry.getKey();
			Integer value = entry.getValue();
			if(!stopword.contains(key) && value > 10 && key.length() > 1){
				tMap.put(key, value);
				idMap.put(key, id);
				id++;
			}			
		}
		hashMap = tMap;
		System.out.println("filter done.");
	}
	
	public void documentsFilter(){
		System.out.println("documents processing start...");
		Connection conn = ConnUtil.getConn();
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			String select_sql = String.format("select data_seg from t20140702_100w_seg;");
			ResultSet select_rs = stmt.executeQuery(select_sql);
			//File file = new File(DOCUMENTS);
			//file.createNewFile();
			//Writer writer = new OutputStreamWriter(new FileOutputStream(file), "UTF-8");
			RandomAccessFile raf = new RandomAccessFile(DOCUMENTS, "rw");
			int process = 0;	
			raf.write("          \n".getBytes("UTF-8"));
			while(select_rs.next()){
				StringBuilder sBuilder = new StringBuilder("");
				String content = select_rs.getString("data_seg");
				StringTokenizer st = new StringTokenizer(content, " ");
				while(st.hasMoreTokens()){
					String key = st.nextToken();
					if(hashMap.containsKey(key)){
						String valueString = "";
						if(DOCUMENTS_TYPE.equals("NUM")){
							valueString = String.valueOf(idMap.get(key));
						}
						if(DOCUMENTS_TYPE.equals("STRING")){
							valueString = key;
						}
						//String valueString = String.valueOf(hashMap.get(key));
						//String valueString = String.valueOf(idMap.get(key));
						sBuilder.append(valueString);
						sBuilder.append(" ");
					}
				}
				if(sBuilder.length() == 0)
					continue;
				sBuilder.append("\n");
				//writer.write(sBuilder.toString());
				raf.write(sBuilder.toString().getBytes("UTF-8"));
				process++;
				if(process % 1000 == 0)
					System.out.println(String.valueOf(process) + " records done.");
			}
			raf.seek(0);
			raf.write(String.valueOf(process).getBytes("utf-8"));
			raf.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("documents processing done.");
	}
	public static void main(String[] args){
		GlobalTF tf = new GlobalTF();
		//tf.count();
		tf.load();
		tf.documentsFilter();
		//tf.filter();
		//tf.save();
	}
}
