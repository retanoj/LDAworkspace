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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import de.tototec.cmdoption.CmdOption;
import de.tototec.cmdoption.CmdlineParser;
import de.tototec.cmdoption.CmdlineParserException;


public class GlobalTF {
	private Map<String, Integer> hashMap = new HashMap<String, Integer>();
	private Map<String, Integer> idMap = new HashMap<String, Integer>();
	private Set<String> hfWordSet = new HashSet<String>();//high-frequency word set
	private final String HFWORD = "gaopin.txt";		//high-frequency word list
	private final String FILE = "cipin.txt";
	private String DOCUMENTS = "documents.txt";
	private final String DOCUMENTS_TYPE = "STRING"; // STRING
	public String database = "";
	public int step = 10000;
	public int MIN;
	public int MAX;
	
	//init hfWordSet
	private void initHFWordSet(){
		File file = new File(HFWORD);
		try {
			InputStreamReader inReader = new InputStreamReader(new FileInputStream(file), "UTF-8");
			BufferedReader reader = new BufferedReader(inReader);
			while(true){
				String line = reader.readLine();
				if(line == null)
					break;
				hfWordSet.add(line.trim());
			}
			reader.close();
		} catch (IOException e) {
			System.out.println("Error:" + e.getLocalizedMessage());
			return ;
		}	
	}
	
	public void count(){
		System.out.println("Counting start....");
		Connection conn = ConnUtil.getConn();
		Statement stmt = null;
		int id = 0;
		try {
			stmt = conn.createStatement();
			int left = 0;
			while (true) {
				String select_sql = String
						.format("select seg_id,data_seg from %s where seg_id>=%d and seg_id<%d;",
								database, left, left + step);
				ResultSet select_rs = stmt.executeQuery(select_sql);
				
				//if resultset is empty
				boolean isEmpty = true;
				
				while (select_rs.next()) {
					isEmpty = false;
					String content = select_rs.getString("data_seg");
					StringTokenizer st = new StringTokenizer(content, " ");
					while (st.hasMoreTokens()) {
						String key = st.nextToken();
						if (hashMap.containsKey(key)) {
							Integer freq = hashMap.get(key);
							freq++;
							hashMap.put(key, freq);
						} else {
							hashMap.put(key, new Integer(1));
							idMap.put(key, id);
							id++;
						}
					}
				}				
				if(isEmpty == true)
					break;	
				left = left + step;
				System.out.println(String.valueOf(left) + " records done.");
			}
			System.out.println("Counting done.");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	public void save(){
		System.out.println("Saving start...");
		ArrayList<DictRecord> dictRecords = new ArrayList<DictRecord>();
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
				DictRecord record = new DictRecord(id, key, value);
				dictRecords.add(record);			
				//id++;
			}
			Collections.sort(dictRecords, new DictComp());
			for (int i = 0; i < dictRecords.size(); i++) {
				DictRecord record = dictRecords.get(i);
				writer.write(String.valueOf(i) + " " + record.word
						+ " " + String.valueOf(record.freq) + "\n");
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
		Iterator it = hashMap.entrySet().iterator();
		HashMap<String, Integer> tMap = new HashMap<>();
		idMap.clear();
		int id = 0;
		while(it.hasNext()){
			Map.Entry<String, Integer> entry = (Map.Entry<String, Integer>) it.next();
			String key = entry.getKey();
			Integer value = entry.getValue();
			if(value > MIN && value < MAX && !hfWordSet.contains(key)){
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
			int process = 0;
			RandomAccessFile raf = new RandomAccessFile(DOCUMENTS, "rw");
			int left = 0;
			while (true) {
				boolean isEmpty = true;
				String select_sql = String
						.format("select seg_id,data_seg from %s where seg_id>=%d and seg_id<%d;",
								database, left, left + step);
				ResultSet select_rs = stmt.executeQuery(select_sql);
				raf.write("          \n".getBytes("UTF-8"));
				while (select_rs.next()) {
					isEmpty = false;
					StringBuilder sBuilder = new StringBuilder("");
					String content = select_rs.getString("data_seg");
					StringTokenizer st = new StringTokenizer(content, " ");
					while (st.hasMoreTokens()) {
						String key = st.nextToken();
						if (hashMap.containsKey(key)) {
							String valueString = "";
							if (DOCUMENTS_TYPE.equals("NUM")) {
								valueString = String.valueOf(idMap.get(key));
							}
							if (DOCUMENTS_TYPE.equals("STRING")) {
								valueString = key;
							}
							sBuilder.append(valueString);
							sBuilder.append(" ");
						}
					}
					if (sBuilder.length() == 0)
						continue;
					sBuilder.append("\n");
					raf.write(sBuilder.toString().getBytes("UTF-8"));
					process++;
					if (process % 1000 == 0)
						System.out.println(String.valueOf(process)
								+ " records done.");
				}
				if(isEmpty)
					break;
				left += step;
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
		long startTime=System.currentTimeMillis(); 
		Config config = new Config();
		CmdlineParser cp = new CmdlineParser(config);
		cp.setResourceBundle(GlobalTF.class.getPackage().getName()
				+ ".Messages", GlobalTF.class.getClassLoader());
		cp.setProgramName("GlobalTF");
		try {
			cp.parse(args);

		} catch (CmdlineParserException e) {
			System.err.println("Error: " + e.getLocalizedMessage());
			System.exit(1);
		}
		if (config.help) {
			cp.usage();
			System.exit(0);
		}
		GlobalTF tf = new GlobalTF();
		tf.database = config.db;
		tf.step = config.step;
		tf.MAX = config.max;
		tf.MIN = config.min;
		if(config.cipin){
			tf.count();
			tf.save();
			//System.exit(0);
		}
		if(config.documents){
			tf.initHFWordSet();
			tf.load();
			tf.filter();
			tf.documentsFilter();
			//System.exit(0);
		}
		long endTime=System.currentTimeMillis();
		System.out.println(String.valueOf(endTime - startTime) + " millisecond used.");
	}
	public static class Config {
	    @CmdOption(names = {"--help", "-h"}, description = "Show this help.", isHelp = true)
	    public boolean help;

	    
	    @CmdOption(names = {"--database", "-d"}, args = {"db"},  description = "表名", minCount = 1, maxCount = -1)
	    public String db;
	    
	    @CmdOption(names = {"--min"}, args = {"min"}, description = "低频过滤阈值，默认为10", maxCount = -1)
	    public Integer min = 10;
	    
	    @CmdOption(names = {"--max"}, args = {"max"}, description = "高频词过滤阈值，默认为Integer.MAX_VALUE", maxCount = -1)
	    public Integer max = Integer.MAX_VALUE;
	    
	    @CmdOption(names = {"--cipin"}, description = "计算词频，输出到cipin.txt中", maxCount = -1)
	    public boolean cipin = false;
	    
	    @CmdOption(names = {"--documents"}, description = "词频过滤，输出到documents.txt中", maxCount = -1)
	    public boolean documents = false;
	    
	    @CmdOption(names = {"--step"}, args = {"step"}, description = "数据库查询递增步长，默认10000", maxCount = -1)
	    public Integer step = 10000;
	        
	}
	public class DictRecord{
		public int id;
		public String word;
		public int freq;
		public DictRecord(int i1, String i2, int i3){
			id = i1;
			word = i2;
			freq = i3;
		}
	}
	public class DictComp implements Comparator<DictRecord>{

		@Override
		public int compare(DictRecord o1, DictRecord o2) {
			return o1.freq - o2.freq;
		}	
	}
}
