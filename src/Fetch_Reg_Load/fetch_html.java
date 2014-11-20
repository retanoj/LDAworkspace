package Fetch_Reg_Load;

import java.util.*;
import java.util.concurrent.*;
import java.lang.Thread;
import java.sql.*;

import org.postgresql.util.PSQLException;

import de.tototec.cmdoption.CmdOption;
import de.tototec.cmdoption.CmdlineParser;
import de.tototec.cmdoption.CmdlineParserException;


class dbLoad extends Thread{
	int start_pos;
	int step;
	String table_from;
	String table_to;
	Thread[] tpool;
	
	LinkedBlockingQueue qUndo;
	LinkedBlockingQueue qDone;
	Connection conn;
	Statement stmt;
	
	private void insert_db(int step) throws SQLException{
		String insert_sql = "";
		while(step-- != 0){
			Data t = (Data) this.qDone.poll();
			insert_sql += String.format("insert into %s (id, data_voc) values(%d, '%s');", table_to, t.id, t.data_voc);
		}
		stmt.execute(insert_sql);
	}
	
	private boolean threadAllDead(){
		for(int i=0; i<tpool.length; i++){
			if(tpool[i].isAlive())
				return false;
		}
		return true;
	}
	
	long count = 0;
	public dbLoad(LinkedBlockingQueue<Data> qUndo, LinkedBlockingQueue<Data> qDone, String table_from, String table_to, int start_pos, int step, Thread[] t){
		this.start_pos = start_pos;
		this.step = step;
		this.table_from = table_from;
		this.table_to = table_to;
		this.tpool = t;
		this.qUndo = qUndo;
		this.qDone = qDone;
		
		this.conn = ConnUtil.getConn();
		this.count += start_pos;
	}
	
	@Override
	public void run(){
		try {
			stmt = conn.createStatement();
			
			int left = start_pos;
			double tBegin = System.currentTimeMillis();
			boolean select_rs_flag = true;
			
			System.out.println("\033[1;31;40m dbLoad thread is running... \033[0m");
			while(select_rs_flag){
				//select part into undo Queue
				if(qUndo.size() < step *5){
					select_rs_flag = false;
					String select_sql = String.format("select id, log_data1 from %s where id>=%d and id<%d;", table_from, left, left+step);
					ResultSet select_rs = stmt.executeQuery(select_sql);
					
					if (select_rs.next()){
						select_rs_flag = true;
						while(select_rs.next()){
							Data t = new Data();
							t.id = select_rs.getInt("id");
							t.url = select_rs.getString("log_data1");
							t.data_voc = "";
							qUndo.offer(t);
						}
						left += step;
					}
				}
				
				Thread.sleep(500);
				
				//insert part from done Queue
				if (qDone.size() > step){
					insert_db(step);
					
					count += step;
					double tEnd = System.currentTimeMillis();
					System.out.println(String.format("\033[1;33;40m until now %d records done. Cost %fs per %d records \033[0m", count, (tEnd-tBegin)/1000, step));
					tBegin = tEnd;
				}
				System.out.println(String.format("\033[1;36;40m undoQueue size is %d, doneQueue size is %d \033[0m", qUndo.size(), qDone.size()));
			}
			
			while(!threadAllDead()){
				if(qDone.size() > step){
					insert_db(step);
				}else{
					Thread.sleep(2000);
				}
				
				System.out.println(String.format("\033[1;36;40m Out of DB; undoQueue size is %d, doneQueue size is %d \033[0m", qUndo.size(), qDone.size()));
			}
			insert_db(qDone.size());

			stmt.close();
			System.out.println("\033[1;31;40m dbLoad thread is done. \033[0m");
		} catch (PSQLException e) {
			e.printStackTrace();
		} catch (SQLException e){
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			try {
				conn.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
	}
}

public class fetch_html {
	int tFetchUrl_num;
	int tNum;
	int start_pos;
	int step;
	String table_from;
	String table_to;
	
	LinkedBlockingQueue<Data> qUndo;
	LinkedBlockingQueue<Data> qDone;
	RepeatData rept;
	
	private void start_dbload(){		
		qUndo = new LinkedBlockingQueue<Data>();
		qDone = new LinkedBlockingQueue<Data>();
		rept = new RepeatData();
		
		Thread[] t = new Thread[tFetchUrl_num];
		
		for(int i=0; i<tFetchUrl_num; i++){
			t[i] = new MyCrawlerThread(qUndo, qDone, rept);
		}
		
		(new dbLoad(qUndo, qDone, table_from, table_to, start_pos, step, t)).start();
		for(int i=0; i<tFetchUrl_num; i++){
			t[i].start();
		}
	}
	
	
	public static void main(String[] args) { 
		Config config = new Config();
		CmdlineParser cp = new CmdlineParser(config);
		
		cp.setProgramName("seg_html");
		try{
			cp.parse(args);
		} catch (CmdlineParserException e){
			System.err.println("Error: " +e.getLocalizedMessage());
			System.exit(1);
		}
		
		if(config.help){
			cp.usage();
			System.exit(0);
		}
		
		fetch_html m = new fetch_html();
		m.table_from = config.table_from;
		m.table_to = config.table_to;
		m.start_pos = config.start_pos;
		m.step = config.step;
		m.tFetchUrl_num = config.thread_num;
		m.start_dbload();
	}
	
	public static class Config {
		@CmdOption(names = {"--help", "-h"}, description = "show usage.", isHelp=true)
		public boolean help;
		
		@CmdOption(names = {"--db_table_from", "-tfrom"}, args = {"[xxx_table]"}, description = "表名,数据源", minCount=1, maxCount=-1)
		public String table_from;
		
		@CmdOption(names = {"--db_table_to", "-tto"}, args = {"[xxx_table]"}, description = "表名,数据目的地", minCount=1, maxCount=-1)
		public String table_to;
		
		@CmdOption(names = {"--thread_num", "-tn"}, args = {"[200]"}, description = "线程数量，默认200", maxCount=-1)
		public Integer thread_num = 200;
		
		@CmdOption(names = {"--start_pos"}, args = {"[0]"}, description = "起始位置,默认为0", maxCount=-1)
		public Integer start_pos = 0;
		
		@CmdOption(names = {"--step"}, args = {"[500]"}, description = "查询步长，默认为500")
		public Integer step = 500;
		
	}


}
