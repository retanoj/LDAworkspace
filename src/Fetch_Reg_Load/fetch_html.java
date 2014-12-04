package Fetch_Reg_Load;

import java.util.concurrent.*;
import java.lang.Thread;
import java.sql.*;

import org.postgresql.util.PSQLException;

import de.tototec.cmdoption.CmdOption;
import de.tototec.cmdoption.CmdlineParser;
import de.tototec.cmdoption.CmdlineParserException;


class db_insert extends Thread{
	int step;
	String table_to;
	Thread[] tpool;
	
	LinkedBlockingQueue qDone;
	Connection conn;
	Statement stmt;
	PreparedStatement stmt_insert;
	
	private void insert_db(int step) throws SQLException{
		while(step-- != 0){
			Data t = (Data) this.qDone.poll();
			//id用来唯一标识一条记录，id_html为表内标识
			stmt_insert.setInt(1, t.id);
			stmt_insert.setString(2, t.data_voc);
			stmt_insert.addBatch();
		}
		try{
			stmt_insert.executeBatch();
		}catch(BatchUpdateException e){
			System.out.println("BatchUpdateException occur: " +e.getUpdateCounts().length +" insert this time");
		}finally{
			conn.commit();
			stmt_insert.clearBatch();
		}
	}
	
	private boolean threadAllDead(){
		for(int i=0; i<tpool.length; i++){
			if(tpool[i].isAlive())
				return false;
		}
		return true;
	}
	
	long count = 0;
	public db_insert(LinkedBlockingQueue<Data> qDone, String table_to, int step, Thread[] t){
		this.step = step;
		this.table_to = table_to;
		this.tpool = t;
		this.qDone = qDone;
		
		try {
			this.conn = ConnUtil.getConn();
			conn.setAutoCommit(false);

			String insert_string = String.format("insert into %s (id, data_voc) values(?, ?)", table_to);
			stmt_insert = conn.prepareStatement(insert_string);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
	}
	
	@Override
	public void run(){
		try {			
			double tBegin = System.currentTimeMillis();
			double tEnd;
			//建表，如果不存在的话
			try{
				stmt = conn.createStatement();
				String create_table = String.format("create table %s("
													+ "id_html serial PRIMARY KEY,"
													+ "id int,"
													+ "data_voc text);"
													, table_to );
				
				stmt.execute(create_table);
				System.out.println("\033[1;31;40m create table done.. \033[0m");
			}catch(PSQLException e){
				System.out.println("\033[1;31;40m table already exists.. \033[0m");
			}finally{
				stmt.close();
			}
			
			//insert part from done Queue
			while(true){
				if(qDone.size() > step){
					insert_db(step);
					count += step;
					tEnd = System.currentTimeMillis();
					System.out.println(String.format("\033[1;33;40m until now %d records done. Cost %fs per %d records \033[0m", count, (tEnd-tBegin)/1000, step));
					System.out.println(String.format("\033[1;32;40m doneQueue size is %d \033[0m", qDone.size()));
					tBegin = tEnd;
				}else if(threadAllDead()){
					break;
				}else{
					Thread.sleep(500);
				}
			}
			insert_db(qDone.size());

			stmt_insert.close();
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

class db_query extends Thread{
	int start_pos;
	int step;
	String table_from;
	
	LinkedBlockingQueue qUndo;
	LinkedBlockingQueue qDone;
	Connection conn;
	Statement stmt;
	PreparedStatement stmt_query;
	
	long count = 0;
	public db_query(LinkedBlockingQueue<Data> qUndo,LinkedBlockingQueue<Data> qDone, String table_from, int start_pos, int step){
		this.start_pos = start_pos;
		this.step = step;
		this.table_from = table_from;
		this.qUndo = qUndo;
		this.qDone = qDone;
		this.count += start_pos;
		
		try {
			this.conn = ConnUtil.getConn();
						
			String query_string = String.format("select id, log_data1 from %s where id>=? and id<?", table_from);
			stmt_query = conn.prepareStatement(query_string);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
	}
	
	@Override
	public void run(){
		try {			
			int left = start_pos;
			boolean select_rs_flag = true;
			int old_step = step;
			
			while(select_rs_flag){
				//select part into undo Queue
				if(qUndo.size() < step *3){
					select_rs_flag = false;
					stmt_query.setInt(1, left);
					stmt_query.setInt(2, left+step);
					ResultSet select_rs = stmt_query.executeQuery();
					
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
				}else{
					Thread.sleep(500);
				}
				
				if(qDone.size() > old_step *10 && step >= old_step){
					step /= 10;
				}else if(qDone.size() < old_step && step < old_step){
					step *= 10;
				}
				
				System.out.println(String.format("\033[1;36;40m undoQueue size is %d \033[0m", qUndo.size()));
			}
			stmt_query.close();
			System.out.println("\033[1;31;40m db_query thread is done. \033[0m");
		} catch (PSQLException e) {
			e.printStackTrace();
		} catch (SQLException e){
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
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
		
		(new db_query(qUndo, qDone, table_from, start_pos, step*2)).start();
		(new db_insert(qDone, table_to, step, t)).start();
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
