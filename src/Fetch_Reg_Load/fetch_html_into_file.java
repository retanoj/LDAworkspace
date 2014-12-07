package Fetch_Reg_Load;


import java.lang.Thread;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.LinkedBlockingQueue;

import org.postgresql.util.PSQLException;

import de.tototec.cmdoption.CmdOption;
import de.tototec.cmdoption.CmdlineParser;
import de.tototec.cmdoption.CmdlineParserException;


class dbQuery extends Thread{
	int start_pos;
	int step;
	String table_from;
	
	LinkedBlockingQueue qUndo;
	Connection conn;
	PreparedStatement stmt_query;
	
	long count = 0;
	public dbQuery(LinkedBlockingQueue<Data> qUndo, String table_from, int start_pos, int step){
		this.start_pos = start_pos;
		this.step = step;
		this.table_from = table_from;
		this.qUndo = qUndo;
		this.count += start_pos;
		
		try {
			this.conn = ConnUtil.getConn();
						
			String query_string = String.format("select id, data_voc from %s where dist>=? and dist<?", table_from);
			stmt_query = conn.prepareStatement(query_string);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
	}
	
	@Override
	public void run(){
		double tBegin = System.currentTimeMillis();
		double tEnd;
		try {			
			int left = start_pos;
			boolean select_rs_flag = true;
			
			while(select_rs_flag){
				//select part into undo Queue
				if(qUndo.size() < step *5){
					select_rs_flag = false;
					stmt_query.setInt(1, left);
					stmt_query.setInt(2, left+step);
					ResultSet select_rs = stmt_query.executeQuery();
					
					if (select_rs.next()){
						select_rs_flag = true;
						while(select_rs.next()){
							Data t = new Data();
							t.id = select_rs.getInt("id");
							t.url = select_rs.getString("data_voc");
							t.data_voc = "";
							qUndo.offer(t);
						}
						left += step;
					}
					
				}else{
					Thread.sleep(500);
				}
				if(left % 100000 == 0){
					System.out.println(String.format("Until now, we fetch \033[1;32;40m %d \033[0m records from db ", left));
				}
				System.out.println(String.format("\033[1;36;40m undoQueue size is %d \033[0m", qUndo.size()));
			}
			stmt_query.close();
		} catch (PSQLException e) {
			e.printStackTrace();
		} catch (SQLException e){
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}finally{
			tEnd = System.currentTimeMillis();
			System.out.println(String.format("\033[1;31;40m dbQuery thread is done. Cost %f s\033[0m", (tEnd-tBegin)/1000));
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
}


public class fetch_html_into_file {
	int tFetchUrl_num;
	int tNum;
	int start_pos;
	int step;
	String table_from;
	
	LinkedBlockingQueue<Data> qUndo;
	
	private void start_dbload(){		
		qUndo = new LinkedBlockingQueue<Data>();
		
		Thread[] t = new Thread[tFetchUrl_num];
		
		for(int i=0; i<tFetchUrl_num; i++){
			t[i] = new Crawler_html(qUndo, "thread"+i);
		}
		
		(new dbQuery(qUndo, table_from, start_pos, step)).start();
		for(int i=0; i<tFetchUrl_num; i++){
			t[i].start();
		}
	}
	
	
	public static void main(String[] args) { 
		Config config = new Config();
		CmdlineParser cp = new CmdlineParser(config);
		
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
		
		fetch_html_into_file m = new fetch_html_into_file();
		m.table_from = config.table_from;
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
		
		@CmdOption(names = {"--thread_num", "-tn"}, args = {"[200]"}, description = "线程数量，默认200", maxCount=-1)
		public Integer thread_num = 200;
		
		@CmdOption(names = {"--start_pos"}, args = {"[0]"}, description = "起始位置,默认为0", maxCount=-1)
		public Integer start_pos = 0;
		
		@CmdOption(names = {"--step"}, args = {"[500]"}, description = "查询步长，默认为500")
		public Integer step = 500;
		
	}


}
