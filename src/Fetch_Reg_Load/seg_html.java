package Fetch_Reg_Load;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.postgresql.util.PSQLException;

import de.tototec.cmdoption.CmdOption;
import de.tototec.cmdoption.CmdlineParser;
import de.tototec.cmdoption.CmdlineParserException;

class dbSeg extends Thread{
	int step;
	int start_pos;
	String table_from;
	String table_to;
	Thread[] tpool;
	
	LinkedBlockingQueue<Data> qUndo;
	LinkedBlockingQueue<Data> qDone;

	Connection conn;
	Statement stmt;
	
	private void insert_db(int step) throws SQLException{
		String insert_sql = "";
		while(step-- != 0){
			Data t = (Data) this.qDone.poll();
			insert_sql += String.format("insert into %s (id, data_seg) values(%d, '%s');", table_to, t.id, t.data_voc);
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
	public dbSeg(LinkedBlockingQueue<Data> qUndo, LinkedBlockingQueue<Data> qDone, String table_from, String table_to,int start_pos, int step, Thread[] t){
		this.step = step;
		this.start_pos = start_pos;
		this.table_from = table_from;
		this.table_to = table_to;
		this.tpool = t;
		this.qUndo = qUndo;
		this.qDone = qDone;
		
		this.conn = ConnUtil.getConn();
	}
	
	@Override
	public void run(){
		try {
			stmt = conn.createStatement();			
			int left = start_pos;
			boolean select_rs_flag = true;
			
			System.out.println("\033[1;31;40m dbSeg thread is running... \033[0m");
			while(select_rs_flag){
				//select part into undo Queue
				if(qUndo.size() < step *5){
					select_rs_flag = false;
					String select_sql = String.format("select id,data_voc from %s where html_id>=%d and html_id<%d;", table_from, left, left +step);
					ResultSet select_rs = stmt.executeQuery(select_sql);
					
					if (select_rs.next()){
						select_rs_flag = true;
						while(select_rs.next()){
							Data t = new Data();
							t.id = select_rs.getInt("id");
							t.data_voc = select_rs.getString("data_voc");
							qUndo.offer(t);
						}
						left += step;
					}
				}
				
				Thread.sleep(500);
				
				//insert part from done Queue					
				if (this.qDone.size() > step)
					insert_db(step);
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
			System.out.println("\033[1;31;40m dbSeg thread is done. \033[0m");
		} catch (PSQLException e) {
			e.printStackTrace();
		} catch (SQLException e){
			e.printStackTrace();
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		} finally {
			try {
				conn.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
public class seg_html {
	int tNum;
	int step;
	int start_pos;	
	String table_from;
	String table_to;
	
	LinkedBlockingQueue<Data> qUndo;
	LinkedBlockingQueue<Data> qDone;
	
	private void start_dbseg() {
		
		this.qUndo = new LinkedBlockingQueue<Data>();
		this.qDone = new LinkedBlockingQueue<Data>();

		Thread[] t = new Thread[tNum];
		
		for (int i=0;i<tNum; i++){
			t[i] = new MySegThread(this.qUndo, this.qDone);
			t[i].start();
		}
		try{
			Thread.sleep(10000);
		} catch (InterruptedException e) {	
			e.printStackTrace();
		}
		
		(new dbSeg(qUndo, qDone, table_from, table_to, start_pos, step, t)).start();
		
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
		
		seg_html m = new seg_html();
		m.table_from = config.table_from;
		m.table_to = config.table_to;
		m.start_pos = config.start_pos;
		m.step = config.step;
		m.tNum = 2; //分词线程数
		m.start_dbseg();
	}
	
	public static class Config {
		@CmdOption(names = {"--help", "-h"}, description = "show usage.", isHelp=true)
		public boolean help;
		
		@CmdOption(names = {"--db_table_from", "-tfrom"}, args = {"table_from"}, description = "表名,数据源", minCount=1, maxCount=-1)
		public String table_from;
		
		@CmdOption(names = {"--db_table_to", "-tto"}, args = {"table_to"}, description = "表名,数据目的地", minCount=1, maxCount=-1)
		public String table_to;
		
		@CmdOption(names = {"--start_pos"}, args = {"start_pos"}, description = "起始位置,默认为0", maxCount=-1)
		public Integer start_pos = 0;
		
		@CmdOption(names = {"--step"}, args = {"step"}, description = "查询步长，默认为300")
		public Integer step = 300;
		
	}

}
