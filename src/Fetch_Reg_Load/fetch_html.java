package Fetch_Reg_Load;

import java.util.*;
import java.util.concurrent.*;
import java.lang.Thread;
import java.sql.*;

import org.postgresql.util.PSQLException;

class dbLoad extends Thread{
	int start_pos;
	int step;
	String fromTableName;
	String toTableName;
	
	LinkedBlockingQueue qUndo;
	LinkedBlockingQueue qDone;
	
	Connection conn;
	
	long count = 0;
	public dbLoad(LinkedBlockingQueue<Data> qUndo, LinkedBlockingQueue<Data> qDone, String fromTableName, String toTableName, int start_pos, int step){
		this.start_pos = start_pos;
		this.step = step;
		this.fromTableName = fromTableName;
		this.toTableName = toTableName;
		
		this.qUndo = qUndo;
		this.qDone = qDone;
		
		this.conn = ConnUtil.getConn();
		this.count += start_pos;
	}
	
	@Override
	public void run(){
		try {
			Statement stmt = conn.createStatement();
			
			int left = start_pos;
			double tBegin = System.currentTimeMillis();
			boolean select_rs_flag;

			System.out.println("\033[1;31;40m dbLoad thread is running... \033[0m");
			while(true){
				select_rs_flag = false;
				//select part into undo Queue
				String select_sql = String.format("select id, log_data1 from %s where id>=%d and id<%d;", this.fromTableName, left, left+this.step);
				ResultSet select_rs = stmt.executeQuery(select_sql);
				
				if (select_rs.next()){
					select_rs_flag = true;
					while(select_rs.next()){
						Data t = new Data();
						t.id = select_rs.getInt("id");
						t.url = select_rs.getString("log_data1");
						t.data_voc = "";
						
						this.qUndo.offer(t);
						left = t.id+1;
					}
					
				}
				try {
					Thread.sleep(800);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				//insert part from done Queue
				int size = this.step *10;
				String insert_sql = "";
				
				if (this.qDone.size() > size){
					for (int i=0; i<size; i++){
						Data t = (Data) this.qDone.poll();
						if (t.data_voc != null && t.data_voc.equals("")){
							insert_sql += String.format("insert into %s (id, data_voc) values(%d, '%s');", this.toTableName, t.id, t.data_voc);
						}
					}
					if (insert_sql != ""){
						stmt.execute(insert_sql);
						//conn.commit();
					}
					this.count += size;
					double tEnd = System.currentTimeMillis();
					System.out.println(String.format("\033[1;33;40m until now %d records done. Cost %fs per %d records \033[0m", this.count, (tEnd-tBegin)/1000, size));
					tBegin = tEnd;
				} else{
					if (select_rs_flag == false){
						Data t = null;
						while(true){
							try {
								t = (Data) this.qDone.poll(100, TimeUnit.SECONDS);
							} catch (InterruptedException e) {
								e.printStackTrace();
							}
							if (t == null)
								break;
							if (t.data_voc != null && t.data_voc.equals("")){
								insert_sql += String.format("insert into %s (id, data_voc) values(%d, '%s');", this.toTableName, t.id, t.data_voc);
							}
						}
						if (insert_sql != ""){
							stmt.execute(insert_sql);
							break;
						}
					}
					
				}

				System.out.println(String.format("\033[1;36;40m undoQueue size is %d, doneQueue size is %d \033[0m", this.qUndo.size(), this.qDone.size()));
			}
			  
			stmt.close();
			System.out.println("\033[1;31;40m dbLoad thread is done. \033[0m");
		} catch (PSQLException e) {
			e.printStackTrace();
		} catch (SQLException e){
			e.printStackTrace();
		} finally {
			try {
				conn.close();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}
}

public class fetch_html {
	int start_pos;
	int tFetchUrl_num;
	int tNum;
	int step;
	String tableName;
	
	LinkedBlockingQueue<Data> qUndo;
	LinkedBlockingQueue<Data> qDone;
	
	private void start_dbload(String tableName,int start_pos, int threadNum, int step){
		this.start_pos = start_pos;
		this.tFetchUrl_num = threadNum;
		this.step = step;
		this.tableName = tableName;
		
		this.qUndo = new LinkedBlockingQueue<Data>();
		this.qDone = new LinkedBlockingQueue<Data>();
		
		(new dbLoad(this.qUndo, this.qDone, this.tableName, this.tableName+"_html", this.start_pos, this.step)).start();
		
		for(int i=0;i<this.tFetchUrl_num; i++){
			(new MyCrawlerThread(this.qUndo, this.qDone)).start();
		}
		
		
	}
	
	
	public static void main(String[] args) { 
		fetch_html m = new fetch_html();
		m.start_dbload(args[0], Integer.parseInt(args[1]) , 200, 400);
	}

}
