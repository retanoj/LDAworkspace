package Fetch_Reg_Load;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.postgresql.util.PSQLException;

class dbSeg extends Thread{
	int step;
	int left;
	String fromTableName;
	String toTableName;
	
	LinkedBlockingQueue<Data> qUndo;
	LinkedBlockingQueue<Data> qDone;
	
	Connection conn;
	
	long count = 0;
	public dbSeg(LinkedBlockingQueue<Data> qUndo, LinkedBlockingQueue<Data> qDone, String fromTableName, String toTableName,int left, int step){
		this.step = step;
		this.left = left;
		this.fromTableName = fromTableName;
		this.toTableName = toTableName;
		
		this.qUndo = qUndo;
		this.qDone = qDone;
		
		this.conn = ConnUtil.getConn();
	}
	
	@Override
	public void run(){
		int size = this.step;
		String insert_sql;
		boolean select_rs_flag;
		Data t;
		try {
			Statement stmt = conn.createStatement();
			
			System.out.println("\033[1;31;40m dbSeg thread is running... \033[0m");
			while(true){
				select_rs_flag = false;
				//select part into undo Queue
				String select_sql = String.format("select id,data_voc from %s where html_id>=%d and html_id<%d;", this.fromTableName, this.left, this.left +this.step/2);
				ResultSet select_rs = stmt.executeQuery(select_sql);
				
				if (select_rs.next()){
					select_rs_flag = true;
					while(select_rs.next()){
						t = new Data();
						t.id = select_rs.getInt("id");
						t.data_voc = select_rs.getString("data_voc");
						
						this.qUndo.offer(t);
					}
					this.left += this.step;
					
				}
				try {
					Thread.sleep(200);
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
				//insert part from done Queue				
				insert_sql = "";
				
				if (this.qDone.size() > size){
					for (int i=0; i<size; i++){
						t = (Data) this.qDone.poll();
						if (t.data_voc != null && t.data_voc.length() != 0){
							insert_sql += String.format("insert into %s (id, data_seg) values(%d, '%s');", this.toTableName, t.id, t.data_voc);
						}
					}
					if (insert_sql != ""){
						stmt.execute(insert_sql);
						//System.out.println(insert_sql);
					}
				} else{
					if (select_rs_flag == false){
						t = null;
						System.out.println("[*] The process is nearing completion.");
						while(true){
							try {
								t = (Data) this.qDone.poll(100, TimeUnit.SECONDS);
							} catch (InterruptedException e) {
								e.printStackTrace();
							}
							if (t == null)
								break;
							if (t.data_voc != null && t.data_voc.length() != 0){
								insert_sql += String.format("insert into %s (id, data_seg) values(%d, '%s');", this.toTableName, t.id, t.data_voc);
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
			System.out.println("\033[1;31;40m dbSeg thread is done. \033[0m");
		} catch (PSQLException e) {
			e.printStackTrace();
		} catch (SQLException e){
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
public class seg_html {
	int tNum;
	int step;
	int left;
	String tableName;
	
	LinkedBlockingQueue<Data> qUndo;
	LinkedBlockingQueue<Data> qDone;
	
	private void start_dbseg(String tableName, int threadNum, int left, int step){
		this.tNum = threadNum;
		this.step = step;
		this.left = left;
		this.tableName = tableName;
		
		this.qUndo = new LinkedBlockingQueue<Data>();
		this.qDone = new LinkedBlockingQueue<Data>();

		for (int i=0;i<this.tNum; i++){
			(new MySegThread(this.qUndo, this.qDone)).start();
		}
		try {
			Thread.sleep(10000);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		(new dbSeg(this.qUndo, this.qDone, this.tableName+"_100w_html_2", this.tableName+"_100w_seg_2", this.left, this.step)).start();
		
	}
	
	
	public static void main(String[] args) {
		seg_html m = new seg_html();
		System.out.println("usage:java -jar seg_url.jar tableName_prefix start_pos");
		m.start_dbseg(args[0], 2, Integer.parseInt(args[1]), 100);
	}

}
