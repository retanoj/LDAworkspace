package Fetch_Reg_Load;

import java.sql.*;
public class ConnUtil {

	public static Connection getConn(){
		Connection conn = null;
		try{
			Class.forName("org.postgresql.Driver");
			String url = "jdbc:postgresql://10.1.10.115:5432/httplog";
			try{
				conn = DriverManager.getConnection(url, "gpadmin", "gpadmin");
				conn.setAutoCommit(true);
			}catch(SQLException e){
				e.printStackTrace();
			}
		}catch (Exception e){
			e.printStackTrace();
		}
		
		return conn;
	}
}
