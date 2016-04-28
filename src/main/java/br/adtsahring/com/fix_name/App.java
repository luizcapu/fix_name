package br.adtsahring.com.fix_name;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Logger;

public class App 
{
	
	private static final Logger logger = Logger.getLogger("sync_users");
	
	private static Connection adtsConn;	
	private static final String DB_ADTS_JDBC_URL = "DB_ADTS_JDBC_URL";
	private static final String DB_ADTS_USER = "DB_ADTS_USER";
	private static final String DB_ADTS_PASS = "DB_ADTS_PASS";
	
	private static final String UPD_ADTS_USER = "update tb_adt_user set cancellation=? where uid_client=?"; 
	private static PreparedStatement updAdtsUser;

	private static Connection odinConn;	
	private static final String DB_ODIN_JDBC_URL = "DB_ODIN_JDBC_URL";
	private static final String DB_ODIN_USER = "DB_ODIN_USER";
	private static final String DB_ODIN_PASS = "DB_ODIN_PASS";
	
	private static final String SEL_ODIN_USER = "select uid_client, cancellation from tb_user where cancellation is not null";
	private static PreparedStatement selectOdinUser;
	
    public static void main( String[] args ) throws SQLException
    {
        App app = new App();
        app.run();
		logger.info("DONE !!");
    }
    
    public App() throws SQLException {
    	
    	try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
    	
    	//-- BEGIN ADTS CONN
        adtsConn = DriverManager.getConnection(
        		System.getenv(DB_ADTS_JDBC_URL), 
        		System.getenv(DB_ADTS_USER), 
        		System.getenv(DB_ADTS_PASS));
        updAdtsUser = adtsConn.prepareStatement(UPD_ADTS_USER);
    	//-- END ADTS CONN
        
    	//-- BEGIN ODIN CONN
        odinConn = DriverManager.getConnection(
        		System.getenv(DB_ODIN_JDBC_URL), 
        		System.getenv(DB_ODIN_USER), 
        		System.getenv(DB_ODIN_PASS));        
        selectOdinUser = odinConn.prepareStatement(SEL_ODIN_USER);
    	//-- END ODIN CONN
    }
    
    private void run() throws SQLException {
		ResultSet odinUsers = selectOdinUser.executeQuery();		
		try {
    		while (odinUsers.next()) {
    			logger.info("Updating cancellation to " + odinUsers.getTimestamp("cancellation") + " for user " + odinUsers.getInt("uid_client"));
    			updAdtsUser.setTimestamp(1, odinUsers.getTimestamp("cancellation"));
    			updAdtsUser.setInt(2, odinUsers.getInt("uid_client"));
    			updAdtsUser.executeUpdate();
    		}    		    			
		} finally {
			odinUsers.close();
		}    	
    }
    
}
