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
	
	private static final Integer qtRecords = 10000;
	private static final String SEL_ADTS_USERS = "select * from tb_adt_user order by uid limit ?,"+qtRecords; 
	private static PreparedStatement selAdtsUsers;

	private static Connection odinConn;	
	private static final String DB_ODIN_JDBC_URL = "DB_ODIN_JDBC_URL";
	private static final String DB_ODIN_USER = "DB_ODIN_USER";
	private static final String DB_ODIN_PASS = "DB_ODIN_PASS";
	
	private static final String UPD_ODIN_USER = "update tb_user set ds_client = ? where uid_client = ?";
	private static PreparedStatement updateOdinUser;

	private static final String SEL_ODIN_USER = "select cancellation from tb_user where uid_client=?";
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
        selAdtsUsers = adtsConn.prepareStatement(SEL_ADTS_USERS);
    	//-- END ADTS CONN
        
    	//-- BEGIN ODIN CONN
        odinConn = DriverManager.getConnection(
        		System.getenv(DB_ODIN_JDBC_URL), 
        		System.getenv(DB_ODIN_USER), 
        		System.getenv(DB_ODIN_PASS));
        
        updateOdinUser = odinConn.prepareStatement(UPD_ODIN_USER);
        selectOdinUser = odinConn.prepareStatement(SEL_ODIN_USER);
    	//-- END ODIN CONN
    }
    
    private void run() throws SQLException {
    	
    	Integer from = 0;
    	boolean hasRecords = true;
    	while (hasRecords) {
    		
    		hasRecords = false;
    		
    		selAdtsUsers.setInt(1, from);
    		ResultSet adtsUsers = selAdtsUsers.executeQuery();
    		
    		try {
        		while (adtsUsers.next()) {
        			hasRecords = true;
        			
        			//User user = new User(adtsUsers);
        			Long uidClient = adtsUsers.getLong("uid_client");
        			String dsClient = adtsUsers.getString("ds_client");
        			
        			logger.info("Updating name to '"+dsClient+"' for uid client " + uidClient);
        			
        			updateOdinUser.setString(1, dsClient);
        			updateOdinUser.setLong(2, uidClient);
        			updateOdinUser.executeUpdate();
        		}    		    			
    		} finally {
    			adtsUsers.close();
    		}
    		
    		from += qtRecords;
    	}
    }
    
}
