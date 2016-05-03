package br.adtsahring.com.fix_name;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class App extends Thread 
{
	
	private static final Logger logger = Logger.getLogger("sync_users");
	
	private static final String DB_ADTS_JDBC_URL = "DB_ADTS_JDBC_URL";
	private static final String DB_ADTS_USER = "DB_ADTS_USER";
	private static final String DB_ADTS_PASS = "DB_ADTS_PASS";

	private static final String SEL_UIDS = "select uid from tb_adt_user where quota = 0 or quota is null order by uid";	
	private static final String UPD_USER = "update tb_adt_user set quota=?, used_space=? where uid=?";
	private static final String SEL_USED_SPACE = "SELECT coalesce(size, 0) as used_space"
			+ " FROM oc_filecache"
			+ " where storage = (select numeric_id from oc_storages where id = concat('object::user:', ?))"
			+ " and path='files' order by fileid desc limit 1";
	private static final String SEL_USER_QUOTA = "SELECT" 
			+ " case upper(trim(SUBSTRING_INDEX(SUBSTRING_INDEX(configvalue,' ', 2), ' ',-1)))"
			+ " when 'B'" 
			+ " then CAST(trim(SUBSTRING_INDEX(configvalue, ' ', 1)) AS UNSIGNED INTEGER)"
			+ " when 'KB'" 
			+ " then CAST(trim(SUBSTRING_INDEX(configvalue, ' ', 1)) AS UNSIGNED INTEGER) * pow(1024, 1)"
			+ " when 'MB'" 
			+ " then CAST(trim(SUBSTRING_INDEX(configvalue, ' ', 1)) AS UNSIGNED INTEGER) * pow(1024, 2)"
			+ " when 'GB'" 
			+ " then CAST(trim(SUBSTRING_INDEX(configvalue, ' ', 1)) AS UNSIGNED INTEGER) * pow(1024, 3)"
			+ " when 'TB'" 
			+ " then CAST(trim(SUBSTRING_INDEX(configvalue, ' ', 1)) AS UNSIGNED INTEGER) * pow(1024, 4)"
			+ " when 'PB'" 
			+ " then CAST(trim(SUBSTRING_INDEX(configvalue, ' ', 1)) AS UNSIGNED INTEGER) * pow(1024, 5)"
			+ " else 0" 
			+ " end"
			+ " as volume"
			+ " FROM oc_preferences" 
			+ " where userid <> 'admin'"
			+ " and userid=?" 
			+ " and appid='files' and configkey='quota'"
			+ " and configvalue <> '?'";
		
	private List<Integer> uidsToProcess;
	protected static boolean errorInThread = false; 
	
    public static void main( String[] args ) throws SQLException
    {    	
    	Integer numberOfThreads = (args.length > 0) ? Integer.parseInt(args[0]) : 1;
    	errorInThread = false;
    	
    	try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
    	
        Connection conn = DriverManager.getConnection(
        		System.getenv(DB_ADTS_JDBC_URL), 
        		System.getenv(DB_ADTS_USER), 
        		System.getenv(DB_ADTS_PASS));
        
        Long startedIn = System.currentTimeMillis();
        PreparedStatement selUids = conn.prepareStatement(SEL_UIDS);
        ResultSet uids = selUids.executeQuery();
        try {
        	
        	try {
            	uids.last();
            	Integer numberOfUsers = uids.getRow();
            	uids.beforeFirst();

                if (numberOfUsers > 0) {
                	
                	Integer usersPerThread = numberOfUsers / numberOfThreads;
                	if (usersPerThread < 1) {
                		usersPerThread = 1;
                		numberOfThreads = numberOfUsers;
                	}

                	List<Thread> threadList = new ArrayList<Thread>();
                	
                    List<Integer> uidsToProcess = new ArrayList<Integer>(); 
                	while (uids.next()) {
                		uidsToProcess.add(uids.getInt(1));
                		
                		if (threadList.size() != numberOfThreads-1 && uidsToProcess.size() == usersPerThread) {
                			threadList.add(new App(uidsToProcess));
                			uidsToProcess = new ArrayList<Integer>();
                		}
                	}
                	
            		if (uidsToProcess.size() > 0) {
            			threadList.add(new App(uidsToProcess));
            		}
                	
                	for (Thread t : threadList) {
                    	t.start();	
                	}
                	
                	for (Thread t : threadList) {
                    	t.join();
                    	if (errorInThread) {
                    		throw new Exception("Error in thread.");
                    	}
                	}
                }
                
                Long elapsedMs = (System.currentTimeMillis() - startedIn);
                String hms = String.format("%02d:%02d:%02d", TimeUnit.MILLISECONDS.toHours(elapsedMs),
                        TimeUnit.MILLISECONDS.toMinutes(elapsedMs) - TimeUnit.HOURS.toMinutes(TimeUnit.MILLISECONDS.toHours(elapsedMs)),
                        TimeUnit.MILLISECONDS.toSeconds(elapsedMs) - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(elapsedMs)));                
                logger.info("DONE !! Elapsed time " + hms);
        	} catch (Exception e) {
        		logger.warning("ENDED WITH ERRORS: " + e.getMessage());
        	}
        	        	        	
        } finally {
        	uids.close();
        	selUids.close();
        	conn.close();
        }
    }
    
    public App(List<Integer> uidsToProcess) {
    	super();
    	this.uidsToProcess = uidsToProcess;
    }
    
    public void run() {
    	
    	Connection conn = null;
    	PreparedStatement updUser = null;
    	PreparedStatement selUsedSpace = null;
    	PreparedStatement selUserQuota = null;
    	ResultSet usedSpaceInfo = null;
    	ResultSet quotaInfo = null;

    	try {
        	try {
        		
                conn = DriverManager.getConnection(
                		System.getenv(DB_ADTS_JDBC_URL), 
                		System.getenv(DB_ADTS_USER), 
                		System.getenv(DB_ADTS_PASS));
        		
                selUsedSpace = conn.prepareStatement(SEL_USED_SPACE);
                selUserQuota = conn.prepareStatement(SEL_USER_QUOTA);
            	updUser = conn.prepareStatement(UPD_USER);
            	
            	for (Integer uid : this.uidsToProcess) {
            		if (errorInThread) {
            			return;
            		}

            		Long quota = 0l; 
        			Long usedSpace = 0l;
        			
        			selUsedSpace.setInt(1, uid);
        			usedSpaceInfo = selUsedSpace.executeQuery();
        			try {
        				while (usedSpaceInfo.next()) {
        					usedSpace = usedSpaceInfo.getLong(1);
        					break;
        				}
        			} finally {
        				usedSpaceInfo.close();
        			}        			
        			
        			selUserQuota.setInt(1, uid);
        			quotaInfo = selUserQuota.executeQuery();
        			try {
        				while (quotaInfo.next()) {
        					quota = quotaInfo.getLong(1);
        					break;
        				}
        			} finally {
        				quotaInfo.close();
        			}        			
        			
        			logger.info("Updating user " + uid + " with quota "+quota+" and used space "+usedSpace);
        			
        			updUser.setLong(1, quota);
        			updUser.setLong(2, usedSpace);
        			updUser.setInt(3, uid);
        			updUser.executeUpdate();
        			
            		yield();
            	}            	
        	} catch (Exception e) {
        		errorInThread = true;
        		logger.warning(e.getMessage());
        		return;
        	}
    	} finally {
    		if (conn != null)
				try {
					conn.close();
				} catch (SQLException e) {
					logger.warning(e.getMessage());
				}
    		if (updUser != null)
				try {
					updUser.close();
				} catch (SQLException e) {
					logger.warning(e.getMessage());
				}
    		if (selUserQuota != null)
				try {
					selUserQuota.close();
				} catch (SQLException e) {
					logger.warning(e.getMessage());
				}
    		if (selUsedSpace != null)
				try {
					selUsedSpace.close();
				} catch (SQLException e) {
					logger.warning(e.getMessage());
				}
    		if (usedSpaceInfo != null)
				try {
					usedSpaceInfo.close();
				} catch (SQLException e) {
					logger.warning(e.getMessage());
				}
    		if (quotaInfo != null)
				try {
					quotaInfo.close();
				} catch (SQLException e) {
					logger.warning(e.getMessage());
				}
    	}
    }
    
}
