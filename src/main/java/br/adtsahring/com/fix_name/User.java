package br.adtsahring.com.fix_name;

import java.sql.ResultSet;
import java.util.Date;

public class User {
	
	private Long uid;
	private Date cancellation;
	private String dsEmail;
	private Long uidClient;
	private String dsClient;
	private Integer status;
	private Integer idQuota;
	
	public User(ResultSet dbResource) {
		
	}
	
}
