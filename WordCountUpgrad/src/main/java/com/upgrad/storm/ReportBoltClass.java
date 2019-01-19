package com.upgrad.storm;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;



public class ReportBoltClass extends BaseRichBolt 
{

    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private HashMap<String, Long> ReportCounts = null;
    //private HashMap<String, Long> wordAggregateCounts = null;
    int temp_count_variable=0;
    private Connection con;
    private Statement stmt;
    static final String username="root";
    static final String password="123";
    private OutputCollector collector;

    public void prepare(Map config, TopologyContext context, OutputCollector collector) 
    {
        this.ReportCounts = new HashMap<String, Long>();
        //this.wordAggregateCounts = new HashMap<String, Long>();
        this.collector = collector;
        
        try {
        	Class.forName("com.mysql.jdbc.Driver");
      		con=DriverManager.getConnection("jdbc:mysql://localhost:3306/upgrad",username,password);  
      		stmt=con.createStatement(); 
			
		}catch(SQLException sqlE) {
			sqlE.printStackTrace();
		}catch(ClassNotFoundException clE) {
			clE.printStackTrace();
		}catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
    }

    public void execute(Tuple tuple) 
    {
        String word = tuple.getStringByField("word");
        Long count = tuple.getLongByField("count");
        
        /*If the word is already available in Report bolt the update its count. Otherwise Add the count with word as ney key*/
        if(this.ReportCounts.containsKey(word)) {
        	Long lastCount = this.ReportCounts.get(word);
        	Long updatedCount = count+lastCount;
        	this.ReportCounts.put(word,updatedCount);
        }else {
        	this.ReportCounts.put(word, count);
        }
        
        temp_count_variable++;
        if(temp_count_variable == 1000) {
        	temp_count_variable =0;
        	List<String> keys = new ArrayList<String>();
            keys.addAll(this.ReportCounts.keySet());
            Collections.sort(keys);
            for (String key : keys) {
            	String selectQueryCountWord = "select wordcount from wordcount_assignment where word='"+key+"'";
            	try {
        			ResultSet resultSet  = stmt.executeQuery(selectQueryCountWord);
        			if(resultSet.next()) {
        				String updateQuery = "update wordcount_assignment set wordcount ="+this.ReportCounts.get(key)+" where word = '"+key+"'";
        				boolean isDataUpdated = stmt.execute(updateQuery);
        			}else {
        				String insertQuery = "INSERT INTO wordcount_assignment(word,wordcount) VALUES ('"+key+"',"+this.ReportCounts.get(key)+")";
        				boolean isDataInserted = stmt.execute(insertQuery);
        			}
        		} catch (SQLException e) {
        			// TODO Auto-generated catch block
        			
        			/*Failing the tuple*/
        			
        			//this.collector.fail(tuple);
        			e.printStackTrace();
        		}
            	
            	
            }
        }
        
  
        
        
        
        /*Acknowledging the Tuple*/
        
        this.collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) 
    {
        // this bolt does not emit anything

    }
    public void cleanup() 
    {
        System.out.println("--- FINAL COUNTS ---");
        List<String> keys = new ArrayList<String>();
        keys.addAll(this.ReportCounts.keySet());
        Collections.sort(keys);
        for (String key : keys) {
            System.out.println(key + " : " + this.ReportCounts.get(key));
        }
        System.out.println("--------------");
    }
}