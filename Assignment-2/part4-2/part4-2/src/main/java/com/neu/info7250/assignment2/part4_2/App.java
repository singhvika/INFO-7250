package com.neu.info7250.assignment2.part4_2;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Locale;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MapReduceIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.neu.info7250.assignment2.part4_2.dao.ClientProviderImpl;
import com.sun.jmx.remote.protocol.rmi.ClientProvider;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
        MongoClient client = new ClientProviderImpl().getMongoClient("localhost", 27017);
        MongoDatabase db = client.getDatabase("accesslog");
        
        Path currentRelativePath = Paths.get("");
        String s = currentRelativePath.toAbsolutePath().toString();
        System.out.println("Current relative path is: " + s);
        
        
        File f1 = null;
        File f2 = null;
        File f3 = null;
        
        try {
        	String filePath = s + File.separator + "data" + File.separator + "access.log";
        	f1 = new File(filePath);
        }
        catch (Exception ex) {
        	ex.printStackTrace();
        }
        
        App app = new App();
        MongoCollection<Document> entries = app.importAccessLog(f1, "accesslog", db);
        
        // get access count  by ip
        app.getAccessCountByIP(entries, "accessCountByIP");
        
        
        //get access count by method
        app.getAccessCountByMethod(entries, "accessCountByMethod");
        
        //get total logged Respnse Codes
        
        MapReduceIterable<Document> op = app.getLoggedStatusCodes(entries, "responseCodes");
        for (Document d : op) {
			System.out.println(d.toString());
		}
        
        // get latest access Times by IP
        MapReduceIterable<Document> accessTimes = app.getLatestAcessByIP(entries, "latestAccessByIP");
        for (Document d : accessTimes) {
			System.out.println(d.toString());
		}
        
    }
    
    public MongoCollection<Document> importAccessLog(File file, String collection, MongoDatabase db){
    	
    	MongoCollection<Document> mc = null;
    	int i = 0;
    	try {
    		if (file == null || file.length()==0) {
    			throw new FileNotFoundException(file.getName()+ "file not found, cannot import");
    		}
    		mc = db.getCollection("entries");
    		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
    		ArrayList<Document> logList = new ArrayList<Document>();
        	String line = null;
        	while ((line = br.readLine()) !=null) {
        		String[] entryTokens = line.split("\\s+");
        		String ip = entryTokens[0];
        		String dateString = line.substring(line.indexOf('[') + 1, line.indexOf(']')).split("\\s+")[0];
        		DateFormat dtf = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);
        		Date date = dtf.parse(dateString);
        		String[] methodTokens = line.substring(line.indexOf("\"") +1,line.lastIndexOf("\"")).split("\\s+"); 
        		String method = methodTokens[0];
        		String url = "";
        		if (method.equals("HEAD") || method.equals("POST") || method.equals("GET") || method.equals("PUT") || method.equals("DELETE") || method.equals("OPTIONS"))
        		{
        			url = methodTokens[1];
        		}else {
        			method = "";
        			url = "";
        		}
        		int responseCode = Integer.parseInt(entryTokens[entryTokens.length-2]);
        		Document log = new Document();
        		log.append("ip", ip);
        		log.append("date", date);
        		log.append("method", method);
        		log.append("url", url);
        		log.append("responseCode", responseCode);
        		logList.add(log);
        		i = i+1;
        		
        	}
        	mc.insertMany(logList);
    		
    		
    	}
    	catch(Exception ex) {
    		ex.printStackTrace();
    	}
    	
    	
    	return mc;
    	
    	
    }
    
    public MapReduceIterable<Document> getAccessCountByIP(MongoCollection<Document> collection, String outputCollection){
    	MapReduceIterable<Document> it = null;
    	try {
    		String map ="function () {\r\n" + 
    				"	emit(this.ip, {\"ip\":this.ip, \"count\": 1});\r\n" + 
    				"}";
    		
    		String reduce = "function (key, values){\r\n" + 
    				"	var sum = 0;\r\n" + 
    				"	values.forEach(function(element){\r\n" + 
    				"		sum = sum + element.count;\r\n" + 
    				"	});\r\n" + 
    				"	return {\"ip\": key, \"count\": sum};\r\n" + 
    				"}";
    		
    		it = collection.mapReduce(map, reduce).collectionName("accessCountByIP");
    		it.toCollection();
    	}catch(Exception ex) {
    		ex.printStackTrace();
    	}
    	return it;
    }
    
    public MapReduceIterable<Document> getAccessCountByMethod(MongoCollection<Document> collection, String outputCollection){
    	MapReduceIterable<Document> it = null;
    	try {
    		String map ="function () {\r\n" + 
    				"	emit(this.method, {\"method\":this.method, \"count\": 1});\r\n" + 
    				"}";
    		
    		String reduce = "function (key, values){\r\n" + 
    				"	var sum = 0;\r\n" + 
    				"	values.forEach(function(element){\r\n" + 
    				"		sum = sum + element.count;\r\n" + 
    				"	});\r\n" + 
    				"	return {\"method\": key, \"count\": sum};\r\n" + 
    				"}";
    		
    		it = collection.mapReduce(map, reduce).collectionName("accessCountByMethod");
    		it.toCollection();
    	}catch(Exception ex) {
    		ex.printStackTrace();
    	}
    	return it;
    }
    
    public MapReduceIterable<Document> getLoggedStatusCodes(MongoCollection<Document> collection, String outputCollection){
    	MapReduceIterable<Document> it = null;
    	try {
    		String map ="function () {\r\n" + 
    				"	emit(this.responseCode, {\"responseCode\":this.responseCode, \"count\": 1});\r\n" + 
    				"}";
    		
    		String reduce = "function (key, values){\r\n" + 
    				"	var sum = 0;\r\n" + 
    				"	values.forEach(function(element){\r\n" + 
    				"		sum = sum + element.count;\r\n" + 
    				"	});\r\n" + 
    				"	return {\"responseCode\": key, \"count\": sum};\r\n" + 
    				"}";
    		
    		it = collection.mapReduce(map, reduce).collectionName(outputCollection);
    		it.toCollection();
    		
    	}catch(Exception ex) {
    		ex.printStackTrace();
    	}
    	return it;
    }
    
    public MapReduceIterable<Document> getLatestAcessByIP(MongoCollection<Document> collection, String outputCollection){
    	MapReduceIterable<Document> it = null;
    	try {
    		String map = "function () {\r\n" + 
    				"	emit(this.ip, {\"date\":this.date});\r\n" + 
    				"}";
    		
    		String reduce = "function (key, values){\r\n" + 
    				"	var max = new Date(values[0].date);\r\n" + 
    				"	values.forEach(function(element){\r\n" + 
    				"		var ct = new Date(element.date);\r\n" + 
    				"		if (ct>max){\r\n" + 
    				"			max = ct;\r\n" + 
    				"		}\r\n" + 
    				"	});\r\n" + 
    				"	return {\"date\": max};\r\n" + 
    				"}";
    		
    		it = collection.mapReduce(map, reduce).collectionName(outputCollection);
    		it.toCollection();
    	}catch(Exception ex) {
    		ex.printStackTrace();
    	}
    	return it;
    }
    
    
}
