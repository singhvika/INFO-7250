package com.neu.info7250.assignment2.part4_1;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.bson.BasicBSONObject;
import org.bson.Document;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MapReduceCommand;
import com.mongodb.MapReduceOutput;
import com.mongodb.MongoClient;
import com.mongodb.client.MapReduceIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.MapReduceAction;
import com.neu.info7250.assignment2.part4_1.dao.ClientProviderImpl;

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
        MongoDatabase db = client.getDatabase("movies");
        
        
        
        Path currentRelativePath = Paths.get("");
        String s = currentRelativePath.toAbsolutePath().toString();
        System.out.println("Current relative path is: " + s);
        
        
        File f1 = null;
        File f2 = null;
        File f3 = null;
        
        try {
        	String filePath = s + File.separator + "data" + File.separator + "movies.dat";
        	f1 = new File(filePath);
        }
        catch (Exception ex) {
        	ex.printStackTrace();
        }
        
        try {
        	String filePath = s + File.separator + "data" + File.separator + "ratings.dat";
        	f2 = new File(filePath);
        }
        catch (Exception ex) {
        	ex.printStackTrace();
        }
        
        try {
        	String filePath = s + File.separator + "data" + File.separator + "users.dat";
        	f3 = new File(filePath);
        }
        catch (Exception ex) {
        	ex.printStackTrace();
        }
        
        App app = new App();
        
        // import data
        MongoCollection rc = app.importRatingsData(f2, "ratings", db);
        MongoCollection<Document> mc = app.importMoviesData(f1, "movies", db);
        
        
        // Map Reduce for Ratings
        app.getMoviesPerRatingCount(rc, "movieCountByRating");
        
        
        // Map reduce for Movies
        app.getMoviesPerYearCount(mc, "movieCountByYear");
        
        // Movie count by genre
        app.getMoviesPerGenreCount(mc, "movieCountByGenre");
        
        
    }
    
    public MongoCollection<Document> importMoviesData(File file, String collectionName, MongoDatabase db ) {
    	MongoCollection<Document> mc = null;
    	try {
    		if (file.exists() == false) {
    			throw new FileNotFoundException(file.getName()+ " not found, cannot import");
    		}
    		mc = db.getCollection(collectionName);
    		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
    		String line;
    		ArrayList<Document> docList = new ArrayList<Document>();
    		while ((line = br.readLine())!=null) {
    			String[] tokens = line.split("::");
    			Document movie = new Document();
    			movie.append("movie_id", tokens[0]);
    			String movieName = tokens[1].substring(0, tokens[1].lastIndexOf('(')-1);
    			int year = Integer.parseInt(tokens[1].substring(tokens[1].lastIndexOf('(')+1, tokens[1].length()-1));
    			movie.append("movie_name", movieName);
    			movie.append("movie_year", year);
    			List<String> cats = Arrays.asList(tokens[2].split("\\|"));
    			movie.append("genre", cats);
    			docList.add(movie);
    		}
    		
    		mc.insertMany(docList);
    		
    		
    	}
    	catch(FileNotFoundException fnf) {
    		fnf.printStackTrace();
    	}
    	catch(Exception ex) {
    		ex.printStackTrace();
    	}
    	return mc;
    }
    
    public MongoCollection<Document> importRatingsData(File file, String collectionName, MongoDatabase db ) {
    	boolean successStatus = true;
    	MongoCollection<Document> rc = null;
    	
    	try {
    		if (file.exists() == false) {
    			successStatus = false;
    			throw new FileNotFoundException(file.getName()+ " not found, cannot import");
    		}
    		
    		rc = db.getCollection(collectionName);
    		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
    		String line = null;
    		List<Document> docList = new ArrayList<Document>();
    		while ((line = br.readLine())!=null) {
    			String[] tokens = line.split("::");
    			Document rating = new Document();
    			rating.append("user_id", tokens[0])
    			.append("movie_id", tokens[1])
    			.append("rating", tokens[2]);
    			
    			docList.add(rating);

    			
    		}
    		rc.insertMany(docList);
    	}catch (Exception ex) {
    		ex.printStackTrace();
    	}
    	
    	return rc;
    }
    
    public MapReduceIterable<Document> getMoviesPerRatingCount(MongoCollection<Document> rc, String outputCollection) {
    	MapReduceIterable<Document> op = null;
    	try {
    		String map = "function (){\n" + 
    				"	emit(this.rating, 1);\n" + 
    				"}";
    		String reduce = "function (key, values){\n" + 
    				"	var sum = 0;\n" + 
    				"	sum = Array.sum(values);\n" + 
    				"	return sum;\n" + 
    				"}";
    		
    		op = rc.mapReduce(map, reduce).collectionName(outputCollection);
    		op.toCollection();
    		for (Document d : op) {
    			System.out.println(d.toString());
    		}
    		
    	}
    	catch(Exception ex) {
    		ex.printStackTrace();
    	}
    	return op;
    }
    
    public MapReduceIterable<Document> getMoviesPerYearCount(MongoCollection<Document> mc, String outputCollection){
    	MapReduceIterable<Document> op = null;
    	try {
    		String map ="function (){\r\n" + 
    				"	emit(this.movie_year, {\"movie_year\": this.movie_year, \"count\": 1});\r\n" + 
    				"}";
    		String reduce = "function (key, values){\r\n" + 
    				"	var count = 0;\r\n" + 
    				"	values.forEach(function(element) {\r\n" + 
    				"		count = count + element.count;\r\n" + 
    				"	})\r\n" + 
    				"	return {\"movie_year\": key, \"count\": count};\r\n" + 
    				"}";
    		
    		op = mc.mapReduce(map, reduce).collectionName(outputCollection);
    		op.toCollection();
    	}catch(Exception ex) {
    		ex.printStackTrace();
    	}
    	return op;
    }
    
    
    public MapReduceIterable<Document> getMoviesPerGenreCount(MongoCollection<Document> mc, String outputCollection){
    	MapReduceIterable<Document> op = null;
    	try {
    		String map = "function (){\r\n" + 
    				"	for (let c = 0; c<this.genre.length; c++){\r\n" + 
    				"	emit(this.genre[c],{\"genre\":this.genre[c], \"count\": 1});\r\n" + 
    				"	}\r\n" + 
    				"	\r\n" + 
    				"}";
    		
    		String reduce = "function (key, values){\r\n" + 
    				"	var sum = 0;\r\n" + 
    				"	values.forEach(function(v){\r\n" + 
    				"		sum = sum + v.count;\r\n" + 
    				"	});\r\n" + 
    				"	return {\"genre\": key, \"count\": sum};\r\n" + 
    				"}";
    		
    		op = mc.mapReduce(map, reduce).collectionName(outputCollection);
    		op.toCollection();
    	}catch(Exception ex) {
    		ex.printStackTrace();
    	}
    	return op;
    }
    
    
}
