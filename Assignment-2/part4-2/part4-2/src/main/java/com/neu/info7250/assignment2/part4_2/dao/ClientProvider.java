package com.neu.info7250.assignment2.part4_2.dao;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

public interface ClientProvider {
	public MongoClient getMongoClient(String host, int port);

}
