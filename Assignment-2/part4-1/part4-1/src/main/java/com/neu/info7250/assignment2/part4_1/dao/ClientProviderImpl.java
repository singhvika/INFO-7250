package com.neu.info7250.assignment2.part4_1.dao;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

public class ClientProviderImpl implements ClientProvider {
	static MongoClient mc;

	public ClientProviderImpl() {
		// TODO Auto-generated constructor stub
		mc = null;
	}

	@Override
	public MongoClient getMongoClient(String host, int port) {
		// TODO Auto-generated method stub
		if (mc == null) {
			try {
				mc = new MongoClient(host, port);
			} catch (Exception ex) {
				ex.printStackTrace();
			}

		}
		return mc;

	}


}
