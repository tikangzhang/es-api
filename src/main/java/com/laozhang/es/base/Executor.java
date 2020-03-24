package com.laozhang.es.base;

import org.apache.commons.pool2.ObjectPool;
import org.elasticsearch.client.RestHighLevelClient;

import com.laozhang.es.base.client.ClientDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Executor{
	public static Logger logger = LoggerFactory.getLogger(Executor.class);
	
	private static ObjectPool<RestHighLevelClient> pool = ClientDriver.getClientPool();
	
	ThreadLocal<RestHighLevelClient> localClient = new ThreadLocal<RestHighLevelClient>();
	
	public Executor(){}
	
	protected RestHighLevelClient getClient() {
		try {
			RestHighLevelClient client = localClient.get();
			if(client == null){
				client = pool.borrowObject();
				localClient.set(client);
			}
			return client;
		} catch (Exception e) {
			logger.error("", e);
			return null;
		}
	}
	
	/**
	 * 使用完就归还，同时置空
	 * 否则再从对象池获取
	 */
	protected void returnBack(){
		RestHighLevelClient client;
		try {
			client = localClient.get();
			if(client != null){
				pool.returnObject(client);
				localClient.remove();
			}
		} catch (Exception e) {
			logger.error("", e);
		}finally {
			client = null;
		}
	}
}
