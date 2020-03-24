package com.laozhang.es.base.client;

public class ClientConstant {
	//es server hosts
	public static final String ES_SERVER_HOSTS = "es.server.hosts";
	
	//es server io protocol
	public final static String ES_SERVER_IO_PROTOCOL = "http";
	
	//the max count of es client in object pool
	public final static String ES_CLIENT_POOL_MAX_COUNT = "es.client.pool.max.count";
	
	//the max idle object count of es client in object pool
	public final static String ES_CLIENT_POOL_MAX_IDLE_COUNT = "es.client.pool.max.idle.count";
	
	//the min idle object count of es client in object pool
	public final static String ES_CLIENT_POOL_MIN_IDLE_COUNT = "es.client.pool.min.idle.count";
	
	//the max millies time for waiting an es client
	public final static String ES_CLIENT_POOL_MAX_WAIT_MILLIS = "es.client.pool.max.wait.millis";
}
