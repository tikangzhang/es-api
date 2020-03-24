package com.laozhang.es.base.client;

import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientDriver {
	private static Logger logger = LoggerFactory.getLogger(ClientDriver.class);
	
	private static String[] ips;
	
	private static int[] ports;
	
	private static GenericObjectPool<RestHighLevelClient> pool;

	static{
		GenericObjectPoolConfig<RestHighLevelClient> config = new GenericObjectPoolConfig<RestHighLevelClient>();
		Properties props = new Properties();
		InputStream in = ClientDriver.class.getClassLoader().getResourceAsStream("es.properties");
	    try {
			props.load(in);
		} catch (Exception e) {
			logger.error("{}", e);
		}
		String hostsProp = props.getProperty(ClientConstant.ES_SERVER_HOSTS);
		String[] hosts = hostsProp.split(";");
		int len;
		if(hosts != null && (len = hosts.length) > 0){
			String host;
			String[] detail;
			ips = new String[len];
			ports = new int[len];
			for (int i = 0; i < len; i++) {
				host = hosts[i];
				if(StringUtils.isEmpty(host)){
					throw new RuntimeException(ClientConstant.ES_SERVER_HOSTS + " config empty error.");
				}
				detail = host.split(":");
				ips[i] = detail[0];
				if(detail.length > 1){
					ports[i] = Integer.parseInt(detail[1]);
				}else{
					ports[i] = 9200;
				}
			}
			config.setMaxTotal(Integer.parseInt(props.getProperty(ClientConstant.ES_CLIENT_POOL_MAX_COUNT)));
			config.setMaxIdle(Integer.parseInt(props.getProperty(ClientConstant.ES_CLIENT_POOL_MAX_IDLE_COUNT)));
			config.setMinIdle(Integer.parseInt(props.getProperty(ClientConstant.ES_CLIENT_POOL_MIN_IDLE_COUNT)));
			config.setMaxWaitMillis(Integer.parseInt(props.getProperty(ClientConstant.ES_CLIENT_POOL_MAX_WAIT_MILLIS)));
			
			pool = new GenericObjectPool<>(new ClientFactory(ips,ports), config);
		}else{
			throw new RuntimeException(ClientConstant.ES_SERVER_HOSTS + " config error.");
		}

	}
	public static ObjectPool<RestHighLevelClient> getClientPool(){
		return pool;
	}
	
	public static void Destroy(){
		if(pool != null){
			pool.clear();
			pool.close();
		}
	}
	
	public static String[] getIp(){
		return ips;
	}
	public static int[] getPort(){
		return ports;
	}
}
