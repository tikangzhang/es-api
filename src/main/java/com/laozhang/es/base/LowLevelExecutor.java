package com.laozhang.es.base;

import java.util.Objects;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import com.laozhang.es.base.client.ClientDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LowLevelExecutor{
	public static Logger logger = LoggerFactory.getLogger(LowLevelExecutor.class);

	public final static String DELETE_BY_QUERY_ENDPOINT = "_delete_by_query";
	
	public final static String UPDATE_BY_QUERY_ENDPOINT = "_update_by_query";
	
	public final static String DEFAULT_METHOD = "POST";
	
	public final static String REQUEST_TYPE = "http";
	
	protected RestClient client;
	
	protected Response response;
	
	protected Request request;

	protected String index;
	
	protected String type;
	
	protected LowLevelExecutor(String index,String type){
		this.index = Objects.requireNonNull(index);
		this.type = type;
		String[] ips = ClientDriver.getIp();
		int[] ports = ClientDriver.getPort();
		HttpHost[] hosts = new HttpHost[ips.length];
		for (int i = 0; i < hosts.length; i++) {
			hosts[i] = new HttpHost(ips[i], ports[i], REQUEST_TYPE);
		}
		this.client = RestClient.builder(hosts).build();
	}
	
	protected String getEndpoint(String methodEndPoint){
		StringBuilder sb = new StringBuilder();
		sb.append(this.index);
		if(!StringUtils.isEmpty(this.type)){
			sb.append("/").append(this.type);
		}
		sb.append("/").append(methodEndPoint);
		return sb.toString();
	}
	
	public String responseToJson(){
		if(this.response == null){
			return null;
		}
		try {
			return EntityUtils.toString(this.response.getEntity());
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			return null;
		}
	}
}
