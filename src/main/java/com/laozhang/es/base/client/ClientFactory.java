package com.laozhang.es.base.client;

import com.laozhang.es.base.client.ClientConstant;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

public class ClientFactory extends BasePooledObjectFactory<RestHighLevelClient>{
	
	private String[] ips;
	
	private int[] ports;
	
	public ClientFactory(String[] ips,int[] ports){
		this.ips = ips;
		this.ports = ports;
	}
	
	@Override
	public RestHighLevelClient create() throws Exception {
		HttpHost[] hosts = new HttpHost[ips.length];
		for (int i = 0; i < hosts.length; i++) {
			hosts[i] = new HttpHost(ips[i], ports[i], ClientConstant.ES_SERVER_IO_PROTOCOL);
		}
		RestClientBuilder rcb = RestClient.builder(hosts);
		
		return new RestHighLevelClient(rcb);
	}

	@Override
	public PooledObject<RestHighLevelClient> wrap(RestHighLevelClient arg0) {
		return new DefaultPooledObject<RestHighLevelClient>(arg0);
	}
	
	@Override
	public void activateObject(PooledObject<RestHighLevelClient> p) throws Exception {
		super.activateObject(p);
	}
	
	@Override
	public void passivateObject(PooledObject<RestHighLevelClient> p) throws Exception {
		//p.getObject().close();
		super.passivateObject(p);
	}
	
    @Override
    public void destroyObject(PooledObject<RestHighLevelClient> p)throws Exception {
    	p.getObject().close();
    }
}
