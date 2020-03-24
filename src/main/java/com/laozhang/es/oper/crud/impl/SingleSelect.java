package com.laozhang.es.oper.crud.impl;

import java.io.IOException;
import java.util.Map;

import com.laozhang.es.oper.crud.ISingleSelectable;
import com.laozhang.es.oper.crud.SingleRequestExecutor;
import com.laozhang.es.base.utils.BeanCopierUtil;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;

import com.laozhang.es.base.exception.NoneIdException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SingleSelect extends SingleRequestExecutor implements ISingleSelectable {
	public static Logger logger = LoggerFactory.getLogger(SingleSelect.class);
	
	private String dataBase;
	
	private String table;
	
	private String id;
	
	public SingleSelect(String database){
		this(database,database);
	}
	
	public SingleSelect(String dataBase, String table){
		this.dataBase = dataBase;
		this.table = table;
	}

	@Override
	public <T> T getEntityById(String id,Class<T> clazz){
		if(StringUtils.isEmpty(id)){
			throw new NoneIdException();
		}
		this.id = id;
		GetResponse getResponse = query();
		if(null != getResponse){
			Map<String,Object> data = getResponse.getSource();
			BeanCopierUtil bcu = new BeanCopierUtil();
			T p = bcu.mapToObject(data, clazz);
			return p;
		}
		return null;
	}
	
	@Override
	public Map<String,Object> getMapById(String id){
		if(StringUtils.isEmpty(id)){
			throw new NoneIdException();
		}
		this.id = id;
		GetResponse getResponse = query();
		if(null != getResponse){
			return getResponse.getSource();
		}
		return null;
	}
	
	private GetResponse query(){
		GetResponse gr = null;
		try {
			RestHighLevelClient client = getClient();
			GetRequest getRequest = new GetRequest(dataBase,table,id);
			
			gr = client.get(getRequest, RequestOptions.DEFAULT);
			return gr;
		} catch (IOException e1) {
			logger.error("IOException error", e1);
			return null;
		} catch (Exception ex){
			logger.error("other error", ex);
			return null;
		} finally {
			returnBack();
		}
	}
}
