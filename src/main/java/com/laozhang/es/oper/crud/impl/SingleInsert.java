package com.laozhang.es.oper.crud.impl;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import com.laozhang.es.base.entities.Entity;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import com.alibaba.fastjson.JSON;
import com.laozhang.es.oper.crud.ISingleInsertable;
import com.laozhang.es.oper.crud.SingleRequestExecutor;
import com.laozhang.es.base.exception.ConfictedExistRecordException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SingleInsert extends SingleRequestExecutor implements ISingleInsertable{
	public static Logger logger = LoggerFactory.getLogger(SingleInsert.class);
	
	private String dataBase;
	
	private String table;
	
	public SingleInsert(String database){
		this(database,database);
	}
	
	public SingleInsert(String dataBase, String table){
		this.dataBase = dataBase;
		this.table = table;
	}

	@Override
	public boolean save(Entity e) {
		String curId = e.getId();
		if(!StringUtils.isEmpty(curId)){
			GetRequest getRequest = new GetRequest(dataBase,table,curId);
			try {
				RestHighLevelClient client = getClient();
				GetResponse gr = client.get(getRequest, RequestOptions.DEFAULT);
				if(null != gr && gr.isExists()){
					throw new ConfictedExistRecordException("该记录已存在:" + e);
				}
			} catch (IOException e1) {
				logger.error("IOException error", e1);
				return false;
			} catch (Exception ex){
				logger.error("other error", ex);
				return false;
			} finally {
				returnBack();
			}
		}
		IndexRequest indexRequest = new IndexRequest(dataBase,table);
		String source = JSON.toJSONString(e);
		indexRequest.source(source, XContentType.JSON);
		return inserting(indexRequest,e);
	}

	@Override
	final public boolean saveOrUpdate(Entity e) {
		IndexRequest indexRequest = getRequest(e);
		return inserting(indexRequest,e);
	}

	final public boolean saveOrUpdateWaitForRefresh(Entity e) {
		IndexRequest indexRequest = getRequest(e);
		indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
		return inserting(indexRequest,e);
	}
	final public boolean saveOrUpdateNow(Entity e) {
		IndexRequest indexRequest = getRequest(e);
		indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
		return inserting(indexRequest,e);
	}

	private IndexRequest getRequest(Entity e){
		String curId = e.getId();
		IndexRequest indexRequest;
		if(StringUtils.isEmpty(curId)){
			indexRequest = new IndexRequest(dataBase,table);
		}else{
			indexRequest = new IndexRequest(dataBase,table,curId);
		}
		String source = JSON.toJSONString(e);
		indexRequest.source(source, XContentType.JSON);
		return indexRequest;
	}

	@Override
	final public boolean saveOrUpdate(Map<String, Object> m) {
		IndexRequest indexRequest = getRequest(m);
		return inserting(indexRequest,m);
	}
	final public boolean saveOrUpdateWaitForRefresh(Map<String, Object> m) {
		IndexRequest indexRequest = getRequest(m);
		indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
		return inserting(indexRequest,m);
	}
	final public boolean saveOrUpdateNow(Map<String, Object> m) {
		IndexRequest indexRequest = getRequest(m);
		indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
		return inserting(indexRequest,m);
	}
	private IndexRequest getRequest(Map<String, Object> m){
		String curId = UUID.randomUUID().toString();
		IndexRequest indexRequest = new IndexRequest (dataBase,table,curId);
		indexRequest.source(m);
		return indexRequest;
	}

	private boolean inserting(IndexRequest request,Entity e){
		try {
			RestHighLevelClient client = getClient();
			IndexResponse ir = client.index(request, RequestOptions.DEFAULT);
			Result result = ir.getResult();
			if(result == Result.CREATED || result == Result.UPDATED){
				e.setId(ir.getId());
				return true;
			}
		} catch (IOException e1) {
			logger.error("IOException error", e1);
			return false;
		} catch (Exception ex){
			logger.error("other error", ex);
		} finally {
			returnBack();
		}
		return false;
	}
	
	private boolean inserting(IndexRequest request,Map<String, Object> e){
		try {
			RestHighLevelClient client = getClient();
			IndexResponse ir = client.index(request, RequestOptions.DEFAULT);
			Result result = ir.getResult();
			if(result == Result.CREATED || result == Result.UPDATED){
				e.put("id",ir.getId());
				return true;
			}
		} catch (IOException e1) {
			logger.error("IOException error", e1);
			return false;
		} catch (Exception ex){
			logger.error("other error", ex);
		} finally {
			returnBack();
		}
		return false;
	}
}
