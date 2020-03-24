package com.laozhang.es.oper.crud.impl;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import com.laozhang.es.oper.crud.SingleRequestExecutor;
import com.laozhang.es.base.entities.Entity;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import com.alibaba.fastjson.JSON;
import com.laozhang.es.oper.crud.ISingleInsertable;
import com.laozhang.es.base.exception.ConfictedExistRecordException;

@Deprecated
public class SingleCustomInsert extends SingleRequestExecutor implements ISingleInsertable{
	private String dataBase;
	
	private String table;
	
	public SingleCustomInsert(String database){
		this(database,database);
	}
	
	public SingleCustomInsert(String dataBase, String table){
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
				GetResponse gr = client.get(getRequest);
				if(null != gr && gr.isExists()){
					throw new ConfictedExistRecordException("该记录已存在:" + e);
				}
			} catch (IOException e1) {
				return false;
			} finally {
				returnBack();
			}
		}else{
			curId = UUID.randomUUID().toString();
		}

		IndexRequest indexRequest = new IndexRequest (dataBase,table,curId);
		String source = JSON.toJSONString(e);
		indexRequest.source(source, XContentType.JSON);
		return requesting(indexRequest,curId,e);
	}

	@Override
	public boolean saveOrUpdate(Entity e) {
		String curId = e.getId();
		if(StringUtils.isEmpty(curId)){
			curId = UUID.randomUUID().toString();
		}
		IndexRequest indexRequest = new IndexRequest(dataBase,table,curId);
		String source = JSON.toJSONString(e);
		indexRequest.source(source, XContentType.JSON);
		return requesting(indexRequest,curId,e);
	}

	@Override
	public boolean saveOrUpdate(Map<String, Object> m) {
		String curId = UUID.randomUUID().toString();
		IndexRequest indexRequest = new IndexRequest (dataBase,table,curId);
		indexRequest.source(m);
		return requesting(indexRequest,curId,m);
	}
	
	private boolean requesting(IndexRequest request,String id,Entity e){
		try {
			RestHighLevelClient client = getClient();
			IndexResponse ir = client.index(request);
			Result result = ir.getResult();
			if(result == Result.CREATED || result == Result.UPDATED){
				e.setId(id);
				return true;
			}
		} catch (IOException e1) {
			return false;
		}finally {
			returnBack();
		}
		return false;
	}
	
	private boolean requesting(IndexRequest request,String id,Map<String, Object> e){
		try {
			RestHighLevelClient client = getClient();
			IndexResponse ir = client.index(request);
			Result result = ir.getResult();
			if(result == Result.CREATED || result == Result.UPDATED){
				e.put("id",id);
				return true;
			}
		} catch (IOException e1) {
			return false;
		}finally {
			returnBack();
		}
		return false;
	}
}
