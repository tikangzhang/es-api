package com.laozhang.es.oper.crud.impl;

import java.io.IOException;

import com.laozhang.es.base.entities.Entity;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;

import com.laozhang.es.oper.crud.ISingleDeletable;
import com.laozhang.es.oper.crud.SingleRequestExecutor;
import com.laozhang.es.base.exception.NoRecordException;
import com.laozhang.es.base.exception.NoneIdException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SingleDelete extends SingleRequestExecutor implements ISingleDeletable{
	public static Logger logger = LoggerFactory.getLogger(SingleDelete.class);
			
	private String dataBase;
	
	private String table;
	
	private String id;
	
	public SingleDelete(String database){
		this(database,database);
	}
	
	public SingleDelete(String dataBase, String table){
		this.dataBase = dataBase;
		this.table = table;
	}

	@Override
	public boolean delete(String id) {
		if(StringUtils.isEmpty(id)){
			throw new NoneIdException();
		}
		this.id = id;
		return deleting();
	}

	@Override
	public boolean delete(Entity e) {
		if(StringUtils.isEmpty(e.getId())){
			throw new NoneIdException();
		}
		this.id = e.getId();
		return deleting();
	}
	
	private boolean deleting(){
		try {
			DeleteRequest request = new DeleteRequest(dataBase,table,this.id);
			RestHighLevelClient client = getClient();
			DeleteResponse ir = client.delete(request, RequestOptions.DEFAULT);
			Result result = ir.getResult();
			if(result == Result.DELETED){
				return true;
			}
			if(result == Result.NOT_FOUND){
				throw new NoRecordException();
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
