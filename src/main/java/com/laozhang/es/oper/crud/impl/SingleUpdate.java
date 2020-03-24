package com.laozhang.es.oper.crud.impl;

import java.io.IOException;
import java.util.Map;

import com.laozhang.es.oper.crud.SingleRequestExecutor;
import com.laozhang.es.base.entities.Entity;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import com.alibaba.fastjson.JSON;
import com.laozhang.es.oper.crud.ISingleUpdatable;
import com.laozhang.es.base.exception.NoRecordException;
import com.laozhang.es.base.exception.NoneIdException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SingleUpdate extends SingleRequestExecutor implements ISingleUpdatable{
	public static Logger logger = LoggerFactory.getLogger(SingleUpdate.class);
	
	private String dataBase;
	
	private String table;
	
	public SingleUpdate(String database){
		this(database,database);
	}
	
	public SingleUpdate(String dataBase, String table){
		this.dataBase = dataBase;
		this.table = table;
	}
	
	@Override
	final public boolean update(Entity e) {
		UpdateRequest ur = getRequest(e);
		return updating(ur);
	}

	final public boolean updateNow(Entity e){
		UpdateRequest ur = getRequest(e);
		ur.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
		return updating(ur);
	}

	final public boolean updateWaitForRefresh(Entity e){
		UpdateRequest ur = getRequest(e);
		ur.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
		return updating(ur);
	}

	private UpdateRequest getRequest(Entity e){
		String curId = e.getId();
		if(StringUtils.isEmpty(curId)){
			throw new NoneIdException();
		}
		UpdateRequest ur = new UpdateRequest(dataBase,table,curId);
		String source = JSON.toJSONString(e);
		ur.doc(source, XContentType.JSON);
		return ur;
	}


	@Override
	final public boolean update(Map<String, Object> m) {
		UpdateRequest ur = getRequest(m);
		return updating(ur);
	}

	final public boolean updateNow(Map<String, Object> m){
		UpdateRequest ur = getRequest(m);
		ur.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
		return updating(ur);
	}

	final public boolean updateWaitForRefresh(Map<String, Object> m){
		UpdateRequest ur = getRequest(m);
		ur.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
		return updating(ur);
	}

	private UpdateRequest getRequest(Map<String, Object> m){
		String curId = (String)m.get("id");
		if(StringUtils.isEmpty(curId)){
			throw new NoneIdException();
		}
		m.remove("id");
		UpdateRequest ur = new UpdateRequest(dataBase,table,curId);
		ur.doc(m);
		return ur;
	}
	
	private boolean updating(UpdateRequest request){
		try {
			RestHighLevelClient client = getClient();
			UpdateResponse ir = client.update(request, RequestOptions.DEFAULT);
			Result result = ir.getResult();
			if(result == Result.UPDATED){
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
