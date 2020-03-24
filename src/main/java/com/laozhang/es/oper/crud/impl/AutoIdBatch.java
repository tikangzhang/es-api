package com.laozhang.es.oper.crud.impl;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.laozhang.es.oper.crud.IBatchOperatable;
import com.laozhang.es.base.entities.Entity;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest.OpType;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.xcontent.XContentType;

import com.alibaba.fastjson.JSON;
import com.laozhang.es.base.Executor;
import com.laozhang.es.base.exception.NoneIdException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AutoIdBatch extends Executor implements IBatchOperatable {
	public static Logger logger = LoggerFactory.getLogger(AutoIdBatch.class);
	
	private String dataBase;
	
	private String table;
	
	private BulkRequest br;
	
	public AutoIdBatch(String database){
		this(database,database);
	}
	
	public AutoIdBatch(String dataBase, String table){
		this.dataBase = dataBase;
		this.table = table;
		this.br = new BulkRequest();
	}
	
	/**
	 * 插入需要自动生成ID的记录对象
	 */
	@Override
	public AutoIdBatch addInsert(Entity e) {
		if(null == e){
			return this;
		}
		IndexRequest indexRequest = new IndexRequest (dataBase,table);
		String source = JSON.toJSONString(e);
		indexRequest.source(source, XContentType.JSON);
		this.br.add(indexRequest);
		logger.debug("add insert entity:" + e);
		return this;
	}

	@Override
	public AutoIdBatch addInsertForListEntity(List<? extends Entity> list) {
		if(null != list) {
			for (Entity e : list) {
				IndexRequest indexRequest = new IndexRequest(dataBase, table);
				String source = JSON.toJSONString(e);
				indexRequest.source(source, XContentType.JSON);
				this.br.add(indexRequest);
			}
		}
		return this;
	}

	@Override
	public AutoIdBatch addInsertForArrayEntity(Entity[] list) {
		addInsertForListEntity(Arrays.asList(list));
		return this;
	}

	/**
	 * 插入需要自动生成ID的记录Map
	 */
	@Override
	public AutoIdBatch addInsert(Map<String, Object> e) {
		if(null == e){
			return this;
		}
		IndexRequest indexRequest = new IndexRequest (dataBase,table);
		indexRequest.source(JSON.toJSONString(e),XContentType.JSON);
		this.br.add(indexRequest);
		logger.debug("add insert map:" + e);
		return this;
	}

	@Override
	public AutoIdBatch addInsertForListMap(List<Map<String, Object>> list) {
		if(null != list) {
			for (Map<String, Object> e : list) {
				IndexRequest indexRequest = new IndexRequest (dataBase,table);
				indexRequest.source(JSON.toJSONString(e),XContentType.JSON);
				this.br.add(indexRequest);
			}
		}
		return this;
	}

	@Override
	public AutoIdBatch addUpdate(Entity e) {
		String curId = e.getId();
		if(StringUtils.isEmpty(curId)){
			throw new NoneIdException();
		}
		UpdateRequest ur = new UpdateRequest(dataBase,table,curId);
		String source = JSON.toJSONString(e);
		ur.doc(source, XContentType.JSON);
		this.br.add(ur);
		logger.debug("add update map:" + e);
		return this;
	}

	@Override
	public AutoIdBatch addUpdateForListEntity(List<? extends Entity> list) {
		if(null != list) {
			for (Entity e : list) {
				UpdateRequest ur = new UpdateRequest(dataBase,table,e.getId());
				ur.doc(JSON.toJSONString(e), XContentType.JSON);
				this.br.add(ur);
			}
		}
		return this;
	}

	@Override
	public AutoIdBatch addUpdateForArrayEntity(Entity[] list) {
		addUpdateForListEntity(Arrays.asList(list));
		return this;
	}

	@Override
	public AutoIdBatch addUpdate(Map<String, Object> m) {
		String curId = (String)m.get("id");
		if(StringUtils.isEmpty(curId)){
			throw new NoneIdException();
		}
		m.remove("id");
		UpdateRequest ur = new UpdateRequest(dataBase,table,curId);
		ur.doc(JSON.toJSONString(m),XContentType.JSON);
		this.br.add(ur);
		logger.debug("add update map:" + m);
		return this;
	}

	@Override
	public AutoIdBatch addUpdateForListMap(List<Map<String, Object>> list) {
		if(null != list) {
			String curId;
			for (Map<String, Object> m : list) {
				curId = (String)m.get("id");
				UpdateRequest ur = new UpdateRequest(dataBase,table,curId);
				m.remove("id");
				ur.doc(JSON.toJSONString(m),XContentType.JSON);
				this.br.add(ur);
			}
		}
		return this;
	}

	@Override
	public AutoIdBatch addDelete(String ids) {
		if(StringUtils.isEmpty(ids)){
			throw new NoneIdException();
		}
		DeleteRequest dr = new DeleteRequest(dataBase,table,ids);
		this.br.add(dr);
		logger.debug("add delete ids:" + ids);
		return this;
	}

	@Override
	public AutoIdBatch addDeleteForListStr(List<String> list) {
		if(null != list) {
			for (String ids : list) {
				DeleteRequest dr = new DeleteRequest(dataBase,table,ids);
				this.br.add(dr);
			}
		}
		return this;
	}

	@Override
	public AutoIdBatch addDeleteForArrayStr(String... list) {
		addDeleteForListStr(Arrays.asList(list));
		return this;
	}

	@Override
	public AutoIdBatch addDelete(Entity e) {
		if(StringUtils.isEmpty(e.getId())){
			throw new NoneIdException();
		}
		DeleteRequest dr = new DeleteRequest(dataBase,table,e.getId());
		this.br.add(dr);
		logger.debug("add delete from entity:" + e.getId());
		return this;
	}

	@Override
	public AutoIdBatch addDeleteForListEntity(List<Entity> list) {
		if(null != list) {
			for (Entity e : list) {
				DeleteRequest dr = new DeleteRequest(dataBase,table,e.getId());
				this.br.add(dr);
			}
		}
		return this;
	}

	@Override
	public AutoIdBatch addDeleteForListMap(List<Map<String, Object>> list) {
		if(null != list) {
			for (Map<String, Object> e : list) {
				DeleteRequest dr = new DeleteRequest(dataBase,table,(String)e.get("id"));
				e.remove("id");
				this.br.add(dr);
			}
		}
		return this;
	}

	/**
	 * 同步提交并获取成功条数，速度慢
	 */
	@Override
	public int excute() {
		int exeSuccCount = 0;
		BulkResponse bulkResponse;
		try {
			bulkResponse = getClient().bulk(this.br,RequestOptions.DEFAULT);
			for (BulkItemResponse bulkItemResponse : bulkResponse) { 
			    DocWriteResponse itemResponse = bulkItemResponse.getResponse(); 

			    if (itemResponse != null && bulkItemResponse.getOpType() == OpType.INDEX
			            || bulkItemResponse.getOpType() == OpType.CREATE) { 
			        IndexResponse indexResponse = (IndexResponse) itemResponse;
			        if(indexResponse.getResult() == Result.CREATED){
			        	exeSuccCount++;
			        }
			    } else if (itemResponse != null && bulkItemResponse.getOpType() == OpType.UPDATE) {
			        UpdateResponse updateResponse = (UpdateResponse) itemResponse;
			        if(updateResponse.getResult() == Result.UPDATED){
			        	exeSuccCount++;
			        }

			    } else if (itemResponse != null && bulkItemResponse.getOpType() == OpType.DELETE) {
			        DeleteResponse deleteResponse = (DeleteResponse) itemResponse;
			        if(deleteResponse.getResult() == Result.DELETED){
			        	exeSuccCount++;
			        }
			    }
			}
			return exeSuccCount;
		} catch (IOException e) {
			logger.error("IOException error.", e);
			return -1;
		}finally {
			returnBack();
		}
	}

	/**
	 * 同步提交并判断是否存在失败，速度略慢
	 */
	@Override
	public boolean excuteSuccOrNot() {
		BulkResponse bulkResponse;
		try {
			bulkResponse = getClient().bulk(this.br,RequestOptions.DEFAULT);
			if(bulkResponse.hasFailures()){
				return false;
			}
			return true;
		} catch (IOException e1) {
			logger.error("IOException error.", e1);
			return false;
		} catch (Exception ex){
			logger.error("other error.", ex);
			return false;
		} finally {
			returnBack();
		}
	}

	/**
	 * 同步提交，速度略快
	 */
	@Override
	public void excuteSyc() {
		try {
			getClient().bulk(this.br,RequestOptions.DEFAULT);
		} catch (IOException e1) {
			logger.error("IOException error.", e1);
		} catch (Exception ex){
			logger.error("other error.", ex);
		} finally {
			returnBack();
		}
	}
	
	/**
	 * 异步提交，速度最快
	 */
	@Override
	public void excuteAsyc() {
		try {
			getClient().bulkAsync(this.br,RequestOptions.DEFAULT,new  ActionListener<BulkResponse>(){

				@Override
				public void onFailure(Exception arg0) {
					logger.error("BulkAsync OnFailure.", arg0);
				}

				@Override
				public void onResponse(BulkResponse arg0) {
					logger.debug("BulkAsync onResponse return." + arg0.toString());
				}
				
			});
		} catch (Exception ex){
			logger.error("other error.", ex);
		} finally {
			returnBack();
		}
	}
	
	@Override
	public boolean batched(){
		return this.br.numberOfActions() != 0;
	}
}
