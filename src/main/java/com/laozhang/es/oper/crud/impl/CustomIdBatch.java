package com.laozhang.es.oper.crud.impl;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.laozhang.es.oper.crud.ICustomBatchOperatable;
import com.laozhang.es.base.entities.Entity;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.DocWriteRequest.OpType;
import org.elasticsearch.action.ActionListener;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomIdBatch extends Executor implements ICustomBatchOperatable {
	public static Logger logger = LoggerFactory.getLogger(CustomIdBatch.class);
	
	private String dataBase;
	
	private String table;
	
	private BulkRequest br;
	
	public CustomIdBatch(String database){
		this(database,database);
	}
	
	public CustomIdBatch(String dataBase, String table){
		this.dataBase = dataBase;
		this.table = table;
		this.br = new BulkRequest();
	}
	
	/**
	 * 使用UUID生成自定义ID插入记录
	 */
	@Override
	public CustomIdBatch addInsert(Entity e) {
		if(null == e){
			return this;
		}
		String id = e.getId();
		if(StringUtils.isEmpty(id)){
			id = UUID.randomUUID().toString();
			e.setId(id);
		}
		IndexRequest indexRequest = new IndexRequest (dataBase,table,id);
		String source = JSON.toJSONString(e);
		indexRequest.source(source, XContentType.JSON);
		this.br.add(indexRequest);
		logger.debug("add insert entity:" + e);
		return this;
	}

	@Override
	public CustomIdBatch addInsertForListEntity(List<? extends Entity> list) {
		if(null != list) {
			for(Entity e : list) {
				IndexRequest indexRequest = new IndexRequest(dataBase, table, UUID.randomUUID().toString());
				String source = JSON.toJSONString(e);
				indexRequest.source(source, XContentType.JSON);
				this.br.add(indexRequest);
			}
		}
		return this;
	}

	@Override
	public CustomIdBatch addInsertForArrayEntity(Entity[] list) {
		addInsertForListEntity(Arrays.asList(list));
		return this;
	}

	/**
	 * 使用ES自动生成ID插入记录
	 */
	@Override
	public CustomIdBatch addInsertWithoutId(Entity e) {
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

	/**
	 * 使用UUID生成自定义ID插入Map记录
	 */
	@Override
	public CustomIdBatch addInsert(Map<String, Object> e) {
		if(null == e){
			return this;
		}
		String id = String.valueOf(e.get("id"));
		if(StringUtils.isEmpty(id)){
			id = UUID.randomUUID().toString();
			e.put("id",id);
		}
		IndexRequest indexRequest = new IndexRequest (dataBase,table,id);
		indexRequest.source(JSON.toJSONString(e),XContentType.JSON);
		this.br.add(indexRequest);
		logger.debug("add insert map:" + e);
		return this;
	}

	@Override
	public CustomIdBatch addInsertForListMap(List<Map<String, Object>> list) {
		if(null != list) {
			for(Map<String, Object> e : list) {
				IndexRequest indexRequest = new IndexRequest(dataBase, table, UUID.randomUUID().toString());
				e.remove("id");
				indexRequest.source(JSON.toJSONString(e),XContentType.JSON);
				this.br.add(indexRequest);
			}
		}
		return this;
	}

	public CustomIdBatch addInsert(String id, Map<String, Object> e) {
		if(null == e){
			return this;
		}
		if(StringUtils.isEmpty(id)){
			id = UUID.randomUUID().toString();
			e.put("id",id);
		}
		IndexRequest indexRequest = new IndexRequest (dataBase,table,id);
		indexRequest.source(JSON.toJSONString(e),XContentType.JSON);
		this.br.add(indexRequest);
		logger.debug("add insert map:" + e);
		return this;
	}
	
	/**
	 * 使用ES自动生成ID插入Map记录
	 */
	@Override
	public CustomIdBatch addInsertWithoutId(Map<String, Object> e) {
		if(null == e){
			return this;
		}
		IndexRequest indexRequest = new IndexRequest (dataBase,table);
		indexRequest.source(JSON.toJSONString(e),XContentType.JSON);
		this.br.add(indexRequest);
		logger.debug("add insert map:" + e);
		return this;
	}

	/**
	 * 更新记录，若对象id为空则不记入批量并记录错误
	 */
	@Override
	public CustomIdBatch addUpdate(Entity e) {
		if(null == e){
			return this;
		}
		String id = e.getId();
		if(StringUtils.isEmpty(id)){
			logger.error("None ID Entity:" + e);
			return this;
		}
		UpdateRequest ur = new UpdateRequest(dataBase,table,id);
		String source = JSON.toJSONString(e);
		ur.doc(source, XContentType.JSON);
		this.br.add(ur);
		logger.debug("add update entity:" + e);
		return this;
	}

	@Override
	public CustomIdBatch addUpdateForListEntity(List<? extends Entity> list) {
		if(null != list) {
			for(Entity e : list) {
				UpdateRequest ur = new UpdateRequest(dataBase,table,e.getId());
				String source = JSON.toJSONString(e);
				ur.doc(source, XContentType.JSON);
				this.br.add(ur);
			}
		}
		return this;
	}

	@Override
	public CustomIdBatch addUpdateForArrayEntity(Entity[] list) {
		addUpdateForListEntity(Arrays.asList(list));
		return this;
	}

	/**
	 * 更新记录，若Map对象id为空则不记入批量并记录错误
	 */
	@Override
	public CustomIdBatch addUpdate(Map<String, Object> e) {
		if(null == e){
			return this;
		}
		String id = String.valueOf(e.get("id"));
		if(StringUtils.isEmpty(id)){
			logger.error("None ID Map:" + e);
			return this;
		}
		UpdateRequest ur = new UpdateRequest(dataBase,table,id);
		ur.doc(JSON.toJSONString(e),XContentType.JSON);
		this.br.add(ur);
		logger.debug("add update map:" + e);
		return this;
	}

	@Override
	public CustomIdBatch addUpdateForListMap(List<Map<String, Object>> list) {
		if(null != list) {
			for(Map<String, Object> e : list) {
				UpdateRequest ur = new UpdateRequest(dataBase, table, (String)e.get("id"));
				ur.doc(JSON.toJSONString(e),XContentType.JSON);
				this.br.add(ur);
			}
		}
		return this;
	}

	/**
	 * 更新记录，若Map对象id为空则不记入批量并记录错误
	 */
	public CustomIdBatch addUpdate(String id, Map<String, Object> e) {
		if(null == e){
			return this;
		}
		if(StringUtils.isEmpty(id)){
			logger.error("None ID Map:" + e);
			return this;
		}
		UpdateRequest ur = new UpdateRequest(dataBase,table,id);
		ur.doc(JSON.toJSONString(e),XContentType.JSON);
		this.br.add(ur);
		logger.debug("add update map:" + e);
		return this;
	}

	/**
	 * 删除记录，若id为空则不记入批量并记录错误
	 */
	@Override
	public CustomIdBatch addDelete(String id) {
		if(StringUtils.isEmpty(id)){
			logger.error("None ID：" + id);
			return this;
		}
		DeleteRequest dr = new DeleteRequest(dataBase,table,id);
		this.br.add(dr);
		logger.debug("add delete id:" + id);
		return this;
	}

	@Override
	public CustomIdBatch addDeleteForListStr(List<String> list) {
		if(null != list) {
			for(String e : list) {
				DeleteRequest dr = new DeleteRequest(dataBase,table,e);
				this.br.add(dr);
			}
		}
		return this;
	}

	@Override
	public CustomIdBatch addDeleteForArrayStr(String... list) {
		addDeleteForListStr(Arrays.asList(list));
		return this;
	}

	/**
	 * 删除记录，若对象id为空则不记入批量并记录错误
	 */
	@Override
	public CustomIdBatch addDelete(Entity e) {
		if(null == e){
			return this;
		}
		String id = e.getId();
		if(StringUtils.isEmpty(id)){
			logger.error("None ID Entity：" + e);
			return this;
		}
		DeleteRequest dr = new DeleteRequest(dataBase,table,id);
		this.br.add(dr);
		logger.debug("add delete entity:" + e);
		return this;
	}

	@Override
	public CustomIdBatch addDeleteForListEntity(List<Entity> list) {
		if(null != list) {
			for(Entity e : list) {
				DeleteRequest dr = new DeleteRequest(dataBase,table,e.getId());
				this.br.add(dr);
			}
		}
		return this;
	}

	@Override
	public CustomIdBatch addDeleteForListMap(List<Map<String, Object>> list) {
		if(null != list) {
			for(Map<String, Object> e : list) {
				DeleteRequest dr = new DeleteRequest(dataBase,table,(String)e.get("id"));
				this.br.add(dr);
			}
		}
		return this;
	}

	/**
	 * 删除记录，若Map对象id为空则不记入批量并记录错误
	 */
	@Override
	public CustomIdBatch addDelete(Map<String, Object> e) {
		if(null == e){
			return this;
		}
		String id = String.valueOf(e.get("id"));
		if(StringUtils.isEmpty(id)){
			logger.error("None ID Map：" + e);
			return this;
		}
		DeleteRequest dr = new DeleteRequest(dataBase,table,id);
		this.br.add(dr);
		logger.debug("add delete map:" + e);
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

			    if (bulkItemResponse.getOpType() == OpType.INDEX
			            || bulkItemResponse.getOpType() == OpType.CREATE) { 
			        IndexResponse indexResponse = (IndexResponse) itemResponse;
			        if(indexResponse.getResult() == Result.CREATED){
			        	exeSuccCount++;
			        }
			    } else if (bulkItemResponse.getOpType() == OpType.UPDATE) { 
			        UpdateResponse updateResponse = (UpdateResponse) itemResponse;
			        if(updateResponse.getResult() == Result.UPDATED){
			        	exeSuccCount++;
			        }

			    } else if (bulkItemResponse.getOpType() == OpType.DELETE) { 
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
				logger.error("hasFailures:"+bulkResponse.buildFailureMessage());
				logger.error("hasFailures:"+bulkResponse.toString());
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
	
	@Override
	public boolean batched(){
		return this.br.numberOfActions() != 0;
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
	 * 异步提交，速度最快,即刻返回
	 */
	@Override
	public void excuteAsyc() {
		try {
			getClient().bulkAsync(this.br,RequestOptions.DEFAULT,new  ActionListener<BulkResponse>(){
				//下面两个方法暂时不知道拿来干嘛
				@Override
				public void onFailure(Exception arg0) {
				}

				@Override
				public void onResponse(BulkResponse arg0) {
					
				}
			});
		} catch (Exception ex){
			logger.error("other error", ex);
		} finally {
			returnBack();
		}
	}
}
