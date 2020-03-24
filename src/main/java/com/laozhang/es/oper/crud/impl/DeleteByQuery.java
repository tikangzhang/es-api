package com.laozhang.es.oper.crud.impl;

import java.io.IOException;

import com.laozhang.es.oper.crud.IDeleteByQuery;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;

import com.laozhang.es.base.LowLevelExecutor;

public class DeleteByQuery extends LowLevelExecutor implements IDeleteByQuery {

	public DeleteByQuery(String index, String type){
		super(index,type);
	}

	@Override
	public DeleteByQuery delete(String body) {
		try {
			this.request = new Request(DEFAULT_METHOD, getEndpoint(DELETE_BY_QUERY_ENDPOINT));
			this.request.setJsonEntity(body);
			this.response = this.client.performRequest(this.request);
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
		}finally {
			if(this.client != null){
				try {
					this.client.close();
				} catch (IOException e) {
					logger.error(e.getMessage(), e);
				}
			}
		}
		return this;
	}
	@Override
	public DeleteByQuery delete(Request request) {
		try {
			this.response = this.client.performRequest(request);
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
		} finally {
			if(this.client != null){
				try {
					this.client.close();
				} catch (IOException e) {
					logger.error(e.getMessage(), e);
				}
			}
		}
		return this;
	}
	@Override
	public Response getResponse() {
		return this.response;
	}
	@Override
	public String getJson() {
		return responseToJson();
	}
}
