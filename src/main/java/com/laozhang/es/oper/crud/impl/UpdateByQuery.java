package com.laozhang.es.oper.crud.impl;

import java.io.IOException;

import com.laozhang.es.oper.crud.IUpdateByQuery;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;

import com.laozhang.es.base.LowLevelExecutor;

public class UpdateByQuery extends LowLevelExecutor implements IUpdateByQuery {

	public UpdateByQuery(String index, String type){
		super(index,type);
	}

	@Override
	public UpdateByQuery update(String body) {
		convertJson2Request(body);
		doUpdate(this.request);
		return this;
	}

	public UpdateByQuery updateWaitForRefresh(String body) {
		convertJson2Request(body);
		updateWaitForRefresh(this.request);
		return this;
	}

	public UpdateByQuery updateNow(String body) {
		convertJson2Request(body);
		updateNow(this.request);
		return this;
	}


	@Override
	public UpdateByQuery update(Request request) {
		doUpdate(request);
		return this;
	}

	public void updateWaitForRefresh(Request request) {
		request.addParameter("refresh","wait_for");
		doUpdate(request);
	}
	public void updateNow(Request request) {
		request.addParameter("refresh","true");
		doUpdate(request);
	}

	private void doUpdate(Request request){
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
	}

	private void convertJson2Request(String body){
		this.request = new Request(DEFAULT_METHOD, getEndpoint(UPDATE_BY_QUERY_ENDPOINT));
		this.request.setJsonEntity(body);
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
