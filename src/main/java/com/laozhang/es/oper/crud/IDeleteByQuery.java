package com.laozhang.es.oper.crud;

import org.elasticsearch.client.Request;

import com.laozhang.es.base.ILowLevelClient;

public interface IDeleteByQuery extends ILowLevelClient{
	/**
	 * @body 查询json串
	 */
	IDeleteByQuery delete(String body);
	/**
	 * @request 查出的查询请求
	 */
	IDeleteByQuery delete(Request request);
}
