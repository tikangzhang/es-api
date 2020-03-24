package com.laozhang.es.oper.crud;

import org.elasticsearch.client.Request;
import com.laozhang.es.base.ILowLevelClient;

public interface IUpdateByQuery extends ILowLevelClient{
	/**
	 * @body 更新json串
	 */
	IUpdateByQuery update(String body);
	/**
	 * @request 更新请求
	 */
	IUpdateByQuery update(Request request);
}
