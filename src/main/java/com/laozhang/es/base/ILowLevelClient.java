package com.laozhang.es.base;

import org.elasticsearch.client.Response;

public interface ILowLevelClient {
	/**
	* 返回转json串
	*/
	String getJson();
	/**
	* 返回响应对象
	*/
	Response getResponse();
}

