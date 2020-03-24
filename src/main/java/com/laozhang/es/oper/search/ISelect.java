package com.laozhang.es.oper.search;

import java.util.List;
import java.util.Map;

import org.elasticsearch.action.search.SearchResponse;

public interface ISelect {
	/**
	 * 选择列，不使用该方法则返回所有列
	 * @param cols
	 * @return
	 */
	ISelect SelectCols(String... cols);
	/**
	 * 设置返回数据集的开始索引
	 * @param start
	 * @return
	 */
	ISelect From(int start);
	/**
	 * 设置返回数据集的大小
	 * @param size
	 * @return
	 */
	ISelect Size(int size);
	/**
	 * 按某列排序。按多列复合排序时，需调用多次
	 * @param desc
	 * @return
	 */
	ISelect OrderBy(String col, boolean desc);
	
	<T> T getEntity(Class<T> eClass);
	
	<T> List<T> getEntityList(Class<T> eClass);
	
	List<Map<String,Object>> getMapList();
	
	String getSourceStr();
	
	SearchResponse getSource();
}
