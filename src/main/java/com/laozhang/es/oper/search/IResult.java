package com.laozhang.es.oper.search;

import com.laozhang.es.base.utils.page.PageHelper;

import java.util.List;
import java.util.Map;

public interface IResult {
	/**
	 * 获取顶级聚合结果，返回其中某个键的值
	 * es 固定给单值统一为double类型
	 */
	double getSingleResult(String key);
	/**
	 * 获取聚合结果，返回List<Map<String,Object>>
	 */
	List<Map<String,Object>> getListMap();
	/**
	 * 根据聚合路径深度，获取路径截止的聚合结果，返回List<Map<String,Object>>
	 */
	List<Map<String,Object>> getListMap(String path);
	/**
	 * 提供分页，获取聚合结果，返回List<Map<String,Object>>
	 * from 开始
	 * to 结束但不包括
	 */
	List<Map<String,Object>> getListMap(int from, int to);
	/**
	 * 提供分页，获取聚合结果，返回List<Map<String,Object>>
	 */
	List<Map<String,Object>> getListMap(PageHelper helper);
	/**
	 * 获取第一个聚合结果，返回时自动映射为指定实体
	 */
	<T> T getEntity(Class<T> eClass);
	/**
	 * 获取聚合结果，返回时自动映射为指定实体集
	 */
	<T> List<T> getEntityList(Class<T> eClass);
	/**
	 * 提供分页，获取聚合结果，返回时自动映射为指定实体集
	 * from 开始
	 * to 结束但不包括
	 */
	<T> List<T> getEntityList(Class<T> eClass, int from, int to);
	/**
	 * 提供分页，获取聚合结果，返回时自动映射为指定实体集
	 */
	<T> List<T> getEntityList(Class<T> eClass, PageHelper helper);
}
