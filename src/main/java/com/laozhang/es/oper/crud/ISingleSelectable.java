package com.laozhang.es.oper.crud;

import java.util.Map;

public interface ISingleSelectable {
	/**
	 * 根据id获取数据，并映射成对应clazz类实体
	 * @param id
	 * @param clazz
	 * @return
	 * @throws Exception
	 */
	<T> T getEntityById(String id,Class<T> clazz) throws Exception;
	/**
	 * 根据id获取数据，得到一个map
	 * @param id
	 * @return
	 * @throws Exception
	 */
	Map<String,Object> getMapById(String id) throws Exception;
}
