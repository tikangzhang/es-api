package com.laozhang.es.oper.crud;

import com.laozhang.es.base.entities.Entity;

import java.util.Map;

public interface ISingleUpdatable {
	/**
	 * 使用实体数据更新，id为空报错
	 * @param e
	 * @return
	 */
	boolean update(Entity e);
	/**
	 * 使用map更新，id为空报错
	 * @param m
	 * @return
	 */
	boolean update(Map<String,Object> m);
}
