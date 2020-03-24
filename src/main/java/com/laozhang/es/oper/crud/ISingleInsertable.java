package com.laozhang.es.oper.crud;

import com.laozhang.es.base.entities.Entity;

import java.util.Map;

public interface ISingleInsertable {
	/**
	 * 插入前会校验实体id是否存在，存在则跑出异常，否则插入。
	 * @param e
	 * @return
	 * @throws Exception
	 */
	boolean save(Entity e) throws Exception;
	/**
	 * 如果实体id不为空，则直接更新存储的记录，否则以新纪录插入。
	 * @param e
	 * @return
	 * @throws Exception
	 */
	boolean saveOrUpdate(Entity e) throws Exception;
	/**
	 * 插入Map。如果map有id键值对，则会把id键值对当普通字段插入。
	 * @param m
	 * @return
	 * @throws Exception
	 */
	boolean saveOrUpdate(Map<String,Object> m) throws Exception;
}
