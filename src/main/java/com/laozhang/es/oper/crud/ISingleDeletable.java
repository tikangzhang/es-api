package com.laozhang.es.oper.crud;

import com.laozhang.es.base.entities.Entity;

public interface ISingleDeletable {
	/**
	 * 使用id删除
	 * @return
	 */
	boolean delete(String id);
	/**
	 * 使用实体中的id删除
	 * @param e
	 * @return
	 */
	boolean delete(Entity e);
}
