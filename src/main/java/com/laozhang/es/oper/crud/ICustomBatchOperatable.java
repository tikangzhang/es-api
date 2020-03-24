package com.laozhang.es.oper.crud;

import com.laozhang.es.base.entities.Entity;

import java.util.Map;

/**
 * 包括自定义ID的对象或自动ID的对象
 *
 */
public interface ICustomBatchOperatable extends IBatchOperatable{
	
	ICustomBatchOperatable addInsertWithoutId(Entity e);
	
	ICustomBatchOperatable addInsertWithoutId(Map<String,Object> e);
	
	ICustomBatchOperatable addUpdate(Entity e);
	
	ICustomBatchOperatable addUpdate(Map<String,Object> e);
	
	ICustomBatchOperatable addDelete(String id);
	
	ICustomBatchOperatable addDelete(Entity e);
	
	ICustomBatchOperatable addDelete(Map<String,Object> e);
}
