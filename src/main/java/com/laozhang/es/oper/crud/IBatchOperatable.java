package com.laozhang.es.oper.crud;

import com.laozhang.es.base.entities.Entity;

import java.util.List;
import java.util.Map;

/**
 * 只包括自动ID的对象
 *
 */
public interface IBatchOperatable {
	/**
	 * 增加插入实体操作。使用es自生id
	 */
	IBatchOperatable addInsert(Entity e);
	IBatchOperatable addInsertForListEntity(List<? extends Entity> list);
	IBatchOperatable addInsertForArrayEntity(Entity[] list);
	/**
	 * 增加插入map操作。使用es自生id
	 */
	IBatchOperatable addInsert(Map<String,Object> e);
	IBatchOperatable addInsertForListMap(List<Map<String,Object>> list);
	/**
	 * 增加更新实体操作。
	 */
	IBatchOperatable addUpdate(Entity e);
	IBatchOperatable addUpdateForListEntity(List<? extends Entity> list);
	IBatchOperatable addUpdateForArrayEntity(Entity[] list);
	/**
	 * 增加更新map操作。
	 */
	IBatchOperatable addUpdate(Map<String,Object> e);
	IBatchOperatable addUpdateForListMap(List<Map<String,Object>> list);
	/**
	 * 增加使用id删除。
	 */
	IBatchOperatable addDelete(String ids);
	IBatchOperatable addDeleteForListStr(List<String> list);
	IBatchOperatable addDeleteForArrayStr(String...list);
	/**
	 * 增加使用实体id删除。
	 */
	IBatchOperatable addDelete(Entity e);
	IBatchOperatable addDeleteForListEntity(List<Entity> e);
	IBatchOperatable addDeleteForListMap(List<Map<String,Object>> e);
	/**
	 * 同步执行，返回成功操作数
	 * 返回-1 批量执行异常或失败
	 */
	int excute();
	/**
	 * 同步执行，不统计成功数，只快速获取结果
	 * 返回 true 全部成功
	 * 返回false 部分成功或全部失败
	 */
	boolean excuteSuccOrNot();
	/**
	 * 同步执行，不强求结果
	 */
	void excuteSyc();
	/**
	 * 异步执行，不强求结果，直接快速返回
	 */
	void excuteAsyc();
	/**
	 * 是否有存在的批量任务
	 */
	boolean batched();
}
