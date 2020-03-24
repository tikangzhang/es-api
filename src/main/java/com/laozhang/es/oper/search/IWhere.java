package com.laozhang.es.oper.search;

import java.util.Collection;

import com.laozhang.es.base.condition.BoolCondition;
import com.laozhang.es.base.condition.common.LogicRelation;

public interface IWhere {
	IWhere Where(BoolCondition bc);
	/**
	 * where 与条件
	 * @param k 键
	 * @param v 值
	 * @return
	 */
	<T> IWhere WhereAnd(String k, T v);
	//前缀匹配
	IWhere WhereAndPrefix(String k, String v);
	//通配匹配
	IWhere WhereAndWildcard(String k, String v);
	//正则匹配
	IWhere WhereAndRegexp(String k, String v);
	//模糊匹配
	IWhere WhereAndFuzzy(String k, String v);
	/**
	 * where 与 IN条件
	 * @param k 键
	 * @param vs 值数组
	 * @return
	 */
	<T> IWhere WhereAnd(String k, Object... vs);
	/**
	 * where 与 IN条件
	 * @param k 键
	 * @param vs 值集合
	 * @return
	 */
	<T> IWhere WhereAnd(String k, Collection<?> vs);
	/**
	 * where 与 复合条件
	 * @return
	 */
	<T> IWhere WhereAnd(BoolCondition condition);
	/**
	 * where 或条件
	 * @param k 键
	 * @param v 值
	 * @return
	 */
	<T> IWhere WhereOr(String k, T v);
	/**
	 * where 或 IN条件
	 * @param k 键
	 * @param vs 值数组
	 * @return
	 */
	<T> IWhere WhereOr(String k, Object... vs);
	/**
	 * where 或 IN条件
	 * @param k 键
	 * @param vs 值集合
	 * @return
	 */
	<T> IWhere WhereOr(String k, Collection<?> vs);
	/**
	 * where 或 复合条件
	 * @return
	 */
	<T> IWhere WhereOr(BoolCondition condition);
	//前缀匹配
	IWhere WhereOrPrefix(String k, String v);
	//通配匹配
	IWhere WhereOrWildcard(String k, String v);
	//正则匹配
	IWhere WhereOrRegexp(String k, String v);
	//模糊匹配
	IWhere WhereOrFuzzy(String k, String v);
	/**
	 * where 非条件
	 * @param k 键
	 * @param v 值
	 * @return
	 */
	<T> IWhere WhereNot(String k, T v);
	/**
	 * where 非 IN条件
	 * @param k 键
	 * @param vs 值数组
	 * @return
	 */
	<T> IWhere WhereNot(String k, Object... vs);
	/**
	 * where 非 IN条件
	 * @param k 键
	 * @param vs 值集合
	 * @return
	 */
	<T> IWhere WhereNot(String k, Collection<?> vs);
	/**
	 * where 非 复合条件
	 * @return
	 */
	<T> IWhere WhereNot(BoolCondition condition);
	//前缀匹配
	IWhere WhereNotPrefix(String k, String v);
	//通配匹配
	IWhere WhereNotWildcard(String k, String v);
	//正则匹配
	IWhere WhereNotRegexp(String k, String v);
	//模糊匹配
	IWhere WhereNotFuzzy(String k, String v);
	
	<T> IWhere OrRange(String k, LogicRelation relation, T to);
	<T> IWhere OrRange(String k, T from, T to);
	<T> IWhere OrRange(String k, T from, T to, String timeZone);
	/**
	 * where 范围条件
	 * @param k 键
	 * @param relation 枚举：>=,>,==,!=,<,<=
	 * @param to
	 * @return
	 */
	<T> IWhere WhereRange(String k, LogicRelation relation, T to);
	/**
	 * where 范围条件(>= from and < to)
	 * @param k 键
	 * @param from
	 * @param to
	 * @return
	 */
	<T> IWhere WhereRange(String k, T from, T to);
	/**
	 * where 范围条件(>= from and < to)
	 * @param k 键
	 * @param from
	 * @param to
	 * @param timeZone 时区
	 * @return
	 */
	<T> IWhere WhereRange(String k, T from, T to, String timeZone);
	/**
	 * where 判非null条件
	 * @param k
	 * @return
	 */
	IWhere WhereIsNotNull(String k);
	/**
	 * where 判null条件
	 * @param k
	 * @return
	 */
	IWhere WhereIsNull(String k);
}
