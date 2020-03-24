package com.laozhang.es.oper.search.time;

import com.laozhang.es.base.condition.common.Arithmetic;

public interface IGroupByForDateRange {
	/**
	 * 选择聚合需要的其他列(不排序)
	 * @param alias 
	 * @return
	 */
	IGroupByForDateRange SelectAggCols(String... alias);
	
	IGroupByForDateRange SelectAggCustomCols(String alias, String Script, String... cols);
	/**
	 * 选择聚合需要的其他列,提供排序
	 * @param orderedAlias 需要的其他列
	 * @param desc true递减 false递增
	 * @param alias
	 * @return
	 */
	IGroupByForDateRange SelectOrderedAggCols(String orderedAlias, boolean desc, String... alias);
	
	IGroupByForDateRange SelectOrderedAggCustomCols(String alias, String Script, String orderedCol, boolean desc, String... cols);
	
	IGroupByForDateRange setAliasMap(String col, String alias);
	/**
	 * 求和聚合
	 * @param col 聚合的列
	 * @return
	 */
	IGroupByForDateRange Sum(String col);
	/**
	 * 求和聚合
	 * @param col 聚合的列
	 * @param opType 操作类别
	 * @param opNum 操作数
	 * @return
	 */
	IGroupByForDateRange Sum(String col, Arithmetic opType, Object opNum);
	/**
	 * 求和聚合
	 * @param col 聚合的列
	 * @param script painless脚本字符串
	 * @return
	 */
	IGroupByForDateRange Sum(String col, String script);
	/**
	 * 求平均聚合
	 * @param col 聚合的列
	 * @return
	 */
	IGroupByForDateRange Avg(String col);
	IGroupByForDateRange Avg(String col, String script);
	IGroupByForDateRange Avg(String col, Arithmetic opType, Object opNum);
	/**
	 * 求最大聚合
	 * @param col 聚合的列
	 * @return
	 */
	IGroupByForDateRange Max(String col);
	IGroupByForDateRange Max(String col, String script);
	IGroupByForDateRange Max(String col, Arithmetic opType, Object opNum);
	/**
	 * 求最小聚合
	 * @param col 聚合的列
	 * @return
	 */
	IGroupByForDateRange Min(String col);
	IGroupByForDateRange Min(String col, String script);
	IGroupByForDateRange Min(String col, Arithmetic opType, Object opNum);
	/**
	 * 统计记录数聚合
	 * @param col 聚合的列
	 * @return
	 */
	IGroupByForDateRange Count(String col);
	/**
	 * 统计唯一记录数聚合
	 * @param col 聚合的列
	 * @return
	 */
	IGroupByForDateRange DistinctCount(String col);
	/**
	 * 聚合列长度,不设默认10条
	 * @param size
	 * @return
	 */
	IGroupByForDateRange AggSize(Integer size);
}
