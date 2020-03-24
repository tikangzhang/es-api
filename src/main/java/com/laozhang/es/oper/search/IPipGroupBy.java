package com.laozhang.es.oper.search;

import com.laozhang.es.base.condition.common.LogicRelation;

public interface IPipGroupBy {
	/**
	 * 跟在GroupBy后，对其子级下聚合结果进行再计算。
	 * @param alias 聚合列名
	 * @param path 同级指定聚合列的层级路径。
	 */
	IPipGroupBy AggTopSum(String alias, String path);
	/**
	 * 同AggTopSum
	 */
	IPipGroupBy AggTopAvg(String alias, String path);
	/**
	 * 同AggTopSum
	 */
	IPipGroupBy AggTopMax(String alias, String path);
	/**
	 * 同AggTopSum
	 */
	IPipGroupBy AggTopMin(String alias, String path);
	/**
	 * 基于已统计聚合列的计算另做一列。强调：本列必须与被计算列是兄弟关系,必须跟在GroupBy之后
	 * @param alias 本列别名
	 * @param script 脚本。例如:xxxx / yyyy * 100	
	 * @param aggCols 引用到哪一个聚合列组 xxxx yyyy的值
	 * @return
	 */
	IPipGroupBy AggCalc(String alias, String script, String... aggCols);
	/**
	 * 类似sql 的having 对其子级分组进行筛选,必须跟在GroupBy之后
	 */
	<T> IPipGroupBy Having(String aggCol, LogicRelation relation, T value);
	/**
	 * 把子级统计结果排序,必须跟在GroupBy之后。！排序的列必须是个统计值
	 */
	IPipGroupBy OrderGroup(String aggCol, boolean isDesc);
	IPipGroupBy OrderGroup(String[] aggCols, boolean[] isDescs);
}
