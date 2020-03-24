package com.laozhang.es.oper.search.time;

import com.laozhang.es.oper.search.IGroupBy;

public interface ITimePeriodGroupByTable {
	/**
	 * 按年统计
	 * @return
	 */
	ITimePeriodGroupByTable StatByYear(String col);
	/**
	 * 按月统计
	 * @return
	 */
	ITimePeriodGroupByTable StatByMonth(String col);
	/**
	 * 按周统计
	 * @return
	 */
	ITimePeriodGroupByTable StatByWeek(String col);
	/**
	 * 按日统计
	 * @return
	 */
	ITimePeriodGroupByTable StatByDay(String col);
	/**
	 * es聚合排序
	 * @param alias 别名
	 * @param desc 排序方向
	 * @return
	 */
	ITimePeriodGroupByTable OrderBy(String alias, boolean desc);
	/**
	 * 聚合列长度,不设默认10条
	 * @param size
	 * @return
	 */
	IGroupBy AggSize(Integer size);
	/**
	 * 设置每条结果的主键，可以不设。不设则不返回主键值
	 * @param key 主键
	 * @return
	 */
	ITimePeriodGroupByTable setMainKey(String key);
	/**
	 * 开始自定义列构造，选择聚合需要的其他列,使用时与SelectAggScriptCols互斥，二取其一
	 */
	IGroupBy SelectAggCols(String... alias);
	/**
	 * 开始自定义列构造，支持脚本形式,使用时与SelectAggCols互斥，二取其一
	 */
	IGroupBy SelectAggScriptCols(String alias, String Script);
	/**
	 * 为选择聚合需要的其他列提供排序
	 */
	IGroupBy OrderAggCols(String orderedCol, boolean desc);
	/**
	 * 跟在SelectAggCols或SelectAggScriptCols后用于追加脚本自定义列，无限制
	 */
	IGroupBy AppendAggScriptCol(String alias, String script);
	/**
	 * 配合SelectAggCols系列使用，为列设置一个映射别名
	 */
	IGroupBy setAliasMap(String col, String alias);
	/**
	 * 结束自定义列构造
	 */
	IGroupBy EndSelectAggCols();
}
