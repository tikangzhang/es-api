package com.laozhang.es.oper.search.simple;

public interface ISimpleGroupByTable {
	/**
	 * 需要聚合的列,同时指定别名，将属于不同分组的结果集装进对应容器
	 * @param alias
	 * @return
	 */
	ISimpleGroupByTable GroupByWithAlias(String col, String alias);
	/**
	 * 需要聚合的列,不需要别名，将属于不同分组的结果集装进对应容器
	 * @return
	 */
	ISimpleGroupByTable GroupBy(String... cols);
	/**
	 * 按聚合列名排序，只对统计聚合列有效
	 * @param alias 别名
	 * @param desc 排序方向
	 * @return
	 */
	ISimpleGroupByTable OrderBy(String alias, boolean desc);
	/**
	 * 按分组名排序
	 * @param desc 排序方向
	 * @return
	 */
	ISimpleGroupByTable OrderByGroupName(boolean desc);
	/**
	 * 按分组后的记录数排序
	 * @param desc 排序方向
	 * @return
	 */
	ISimpleGroupByTable OrderByGroupRecordCount(boolean desc);
	/**
	 * 聚合列长度,不设默认10条。
	 * ！！！这里是es的一个坑。查询带的这个size并不是指最后结果组的数量，而是每个分片的结果组的数量
	 * 因为每个分片都是数据的一部分，底层取top n数据的时候，每个分片可能都不一样，在合并的时候出现差异
	 * 所以在多分片的索引上做top n的时候不使用这个size
	 * @param size
	 * @return
	 */
	ISimpleGroupByTable AggSize(Integer size);
	/**
	 * 选择需要的其他列(即非聚合列。概念：既不是分组的列，也不是被聚合的列。),使用时与SelectAggScriptCols互斥，二取其一
	 */
	ISimpleGroupByTable SelectAggCols(String... alias);
	/**
	 * 使用脚本形式制造需要的其他自定义列(非聚合列),使用时与SelectAggCols互斥，二取其一
	 */
	ISimpleGroupByTable SelectAggScriptCols(String alias, String Script);
	/**
	 * 为非聚合列提供排序
	 */
	ISimpleGroupByTable OrderAggCols(String orderedCol, boolean desc);
	/**
	 * 跟在SelectAggCols或SelectAggScriptCols后用于追加脚本自定义列(非聚合列)，无限制
	 */
	ISimpleGroupByTable AppendAggScriptCol(String alias, String script);
	/**
	 * 配合SelectAggCols系列使用，为列设置一个映射别名
	 */
	ISimpleGroupByTable setAliasMap(String col, String alias);
	/**
	 * 结束自定义列构造
	 */
	ISimpleGroupByTable EndSelectAggCols();
}
