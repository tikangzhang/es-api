package com.laozhang.es.oper.search.time;

public interface ITimeRangeGroupByTable {
	/**
	 * 按某列进行统计
	 * @param col 列名
	 * @return
	 */
	TimeRangeGroupByTable StatBy(String col);
	/**
	 * 添加开始( >= Start)
	 * @param Start 范围开始
	 * @param alias 别名
	 * @return
	 */
	TimeRangeGroupByTable AddStart(String Start, String alias);
	/**
	 * 添加结束( < End)
	 * @param End 范围结束
	 * @param alias 别名
	 * @return
	 */
	TimeRangeGroupByTable AddEnd(String End, String alias);
	/**
	 * 添加范围( >= Start and < End )
	 * @param Start 范围开始
	 * @param End 范围结束
	 * @param alias 别名
	 * @return
	 */
	TimeRangeGroupByTable AddRange(String Start, String End, String alias);
	/**
	 * 设置每条结果的主键，可以不设。不设则不返回主键值
	 * @param key 主键
	 * @return
	 */
	TimeRangeGroupByTable setMainKey(String key);
}
