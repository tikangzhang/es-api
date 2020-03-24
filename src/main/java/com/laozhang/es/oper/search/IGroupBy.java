package com.laozhang.es.oper.search;

import com.laozhang.es.base.condition.common.Arithmetic;
import java.util.Map;

public interface IGroupBy {
	/**
	 * 求和聚合
	 */
	IGroupBy Sum(String col);
	/**
	 * 求和聚合并设置别名
	 */
	IGroupBy Sum(String col, String alias);
	/**
	 * 求和聚合，Arithmetic计算类别，仅支持四则运算，即加减乘除
	 */
	IGroupBy Sum(String col, Arithmetic opType, Object opNum);
	IGroupBy Sum(String col, String alias, Arithmetic opType, Object opNum);
	/**
	 * 不推荐使用
	 * 求和聚合，使用脚本，针对每一条记录返回被操作的值。
	 * 例如:
	 * script：if(doc.machineNo.value == 'A02'){return doc.logtimediff.value}else{return 0}
	 * 求machineNo=A02的所有记录的logtimediff之和
	 */
	IGroupBy Sum(String col, String alias, String script);
	/**
	 * 推荐使用
	 */
	IGroupBy Sum(String col, String alias, String script, Map<String, Object> params);
	/**
	 * 求平均聚合
	 */
	IGroupBy Avg(String col);
	IGroupBy Avg(String col, String alias);
	/**
	 * 求平均聚合，Arithmetic计算类别，仅支持四则运算，即加减乘除
	 */
	IGroupBy Avg(String col, Arithmetic opType, Object opNum);
	IGroupBy Avg(String col, String alias, Arithmetic opType, Object opNum);
	/**
	 * 不推荐使用
	 * 求平均聚合，使用脚本，针对每一条记录返回被操作的值。
	 * 例如:
	 * script：if(doc.machineNo.value == 'A02'){return doc.logtimediff.value}else{return 0}
	 * 求machineNo=A02的所有记录的logtimediff平均值
	 */
	IGroupBy Avg(String col, String alias, String script);
	/**
	 * 推荐使用
	 */
	IGroupBy Avg(String col, String alias, String script, Map<String, Object> params);
	/**
	 * 求最大聚合
	 */
	IGroupBy Max(String col);
	IGroupBy Max(String col, String alias);
	/**
	 * 求最大聚合，Arithmetic计算类别，仅支持四则运算，即加减乘除
	 */
	IGroupBy Max(String col, Arithmetic opType, Object opNum);
	IGroupBy Max(String col, String alias, Arithmetic opType, Object opNum);
	/**
	 * 不推荐使用
	 * 求最大聚合，使用脚本，针对每一条记录返回被操作的值。
	 * 例如:
	 * script：if(doc.machineNo.value == 'A02'){return doc.logtimediff.value}else{return 0}
	 * 求machineNo=A02的所有记录的logtimediff最大值
	 */
	IGroupBy Max(String col, String alias, String script);
	/**
	 * 推荐使用
	 */
	IGroupBy Max(String col, String alias, String script, Map<String, Object> params);
	/**
	 * 求最小聚合
	 */
	IGroupBy Min(String col);
	IGroupBy Min(String col, String alias);
	/**
	 * 求最小聚合，Arithmetic计算类别，仅支持四则运算，即加减乘除
	 */
	IGroupBy Min(String col, Arithmetic opType, Object opNum);
	IGroupBy Min(String col, String alias, Arithmetic opType, Object opNum);
	/**
	 * 不推荐使用
	 * 求最小聚合，使用脚本，针对每一条记录返回被操作的值。
	 * 例如:
	 * script：if(doc.machineNo.value == 'A02'){return doc.logtimediff.value}else{return 0}
	 * 求machineNo=A02的所有记录的logtimediff最小值
	 */
	IGroupBy Min(String col, String alias, String script);
	/**
	 * 推荐使用
	 */
	IGroupBy Min(String col, String alias, String script, Map<String, Object> params);
	/**
	 * 统计记录数聚合
	 */
	IGroupBy Count(String col);
	IGroupBy Count(String col, String alias);
	/**
	 * 统计唯一记录数聚合
	 */
	IGroupBy DistinctCount(String col);
	IGroupBy DistinctCount(String col, String alias);
}
