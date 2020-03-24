package com.laozhang.es.base.condition.aggs;

import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.sum.SumAggregationBuilder;

import com.laozhang.es.base.condition.common.Arithmetic;

import java.util.Map;

public class AggSumCondition extends AggCondition{
	private SumAggregationBuilder qb;
	
	public final static String DEFAULT_ALIAS_PREFIX = "sum_";
	
	public AggSumCondition(String colName){
		this.qb = AggregationBuilders.sum(DEFAULT_ALIAS_PREFIX.concat(colName)).field(colName);
	}
	
	public AggSumCondition(String colName,String alias){
		this.qb = AggregationBuilders.sum(alias).field(colName);
	}
	
	public AggSumCondition(String colName,String alias,String script){
		this.qb = AggregationBuilders.sum(alias).script(getScript(script));
	}

	public AggSumCondition(String colName, String alias, String script, Map<String,Object> params){
		this.qb = AggregationBuilders.sum(alias).script(getScript(script,params));
	}

	public AggSumCondition(String colName,String alias,Arithmetic c,Object o){
		this.qb = AggregationBuilders.sum(alias).field(colName).script(getScript(c,o));
	}
	
	@Override
	public AggregationBuilder getCondition() {
		return this.qb;
	}

	@Override
	public String getName() {
		return this.qb.getName();
	}
}
