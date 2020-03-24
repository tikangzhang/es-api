package com.laozhang.es.base.condition.aggs;

import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCountAggregationBuilder;

import com.laozhang.es.base.condition.common.Arithmetic;

public class AggCountCondition extends AggCondition{
	private ValueCountAggregationBuilder qb;
	
	public final static String DEFAULT_ALIAS_PREFIX = "count_";
	
	public AggCountCondition(String colName){
		this.qb = AggregationBuilders.count(DEFAULT_ALIAS_PREFIX.concat(colName)).field(colName);
	}
	
	public AggCountCondition(String colName,String alias){
		this.qb = AggregationBuilders.count(alias).field(colName);
	}
	
	public AggCountCondition(String colName,String alias,Arithmetic c,Object o){
		this.qb = AggregationBuilders.count(alias).field(colName).script(getScript(c,o));
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
