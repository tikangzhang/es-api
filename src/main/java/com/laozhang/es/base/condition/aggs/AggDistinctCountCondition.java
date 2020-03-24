package com.laozhang.es.base.condition.aggs;

import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.cardinality.CardinalityAggregationBuilder;

import com.laozhang.es.base.condition.common.Arithmetic;

public class AggDistinctCountCondition extends AggCondition{
	private CardinalityAggregationBuilder qb;
	
	public final static String DEFAULT_ALIAS_PREFIX = "distinct_";
	
	public AggDistinctCountCondition(String colName){
		this.qb = AggregationBuilders.cardinality(DEFAULT_ALIAS_PREFIX.concat(colName)).field(colName);
	}
	
	public AggDistinctCountCondition(String colName,String alias){
		this.qb = AggregationBuilders.cardinality(alias).field(colName);
	}
	
	public AggDistinctCountCondition(String colName,String alias,Arithmetic c,Object o){
		this.qb = AggregationBuilders.cardinality(alias).field(colName).script(getScript(c,o));
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
