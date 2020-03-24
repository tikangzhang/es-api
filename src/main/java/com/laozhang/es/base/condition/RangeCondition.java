package com.laozhang.es.base.condition;

import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;

import com.laozhang.es.base.CommonConstant;

public class RangeCondition extends FunctionCondition{
	RangeQueryBuilder qb;
	
	public RangeCondition(String colName){
		this.qb = QueryBuilders.rangeQuery(colName);
		//this.qb.timeZone(CommonConstant.DEFAULT_TIME_ZONE);
		this.qb.timeZone(CommonConstant.COMMON_EAST8_TIME_ZONE);
	}
	
	public RangeCondition setTimeZone(String timeZone){
		this.qb.timeZone(timeZone);
		return this;
	}
	
	public RangeCondition Gt(Object o){
		qb.gt(o);
		return this;
	}
	
	public RangeCondition Gte(Object o){
		qb.gte(o);
		return this;
	}
	
	public RangeCondition Lt(Object o){
		qb.lt(o);
		return this;
	}
	
	public RangeCondition Lte(Object o){
		qb.lte(o);
		return this;
	}
	
	public RangeCondition Range(Object from,Object to){
		qb.gte(from).lt(to);
		return this;
	}

	@Override
	public RangeQueryBuilder getCondition() {
		return this.qb;
	}
}
