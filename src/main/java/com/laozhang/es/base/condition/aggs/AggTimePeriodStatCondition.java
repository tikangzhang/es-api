package com.laozhang.es.base.condition.aggs;

import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.joda.time.DateTimeZone;

public class AggTimePeriodStatCondition extends AggCondition{
	public final static String DEFAULT_YEAR_FORMATER = "strict_year";
	
	public final static String DEFAULT_MONTH_FORMATER = "strict_year_month";
	
	public final static String DEFAULT_WEEK_FORMATER = "strict_week_date";
	
	public final static String DEFAULT_DAY_FORMATER = "strict_date";
	
	private DateHistogramAggregationBuilder qb;
	
	private List<BucketOrder> orderList;
	
	private AggTopSelectCondition tb;
	
	public AggTimePeriodStatCondition(String colName){
		this.qb = AggregationBuilders.dateHistogram(colName).field(colName);
		this.cursor = this.qb;
		//this.qb.timeZone(DateTimeZone.forID(CommonConstant.COMMON_EAST8_TIME_ZONE));
	}
	
	public void setInterval(DateHistogramInterval d){
		this.qb.dateHistogramInterval(d);
	}
	
	public void setFormater(String formater){
		this.qb.format(formater);
	}
	
	public void setOffsetTimeZone(int offsetHours){
		this.qb.timeZone(DateTimeZone.forOffsetHours(offsetHours));
	}
	
	public void setTimeZone(String timeZone){
		this.qb.timeZone(DateTimeZone.forID(timeZone));
	}
	
	public void addMetricsAgg(AggCondition a){
		this.qb.subAggregation((AggregationBuilder) a.getCondition());
	}
	
	@Override
	public AggregationBuilder getCondition() {
		return this.qb;
	}
	
	public void top(String... cols){
		if(this.tb == null){
			this.tb = new AggTopSelectCondition(this.cursor.getName());
			this.tb.select(cols);
		}
	}
	
	public void top(String alias,String script){
		if(this.tb == null){
			this.tb = new AggTopSelectCondition(this.cursor.getName(),alias,script);
		}
	}
	
	public void topOrderBy(String orderCol,boolean desc){
		if(!StringUtils.isEmpty(orderCol)){
			if(this.tb != null){
				this.tb.order(orderCol,desc);
			}
		}
	}
	
	public void addScriptTop(String alias,String script){
		if(this.tb != null){
			this.tb.addScriptSelect(alias, script);
		}
	}
	
	public void size(Integer size){
		if(this.tb == null){
			this.tb.size(size);
		}
	}
	
	public void endTop(){
		DateHistogramAggregationBuilder temp = (DateHistogramAggregationBuilder)this.cursor;
		temp.subAggregation(tb.getCondition());
	}
	public void orderBy(String orderCol,boolean desc){
		if(null == orderList){
			orderList = new LinkedList<BucketOrder>();
		}
		orderList.add(BucketOrder.aggregation(orderCol, !desc));
		DateHistogramAggregationBuilder temp = (DateHistogramAggregationBuilder)this.qb;
		temp.order(BucketOrder.compound(orderList));
	}
	
	@Override
	public String getName(){
		return this.qb.getName();
	}
	
	public String getSubAggName(int index){
		return this.qb.getSubAggregations().get(index).getName();
	}
	
	public int getSubAggCount(){
		return this.qb.getSubAggregations().size();
	}
}
