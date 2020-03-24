package com.laozhang.es.base.condition.aggs;

import java.util.LinkedList;
import java.util.List;

import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;

public class AggTermsCondition extends AggBucketLevelCondition{
	private TermsAggregationBuilder qb;
	
	private List<BucketOrder> orderList;
	
	public AggTermsCondition(String colName){
		super();
		this.qb = AggregationBuilders.terms(colName).field(colName);
		this.cursor = qb;
		this.qb.size(DEFAULT_AGG_SIZE);
	}
	public AggTermsCondition(String colName,String alias){
		super();
		this.qb = AggregationBuilders.terms(alias).field(colName);
		this.cursor = qb;
		this.qb.size(DEFAULT_AGG_SIZE);
	}
	
	@Override
	public AggregationBuilder getCondition() {
		return this.qb;
	}

	@Override
	protected void aggBucketLevelReset() {
		this.orderList = null;
	}

	public void size(int size){
		TermsAggregationBuilder temp = (TermsAggregationBuilder)this.cursor;
		temp.size(size);
	}
	
	public void orderBy(String orderCol,boolean desc){
		if(null == orderList){
			orderList = new LinkedList<BucketOrder>();
		}
		orderList.add(BucketOrder.aggregation(orderCol, !desc));
		TermsAggregationBuilder temp = (TermsAggregationBuilder)this.cursor;
		temp.order(BucketOrder.compound(orderList));
	}
	
	public void orderByTerms(boolean desc){
		if(null == orderList){
			orderList = new LinkedList<BucketOrder>();
		}
		orderList.add(BucketOrder.key(!desc));
		TermsAggregationBuilder temp = (TermsAggregationBuilder)this.cursor;
		temp.order(BucketOrder.compound(orderList));
	}
	
	public void orderByRecordCount(boolean desc){
		if(null == orderList){
			orderList = new LinkedList<BucketOrder>();
		}
		orderList.add(BucketOrder.count(!desc));
		TermsAggregationBuilder temp = (TermsAggregationBuilder)this.cursor;
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
