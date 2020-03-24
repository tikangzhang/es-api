package com.laozhang.es.base.condition.aggs;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorBuilders;
import org.elasticsearch.search.aggregations.pipeline.bucketsort.BucketSortPipelineAggregationBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;

public class AggBucketSortCondition extends AggCondition{

	private BucketSortPipelineAggregationBuilder aspab;
	
	public final static String DEFAULT_ALIAS = "default_sort";
	
	public AggBucketSortCondition(String col, boolean isDesc){
		this(DEFAULT_ALIAS,col,isDesc);
	}
	
	public AggBucketSortCondition(String alias, String col, boolean isDesc){
		this(alias,new String[]{col},new boolean[]{isDesc});
	}
	
	public AggBucketSortCondition(String[] cols, boolean[] isDescs){
		this(DEFAULT_ALIAS,cols,isDescs);
	}
	
	public AggBucketSortCondition(String alias, String[] cols, boolean[] isDescs){
		List<FieldSortBuilder> sortList = new ArrayList<>();
		for(int i = 0, len = cols.length; i < len; i++){
			if(isDescs[i]){
				sortList.add(new FieldSortBuilder(cols[i]).order(SortOrder.DESC));
			}else{
				sortList.add(new FieldSortBuilder(cols[i]).order(SortOrder.ASC));
			}
		}
		this.aspab = PipelineAggregatorBuilders.bucketSort(alias, sortList);
	}
	
	public void From(int from){
		this.aspab.from(from);
	}
	
	public void Size(int size){
		this.aspab.size(size);
	}
	
	@Override
	public PipelineAggregationBuilder getCondition() {
		return this.aspab;
	}
	
	@Override
	public String getName(){
		return this.aspab.getName();
	}
}
