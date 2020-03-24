package com.laozhang.es.base.condition;

import java.util.LinkedList;

import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;

import com.laozhang.es.base.condition.aggs.AggCondition;

public class BodyCondition {
	private int start = 0;
	
	private int recordsCount = 100;
	
	private SearchSourceBuilder ssb = new SearchSourceBuilder();
	
	public BodyCondition(){
		ssb.from(start);
		ssb.size(recordsCount);
	}
	
	public void setFirstRecordIndex(int index){
		ssb.from(index);
	}
	
	public void setNumOfRecords(int num){
		ssb.size(num);
	}
	
	public void setIncludeCols(String...cols){
		ssb.fetchSource(cols, null);
	}
	
	public void setOrderStrategy(String col,boolean desc){
		if(desc){
			ssb.sort(col, SortOrder.DESC);
		}else{
			ssb.sort(col, SortOrder.ASC);
		}
	}
	
	public void addQueryCondtion(FunctionCondition fcondtion){
		if(null != fcondtion){
			ssb.query(fcondtion.getCondition());
		}
	}
	
	public void addAggCondition(AggCondition aggCondition){
		if(null != aggCondition){
			ssb.aggregation((AggregationBuilder) aggCondition.getCondition());
		}
	}
	
	public void addRootAggConditions(LinkedList<AggCondition> rootAggConditionList){
		if(null != rootAggConditionList && rootAggConditionList.size() > 0){
			for(AggCondition c : rootAggConditionList){
				ssb.aggregation((AggregationBuilder) c.getCondition());
			}
		}
	}
	
	public void addRootPipleAggConditions(LinkedList<AggCondition> rootAggConditionList){
		if(null != rootAggConditionList && rootAggConditionList.size() > 0){
			for(AggCondition c : rootAggConditionList){
				ssb.aggregation((PipelineAggregationBuilder) c.getCondition());
			}
		}
	}

	public SearchSourceBuilder getCondition() {
		return ssb;
	}
}
