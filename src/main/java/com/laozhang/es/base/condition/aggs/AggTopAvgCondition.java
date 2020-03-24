package com.laozhang.es.base.condition.aggs;

import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorBuilders;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.avg.AvgBucketPipelineAggregationBuilder;

public class AggTopAvgCondition extends AggTopLevelCondition{
	private AvgBucketPipelineAggregationBuilder qb;

	public AggTopAvgCondition(String colName,String path){
		this.qb = PipelineAggregatorBuilders.avgBucket(colName, path);
	}
	
	@Override
	public PipelineAggregationBuilder getCondition() {
		return this.qb;
	}

	@Override
	public String getName() {
		return this.qb.getName();
	}
}
