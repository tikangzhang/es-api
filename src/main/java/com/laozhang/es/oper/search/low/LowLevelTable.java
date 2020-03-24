package com.laozhang.es.oper.search.low;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.laozhang.es.base.Executor;
import com.laozhang.es.base.condition.BodyCondition;
import com.laozhang.es.base.condition.BoolCondition;
import com.laozhang.es.oper.search.ResolveResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LowLevelTable extends Executor {
	private static Logger logger = LoggerFactory.getLogger(LowLevelTable.class);
	
	private SearchResponse searchResponse;
	
	/**
	 * 传入原生查询请求
	 * @param sr
	 * @return
	 */
	public SearchResponse search(SearchRequest sr){
		try {
			logger.debug("[ES Restful Request]: " + sr.toString());
			this.searchResponse = getClient().search(sr,RequestOptions.DEFAULT);
			logger.debug("[ES Response]:" + searchResponse);
		} catch (Exception e) {
			logger.error(e.getLocalizedMessage(),e);
		} finally {
			returnBack();
		}
		return this.searchResponse;
	}
	
	public SearchResponse search(BoolCondition bc){
		try {
			BodyCondition body = new BodyCondition();
			body.addQueryCondtion(bc);
			SearchRequest sr = new SearchRequest().source(body.getCondition());
			logger.debug("[ES Restful Request]: " + sr.toString());
			this.searchResponse = getClient().search(sr,RequestOptions.DEFAULT);
			logger.debug("[ES Response]:" + searchResponse);
		} catch (Exception e) {
			logger.error(e.getLocalizedMessage(),e);
		} finally {
			returnBack();
		}
		return this.searchResponse;
	}
	
	public List<Map<String,Object>> getFilterListMap(){
		if(this.searchResponse == null){
			return null;
		}
		List<Map<String,Object>> datalist = new LinkedList<>();
		ResolveResponse.getFilterSearchDataPerBucket(this.searchResponse.getAggregations(),datalist);
		return datalist;
	}
}
