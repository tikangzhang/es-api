package com.laozhang.es.oper.search.scroll;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.laozhang.es.base.Executor;
import com.laozhang.es.base.condition.BodyCondition;
import com.laozhang.es.base.condition.BoolCondition;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScrollTable extends Executor {
	private static Logger logger = LoggerFactory.getLogger(ScrollTable.class);
	
	private static int SCROLL_BATCH = 3000;
			
	private Scroll scroll;
	
	private int batch;
	
	private SearchRequest sr;
	
	private SearchResponse searchResponse;
	
	private SearchHit[] searchHits;
	
	private List<String> scrollIds = new LinkedList<>();
	
	public ScrollTable(String dataBase){
		this(dataBase,null,SCROLL_BATCH,1L);
	}
	
	public ScrollTable(String dataBase, long activeMinutes){
		this(dataBase,null,SCROLL_BATCH,activeMinutes);
	}
	
	public ScrollTable(String dataBase, int batch){
		this(dataBase,null,batch,1L);
	}
	
	public ScrollTable(String dataBase, String type){
		this(dataBase,type,SCROLL_BATCH,1L);
	}
	
	public ScrollTable(String dataBase, String type, long activeMinutes){
		this(dataBase,type,SCROLL_BATCH,activeMinutes);
	}
	
	public ScrollTable(String dataBase, String type, int batch, long activeMinutes){
		this.sr = new SearchRequest(dataBase);
		if(StringUtils.isNotEmpty(type)){
			this.sr.types(type);
		}
		this.batch = batch;
		this.scroll = new Scroll(TimeValue.timeValueMinutes(activeMinutes));
	}
	
	public SearchResponse search(BoolCondition bc){
		try {
			BodyCondition body = new BodyCondition();
			body.addQueryCondtion(bc);
			body.setNumOfRecords(this.batch);
			SearchRequest sr = this.sr.source(body.getCondition());
			logger.debug("[ES Restful Request]: " + sr.toString());
			this.searchResponse = getClient().search(sr,RequestOptions.DEFAULT);
			logger.debug("[ES Response]:" + searchResponse.status());
		} catch (Exception e) {
			logger.error(e.getLocalizedMessage(),e);
		} finally {
			returnBack();
		}
		return this.searchResponse;
	}
	
	public SearchResponse scroll(BoolCondition bc){
		try {
			BodyCondition body = new BodyCondition();
			body.addQueryCondtion(bc);
			body.setNumOfRecords(this.batch);
			SearchRequest sr = new SearchRequest().source(body.getCondition());
			sr.scroll(this.scroll);
			logger.debug("[ES Restful Request]: " + sr.toString());
			this.searchResponse = getClient().search(sr,RequestOptions.DEFAULT);
			logger.debug("[ES Response]:" + searchResponse.status());
		} catch (Exception e) {
			logger.error(e.getLocalizedMessage(),e);
		} finally {
			returnBack();
		}
		return this.searchResponse;
	}
	
	public boolean hasNext(){
		if(this.searchResponse == null){
			return false;
		}
		this.searchHits = this.searchResponse.getHits().getHits();
		return this.searchHits != null && this.searchHits.length > 0;
	}
	
	public List<Map<String,Object>> next(){
		List<Map<String,Object>> result = new LinkedList<>();
		Arrays.stream(this.searchHits, 0, searchHits.length).forEach(x -> result.add(x.getSourceAsMap()));
		callNextRequest();
		return result;
	}
	
	public void dispose(){
		ClearScrollRequest request = new ClearScrollRequest();;
		request.setScrollIds(this.scrollIds);
		try {
			logger.debug("[ES Restful Request]: " + request.toString());
			getClient().clearScrollAsync(request,RequestOptions.DEFAULT,new ActionListener<ClearScrollResponse>() {

				@Override
				public void onResponse(ClearScrollResponse response) {
					logger.debug("{}",response.status());
				}

				@Override
				public void onFailure(Exception e) {
					logger.warn("Clear Scroll Context", e);
				}
			});
			logger.debug("[ES Response]:" + searchResponse.status());
		} catch (Exception e) {
			logger.error(e.getLocalizedMessage(),e);
		} finally {
			returnBack();
		}
	}
	
	private void callNextRequest(){
		String scrollId = this.searchResponse.getScrollId();
		this.scrollIds.add(scrollId);
		SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
		scrollRequest.scroll(this.scroll);
		try {
			logger.debug("[ES Restful Request]: " + scrollRequest.toString());
			this.searchResponse = getClient().scroll(scrollRequest,RequestOptions.DEFAULT);
			logger.debug("[ES Response]:" + searchResponse.status());
		} catch (Exception e) {
			logger.error(e.getLocalizedMessage(),e);
		} finally {
			returnBack();
		}
	}
}
