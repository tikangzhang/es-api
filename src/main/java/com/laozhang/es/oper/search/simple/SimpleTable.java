package com.laozhang.es.oper.search.simple;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.laozhang.es.base.Executor;
import com.laozhang.es.base.condition.BodyCondition;
import com.laozhang.es.base.condition.BoolCondition;
import com.laozhang.es.base.condition.RangeCondition;
import com.laozhang.es.base.condition.common.LogicRelation;
import com.laozhang.es.base.exception.DataBaseNameNullException;
import com.laozhang.es.oper.search.IWhere;
import com.laozhang.es.oper.search.ISelect;
import com.laozhang.es.base.utils.BeanCopierUtil;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleTable extends Executor implements ISelect, IWhere {
	public static Logger logger = LoggerFactory.getLogger(SimpleTable.class);
	
	private String dataBase;
	
	private String table;
	
	private BodyCondition bodyCondition;
	
	private BoolCondition boolCondition;
	
	private RangeCondition rangCondition;
	
	public SimpleTable(String dataBaseName){
		this(dataBaseName,null);
	}
	
	public SimpleTable(String dataBaseName, String tableName){
		this.dataBase = dataBaseName;
		this.table = tableName;
	}

	@Override
	public SimpleTable SelectCols(String... cols) {
		if(null == this.bodyCondition){
			this.bodyCondition = new BodyCondition();
		}
		this.bodyCondition.setIncludeCols(cols);
		logger.debug("select:" + cols);
		return this;
	}

	@Override
	public <T> SimpleTable WhereAnd(String k, T v) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.And(k, v);
		logger.debug(String.format("add %s condition: %s -> %s","and", k,String.valueOf(v)));
		return this;
	}

	@Override
	public SimpleTable Where(BoolCondition bc) {
		boolCondition = bc;
		logger.debug("add a sub condition");
		return this;
	}

	@Override
	public SimpleTable WhereAndPrefix(String k, String v) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.AndPrefix(k, v);
		logger.debug(String.format("add %s Prefix condition: %s -> %s","and", k,String.valueOf(v)));
		return this;
	}

	@Override
	public SimpleTable WhereAndWildcard(String k, String v) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.AndWildcard(k, v);
		logger.debug(String.format("add %s Wildcard condition: %s -> %s","and", k,String.valueOf(v)));
		return this;
	}

	@Override
	public SimpleTable WhereAndRegexp(String k, String v) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.AndRegexp(k, v);
		logger.debug(String.format("add %s Regexp condition: %s -> %s","and", k,String.valueOf(v)));
		return this;
	}

	@Override
	public SimpleTable WhereAndFuzzy(String k, String v) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.AndFuzzy(k, v);
		logger.debug(String.format("add %s Fuzzy condition: %s -> %s","and", k,String.valueOf(v)));
		return this;
	}

	@Override
	public <T> SimpleTable WhereOr(String k, T v) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.Or(k, v);
		logger.debug(String.format("add %s condition: %s -> %s","or", k,String.valueOf(v)));
		return this;
	}

	@Override
	public <T> SimpleTable WhereNot(String k, T v) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.Not(k, v);
		logger.debug(String.format("add %s condition: %s -> %s","not", k,String.valueOf(v)));
		return this;
	}
	
	@Override
	public <T> SimpleTable WhereAnd(BoolCondition condition) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.addSubAnd(condition);;
		logger.debug("add a composite condition");
		return this;
	}

	@Override
	public <T> SimpleTable WhereOr(BoolCondition condition) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.addSubOr(condition);;
		logger.debug("add a composite condition");
		return this;
	}

	@Override
	public SimpleTable WhereOrPrefix(String k, String v) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.OrPrefix(k, v);
		logger.debug(String.format("add %s prefix condition: %s -> %s","and", k,String.valueOf(v)));
		return this;
	}

	@Override
	public SimpleTable WhereOrWildcard(String k, String v) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.OrWildcard(k, v);
		logger.debug(String.format("add %s Wildcard condition: %s -> %s","and", k,String.valueOf(v)));
		return this;
	}

	@Override
	public SimpleTable WhereOrRegexp(String k, String v) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.OrRegexp(k, v);
		logger.debug(String.format("add %s Regexp condition: %s -> %s","and", k,String.valueOf(v)));
		return this;
	}

	@Override
	public SimpleTable WhereOrFuzzy(String k, String v) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.OrFuzzy(k, v);
		logger.debug(String.format("add %s Fuzzy condition: %s -> %s","and", k,String.valueOf(v)));
		return this;
	}

	@Override
	public <T> SimpleTable WhereNot(BoolCondition condition) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.addSubNot(condition);;
		logger.debug("add a composite condition");
		return this;
	}

	@Override
	public SimpleTable WhereNotPrefix(String k, String v) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.NotPrefix(k, v);
		logger.debug(String.format("add %s Prefix condition: %s -> %s","and", k,String.valueOf(v)));
		return this;
	}

	@Override
	public SimpleTable WhereNotWildcard(String k, String v) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.NotWildcard(k, v);
		logger.debug(String.format("add %s Wildcard condition: %s -> %s","and", k,String.valueOf(v)));
		return this;
	}

	@Override
	public SimpleTable WhereNotRegexp(String k, String v) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.NotRegexp(k, v);
		logger.debug(String.format("add %s Regexp condition: %s -> %s","and", k,String.valueOf(v)));
		return this;
	}

	@Override
	public SimpleTable WhereNotFuzzy(String k, String v) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.NotFuzzy(k, v);
		logger.debug(String.format("add %s Fuzzy condition: %s -> %s","and", k,String.valueOf(v)));
		return this;
	}

	@Override
	public SimpleTable OrderBy(String col, boolean desc) {
		if(null == this.bodyCondition){
			this.bodyCondition = new BodyCondition();
		}
		this.bodyCondition.setOrderStrategy(col, desc);
		logger.debug("add a orderby conditions:" + col);
		return this;
	}

	@Override
	public <T> SimpleTable WhereRange(String k, T from, T to) {
		rangCondition = new RangeCondition(k);
		rangCondition.Gte(from).Lt(to);
		
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.AndRange(rangCondition);
		logger.debug(String.format("add %s condition: %s -> %s","range", String.valueOf(from),String.valueOf(to)));
		return this;
	}
	
	@Override
	public <T> SimpleTable WhereRange(String k, LogicRelation relation, T to) {
		if(LogicRelation.Ne.equals(relation)){
			logger.error("do not support Ne relation");
		}
		rangCondition = new RangeCondition(k);
		if(LogicRelation.Gt.equals(relation)){
			rangCondition.Gt(to);
		}else if(LogicRelation.Gte.equals(relation)){
			rangCondition.Gte(to);
		}else if(LogicRelation.Lt.equals(relation)){
			rangCondition.Lt(to);
		}else if(LogicRelation.Lte.equals(relation)){
			rangCondition.Lte(to);
		}else if(LogicRelation.Eq.equals(relation)){
			WhereAnd(k, to);
		}
		
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.AndRange(rangCondition);
		logger.debug(String.format("add %s condition: %s %s","range", relation.getName(),to.toString()));
		return this;
	}
	
	@Override
	public <T> SimpleTable WhereRange(String k, T from, T to, String timeZone) {
		rangCondition = new RangeCondition(k);
		rangCondition.Gte(from).Lt(to);
		rangCondition.setTimeZone(timeZone);
		
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.AndRange(rangCondition);
		logger.debug(String.format("add %s condition: %s -> %s","range", String.valueOf(from),String.valueOf(to)));
		return this;
	}

	@Override
	public <T> SimpleTable OrRange(String k, LogicRelation relation, T to) {
		if(LogicRelation.Ne.equals(relation)){
			logger.error("do not support Ne relation");
		}
		rangCondition = new RangeCondition(k);
		if(LogicRelation.Gt.equals(relation)){
			rangCondition.Gt(to);
		}else if(LogicRelation.Gte.equals(relation)){
			rangCondition.Gte(to);
		}else if(LogicRelation.Lt.equals(relation)){
			rangCondition.Lt(to);
		}else if(LogicRelation.Lte.equals(relation)){
			rangCondition.Lte(to);
		}else if(LogicRelation.Eq.equals(relation)){
			WhereAnd(k, to);
		}
		
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.OrRange(rangCondition);
		logger.debug(String.format("add %s condition: %s %s","or range", relation.getName(),to.toString()));
		return this;
	}

	@Override
	public <T> SimpleTable OrRange(String k, T from, T to) {
		rangCondition = new RangeCondition(k);
		rangCondition.Gte(from).Lt(to);
		
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.OrRange(rangCondition);
		logger.debug(String.format("add %s condition: %s -> %s","or range", String.valueOf(from),String.valueOf(to)));
		return this;
	}

	@Override
	public <T> SimpleTable OrRange(String k, T from, T to, String timeZone) {
		rangCondition = new RangeCondition(k);
		rangCondition.Gte(from).Lt(to);
		rangCondition.setTimeZone(timeZone);
		
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.OrRange(rangCondition);
		logger.debug(String.format("add %s condition: %s -> %s","or range", String.valueOf(from),String.valueOf(to)));
		return this;
	}
	
	@Override
	public IWhere WhereIsNotNull(String k) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.IsNotNull(k);;
		logger.debug(String.format("add a condition: %s is not null",k));
		return this;
	}

	@Override
	public IWhere WhereIsNull(String k) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.IsNull(k);;
		logger.debug(String.format("add a condition: %s is not null",k));
		return this;
	}
	

	@Override
	public SimpleTable From(int start) {
		if(null == this.bodyCondition){
			this.bodyCondition = new BodyCondition();
		}
		this.bodyCondition.setFirstRecordIndex(start);
		logger.debug("limite from:" + start);
		return this;
	}

	@Override
	public SimpleTable Size(int end) {
		if(null == this.bodyCondition){
			this.bodyCondition = new BodyCondition();
		}
		this.bodyCondition.setNumOfRecords(end);
		logger.debug("limite:" + end);
		return this;
	}
	
	@Override
	public <T> T getEntity(Class<T> clazz) {
		if(null == this.bodyCondition){
			this.bodyCondition = new BodyCondition();
		}
		this.bodyCondition.setFirstRecordIndex(0);
		this.bodyCondition.setNumOfRecords(1);
		SearchResponse searchResponse = query();
		SearchHits hits = searchResponse.getHits();
		SearchHit[] hitArray = hits.getHits();
		if(hitArray.length > 0){
			SearchHit hit = hitArray[0];
			Map<String,Object> data = hit.getSourceAsMap();
			BeanCopierUtil bcu = new BeanCopierUtil();
			T p = bcu.mapToObject(data, clazz);
			
			Method method;
			try {
				method = clazz.getMethod("setId", new Class<?>[]{String.class});
				method.invoke(p, hit.getId());
			} catch (Exception e) {
				logger.error("Set Id Error",e);
			}
			return p;
		}
		return null;
	}

	@Override
	public <T> List<T> getEntityList(Class<T> clazz) {
		SearchResponse searchResponse = query();
		SearchHits hits = searchResponse.getHits();
		SearchHit[] hitArray = hits.getHits();
		int hitsLen = hitArray.length;
		List<T> dataList = new LinkedList<T>();
		if(hitsLen > 0){
			BeanCopierUtil bcu = new BeanCopierUtil();
			SearchHit hit = null;
			Method method;
			try {
				method = clazz.getMethod("setId", new Class<?>[]{String.class});
				for(int i = 0; i < hitsLen; i++){
					hit = hits.getHits()[i];
					Map<String,Object> data = hit.getSourceAsMap();
					T p = bcu.mapToObject(data, clazz);
					method.invoke(p, hit.getId());
					dataList.add(p);
				}
			} catch (Exception e) {
				logger.error("Set Id Error",e);
			}
		}
		return dataList;
	}

	@Override
	public List<Map<String,Object>> getMapList() {
		SearchResponse searchResponse = query();
		SearchHits hits = searchResponse.getHits();
		SearchHit[] hitArray = hits.getHits();
		int hitsLen = hitArray.length;
		List<Map<String,Object>> dataList = new LinkedList<>();
		if(hitsLen > 0){
			SearchHit hit = null;
			for(int i = 0; i < hitsLen; i++){
				hit = hitArray[i];
				dataList.add(hit.getSourceAsMap());
			}
		}
		return dataList;
	}
	
	@Override
	public String getSourceStr() {
		SearchResponse searchResponse = query();
		return searchResponse.toString();
	}
	
	@Override
	public SearchResponse getSource() {
		return query();
	}
	
	private SearchResponse query() {
    	if(StringUtils.isEmpty(dataBase)){
    		throw new DataBaseNameNullException("库名不能为空");
    	}
    	SearchRequest sr = new SearchRequest(dataBase);
    	if(!StringUtils.isEmpty(table)){
    		sr.types(table);
    	}
    	if(null == this.bodyCondition){
    		this.bodyCondition = new BodyCondition();
    	}
    	this.bodyCondition.addQueryCondtion(this.boolCondition);
    	
		sr.source(this.bodyCondition.getCondition());
		
		SearchResponse searchResponse = null;
		try {
			logger.debug("[ES Restful Request]: " + sr.toString());
			RestHighLevelClient client = getClient();
			searchResponse = client.search(sr,RequestOptions.DEFAULT);
			logger.debug("[ES Response]:" + searchResponse);
		} catch (IOException e) {
			logger.error(e.getMessage());
			logger.error("{}",e.getStackTrace());
		} finally {
			returnBack();
		}
		return searchResponse;
	}
	
	@Override
	public <T> SimpleTable WhereAnd(String k, Object... vs) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.And(k, vs);
		logger.debug(String.format("add %s condition: %s","and", k, vs));
		return this;
	}

	@Override
	public <T> SimpleTable WhereAnd(String k, Collection<?> vs) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.And(k, vs);
		logger.debug(String.format("add %s condition: %s","and", k, vs));
		return this;
	}

	@Override
	public <T> SimpleTable WhereOr(String k, Object... vs) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.Or(k, vs);
		logger.debug(String.format("add %s condition: %s","or", k, vs));
		return this;
	}

	@Override
	public <T> SimpleTable WhereOr(String k, Collection<?> vs) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.Or(k, vs);
		logger.debug(String.format("add %s condition: %s","or", k, vs));
		return this;
	}

	@Override
	public <T> SimpleTable WhereNot(String k, Object... vs) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.Not(k, vs);
		logger.debug(String.format("add %s condition: %s","not", k, vs));
		return this;
	}

	@Override
	public <T> SimpleTable WhereNot(String k, Collection<?> vs) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.Not(k, vs);
		logger.debug(String.format("add %s condition: %s","not", k, vs));
		return this;
	}

}
