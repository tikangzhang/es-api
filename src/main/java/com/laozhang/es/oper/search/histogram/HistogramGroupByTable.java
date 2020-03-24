package com.laozhang.es.oper.search.histogram;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Map.Entry;

import com.laozhang.es.base.Executor;
import com.laozhang.es.base.condition.BodyCondition;
import com.laozhang.es.base.condition.BoolCondition;
import com.laozhang.es.base.condition.RangeCondition;
import com.laozhang.es.base.condition.aggs.*;
import com.laozhang.es.base.condition.common.Arithmetic;
import com.laozhang.es.base.condition.common.LogicRelation;
import com.laozhang.es.base.exception.DataBaseNameNullException;
import com.laozhang.es.oper.search.*;
import com.laozhang.es.base.utils.BeanCopierUtil;
import com.laozhang.es.base.utils.page.PageHelper;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.histogram.ParsedHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram.Bucket;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation.SingleValue;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHits;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HistogramGroupByTable extends Executor implements IHistogramGroupByTable, IWhere, IGroupBy, IResult, IPipGroupBy {
	public static Logger logger = LoggerFactory.getLogger(HistogramGroupByTable.class);
	
	private String dataBase;
	
	private String table;
	
	private BodyCondition bodyCondition;
	
	private BoolCondition boolCondition;
	
	private RangeCondition rangCondition;
	
	private AggCondition aggCondition;
	
	private LinkedList<AggCondition> rootAggConditionList = new LinkedList<>();
	
	private LinkedList<AggCondition> rootPipleAggConditionList = new LinkedList<>();
	
	private Map<String,String> selAliasMap;
	
	private String key = "_key";
	
	public HistogramGroupByTable(String dataBaseName){
		this(dataBaseName,null);
	}
	
	public HistogramGroupByTable(String dataBaseName, String tableName){
		this.dataBase = dataBaseName;
		this.table = tableName;
	}

	@Override
	public <T> HistogramGroupByTable WhereAnd(String k, T v) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.And(k, v);
		logger.debug(String.format("add %s condition: %s -> %s","and", k,String.valueOf(v)));
		return this;
	}

	@Override
	public HistogramGroupByTable Where(BoolCondition bc) {
		boolCondition = bc;
		logger.debug("add a sub condition");
		return this;
	}

	@Override
	public <T> HistogramGroupByTable WhereOr(String k, T v) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.Or(k, v);
		logger.debug(String.format("add %s condition: %s -> %s","or", k,String.valueOf(v)));
		return this;
	}

	@Override
	public <T> HistogramGroupByTable WhereNot(String k, T v) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.Not(k, v);
		logger.debug(String.format("add %s condition: %s -> %s","not", k,String.valueOf(v)));
		return this;
	}
	
	@Override
	public <T> HistogramGroupByTable WhereAnd(BoolCondition condition) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.addSubAnd(condition);;
		logger.debug("add a composite condition");
		return this;
	}

	@Override
	public <T> HistogramGroupByTable WhereOr(BoolCondition condition) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.addSubOr(condition);;
		logger.debug("add a composite condition");
		return this;
	}

	@Override
	public <T> HistogramGroupByTable WhereNot(BoolCondition condition) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.addSubNot(condition);;
		logger.debug("add a composite condition");
		return this;
	}
	
	@Override
	public HistogramGroupByTable WhereRange(String k, Object from, Object to) {
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
	public <T> HistogramGroupByTable WhereRange(String k, LogicRelation relation, T to) {
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
	public HistogramGroupByTable WhereRange(String k, Object from, Object to, String timeZone) {
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
	public HistogramGroupByTable WhereIsNotNull(String k) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.IsNotNull(k);;
		logger.debug(String.format("add a condition: %s is not null",k));
		return this;
	}

	@Override
	public HistogramGroupByTable WhereIsNull(String k) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.IsNotNull(k);;
		logger.debug(String.format("add a condition: %s is not null",k));
		return this;
	}
	
	@Override
	public HistogramGroupByTable Interval(String col, double interval) {
		if(StringUtils.isEmpty(col)){
			return this;
		}
		if(null == this.aggCondition){
			AggHistogramCondition atsc = new AggHistogramCondition(col);
			atsc.setInterval(interval);
			this.aggCondition = atsc;
			logger.debug("stat by year,col:" + col);
		}
		return this;
	}
	
	@Override
	public HistogramGroupByTable OrderBy(String alias, boolean desc){
		if(null != this.aggCondition) {
			AggTimePeriodStatCondition temp = (AggTimePeriodStatCondition)this.aggCondition;
			temp.orderBy(alias, desc);
			logger.debug("add a orderby condition:" + alias);
		}
		return this;
	}
	
	@Override
	public HistogramGroupByTable Sum(String col) {
		return Sum(col, AggSumCondition.DEFAULT_ALIAS_PREFIX.concat(col));
	}
	
	@Override
	public HistogramGroupByTable Sum(String col, Arithmetic opType, Object opNum) {
		return Sum(col,AggSumCondition.DEFAULT_ALIAS_PREFIX.concat(col),opType,opNum);
	}
	
	@Override
	public HistogramGroupByTable Sum(String col, String alias) {
		if(null == this.aggCondition){
			this.rootAggConditionList.add(new AggSumCondition(col,alias));
			logger.debug(String.format("add a root %s agg operation : col name %s with alias %s", "sum",col,alias));
			return this;
		}
		AggTimePeriodStatCondition temp = (AggTimePeriodStatCondition)this.aggCondition;
		temp.addMetricsAgg(new AggSumCondition(col,alias));
		logger.debug(String.format("add a metrics %s agg operation : col name %s with alias %s", "sum",col,alias));
		return this;
	}
	
	@Override
	public HistogramGroupByTable Sum(String col, String alias, Arithmetic opType, Object opNum) {
		if(null == this.aggCondition){
			this.rootAggConditionList.add(new AggSumCondition(col,alias));
			logger.debug(String.format("add a root %s agg operation : col name %s %s %s with alias %s", "sum",col,opType.getName(),String.valueOf(opNum),alias));
			return this;
		}
		AggTimePeriodStatCondition temp = (AggTimePeriodStatCondition)this.aggCondition;
		temp.addMetricsAgg(new AggSumCondition(col,alias,opType,opNum));
		logger.debug(String.format("add a metrics %s agg operation : col name %s %s %s with alias %s", "sum",col,opType.getName(),String.valueOf(opNum),alias));
		return this;
	}

	@Override
	public HistogramGroupByTable Avg(String col) {
		return Avg(col,AggAvgCondition.DEFAULT_ALIAS_PREFIX.concat(col));
	}
	
	@Override
	public HistogramGroupByTable Avg(String col, Arithmetic opType, Object opNum) {
		return Avg(col, AggAvgCondition.DEFAULT_ALIAS_PREFIX.concat(col),opType,opNum);
	}
	
	@Override
	public HistogramGroupByTable Avg(String col, String alias) {
		if(null == this.aggCondition){
			this.rootAggConditionList.add(new AggAvgCondition(col,alias));
			logger.debug(String.format("add a root %s agg operation : col name %s with alias %s", "avg",col,alias));
			return this;
		}
		AggTimePeriodStatCondition temp = (AggTimePeriodStatCondition)this.aggCondition;
		temp.addMetricsAgg(new AggAvgCondition(col,alias));
		logger.debug(String.format("add a metrics %s agg operation : col name %s with alias %s", "avg",col,alias));
		return this;
	}
	
	@Override
	public HistogramGroupByTable Avg(String col, String alias, Arithmetic opType, Object opNum) {
		if(null == this.aggCondition){
			this.rootAggConditionList.add(new AggAvgCondition(col,alias));
			logger.debug(String.format("add a root %s agg operation : col name %s %s %s with alias %s", "avg",col,opType.getName(),String.valueOf(opNum),alias));
			return this;
		}
		AggTimePeriodStatCondition temp = (AggTimePeriodStatCondition)this.aggCondition;
		temp.addMetricsAgg(new AggAvgCondition(col,alias,opType,opNum));
		logger.debug(String.format("add a metrics %s agg operation : col name %s %s %s with alias %s", "avg",col,opType.getName(),String.valueOf(opNum),alias));
		return this;
	}

	@Override
	public HistogramGroupByTable Max(String col) {
		return Max(col,AggMaxCondition.DEFAULT_ALIAS_PREFIX.concat(col));
	}
	
	@Override
	public HistogramGroupByTable Max(String col, Arithmetic opType, Object opNum) {
		return Max(col,AggMaxCondition.DEFAULT_ALIAS_PREFIX.concat(col),opType,opNum);
	}
	
	@Override
	public HistogramGroupByTable Max(String col, String alias) {
		if(null == this.aggCondition){
			this.rootAggConditionList.add(new AggMaxCondition(col,alias));
			logger.debug(String.format("add a root %s agg operation : col name %s with alias %s", "max",col,alias));
			return this;
		}
		AggTimePeriodStatCondition temp = (AggTimePeriodStatCondition)this.aggCondition;
		temp.addMetricsAgg(new AggMaxCondition(col,alias));
		logger.debug(String.format("add a metrics %s agg operation : col name %s with alias %s", "max",col,alias));
		return this;
	}
	
	@Override
	public HistogramGroupByTable Max(String col, String alias, Arithmetic opType, Object opNum) {
		if(null == this.aggCondition){
			this.rootAggConditionList.add(new AggMaxCondition(col,alias));
			logger.debug(String.format("add a root %s agg operation : col name %s %s %s with alias %s", "max",col,opType.getName(),String.valueOf(opNum),alias));
			return this;
		}
		AggTimePeriodStatCondition temp = (AggTimePeriodStatCondition)this.aggCondition;
		temp.addMetricsAgg(new AggMaxCondition(col,alias,opType,opNum));
		logger.debug(String.format("add a metrics %s agg operation : col name %s %s %s with alias %s", "max",col,opType.getName(),String.valueOf(opNum),alias));
		return this;
	}

	@Override
	public HistogramGroupByTable Min(String col) {
		return Min(col,AggMinCondition.DEFAULT_ALIAS_PREFIX.concat(col));
	}
	
	@Override
	public HistogramGroupByTable Min(String col, Arithmetic opType, Object opNum) {
		return Min(col,AggMinCondition.DEFAULT_ALIAS_PREFIX.concat(col),opType,opNum);
	}
	
	@Override
	public HistogramGroupByTable Min(String col, String alias) {
		if(null == this.aggCondition){
			this.rootAggConditionList.add(new AggMinCondition(col,alias));
			logger.debug(String.format("add a root %s agg operation : col name %s with alias %s", "min",col,alias));
			return this;
		}
		AggTimePeriodStatCondition temp = (AggTimePeriodStatCondition)this.aggCondition;
		temp.addMetricsAgg(new AggMinCondition(col,alias));
		logger.debug(String.format("add a metrics %s agg operation : col name %s with alias %s", "min",col,alias));
		return this;
	}
	
	@Override
	public HistogramGroupByTable Min(String col, String alias, Arithmetic opType, Object opNum) {
		if(null == this.aggCondition){
			this.rootAggConditionList.add(new AggMinCondition(col,alias));
			logger.debug(String.format("add a root %s agg operation : col name %s %s %s with alias %s", "min",col,opType.getName(),String.valueOf(opNum),alias));
			return this;
		}
		AggTimePeriodStatCondition temp = (AggTimePeriodStatCondition)this.aggCondition;
		temp.addMetricsAgg(new AggMinCondition(col,alias,opType,opNum));
		logger.debug(String.format("add a metrics %s agg operation : col name %s %s %s with alias %s", "min",col,opType.getName(),String.valueOf(opNum),alias));
		return this;
	}

	@Override
	public HistogramGroupByTable Count(String col) {
		return Count(col,AggCountCondition.DEFAULT_ALIAS_PREFIX.concat(col));
	}
	
	@Override
	public HistogramGroupByTable Count(String col, String alias) {
		if(null == this.aggCondition){
			this.rootAggConditionList.add(new AggCountCondition(col,alias));
			logger.debug(String.format("add a root %s agg operation : col name %s with alias %s", "count",col,alias));
			return this;
		}
		AggTimePeriodStatCondition temp = (AggTimePeriodStatCondition)this.aggCondition;
		temp.addMetricsAgg(new AggCountCondition(col,alias));
		logger.debug(String.format("add a metrics %s agg operation : col name %s with alias %s", "count",col,alias));
		return this;
	}
	
	@Override
	public HistogramGroupByTable DistinctCount(String col) {
		return DistinctCount(col,AggDistinctCountCondition.DEFAULT_ALIAS_PREFIX.concat(col));
	}

	@Override
	public HistogramGroupByTable DistinctCount(String col, String alias) {
		if(null == this.aggCondition){
			this.rootAggConditionList.add(new AggDistinctCountCondition(col,alias));
			logger.debug(String.format("add a root %s agg operation : col name %s with alias %s", "disctinct count",col,alias));
			return this;
		}
		AggTimePeriodStatCondition temp = (AggTimePeriodStatCondition)this.aggCondition;
		temp.addMetricsAgg(new AggDistinctCountCondition(col,alias));
		logger.debug(String.format("add a metrics %s agg operation : col name %s with alias %s", "disctinct count",col,alias));
		return this;
	}
	
	@Override
	public HistogramGroupByTable setMainKey(String key) {
		this.key = key;
		return this;
	}

	@Override
	public <T> T getEntity(Class<T> eClass){
		throw new RuntimeException("还不支持");
		//return null;
	}
	
	private void getData(Aggregations aggs,Map<String,Object> mainMap,List<Map<String,Object>> refList,Map<String,String> aliasMap){
		boolean enterSubTerms = false;
		ParsedHistogram next = null;
		Aggregations data = aggs;
		Iterator<Aggregation> ir = data.iterator();
		Map<String,Object> tempMap = new HashMap<String,Object>();
		while(ir.hasNext()){
			Object temp = ir.next();

			if(temp instanceof ParsedHistogram){
				next = ((ParsedHistogram) temp);
				continue;
			}else if(temp instanceof TopHits){
				TopHits th = (TopHits)temp;
				SearchHit[] earchHit = th.getHits().getHits();
				if(null == earchHit || earchHit.length == 0){
					continue;
				}
				Map<String,Object> hitMap = earchHit[0].getSourceAsMap();
				if(null != aliasMap){
					String tempKey;
					for(Entry<String,Object> e : hitMap.entrySet()){
						tempKey = e.getKey();
						if(aliasMap.containsKey(tempKey)){
							tempMap.put(aliasMap.get(tempKey), e.getValue());
						}else{
							tempMap.put(e.getKey(), e.getValue());
						}
					}
				}else{
					tempMap.putAll(hitMap);
				}		
			}else if(temp instanceof SingleValue){
				SingleValue sv = (SingleValue) temp;
				tempMap.put(sv.getName(), sv.value());
			}
		}
		
		if(null != mainMap){
			tempMap.putAll(mainMap);
		}
		
		if(null != next){
			List<? extends Bucket> list = next.getBuckets();
			
			for(int i = 0,len = list.size(); i < len; i++){
				Bucket buket= list.get(i);
				tempMap.put(this.key, buket.getKeyAsString());
				data = null;
				data = buket.getAggregations();
				if(null != data){
					enterSubTerms = true;
					getData(data,tempMap,refList,this.selAliasMap);
				}
			}
		}
		
		if(tempMap.size() > 0 && !enterSubTerms){
			refList.add(tempMap);
		}
	}

	@Override
	public <T> List<T> getEntityList(Class<T> eClass) {
		List<Map<String,Object>> resMapList = getListMap();
		return getEntitys(eClass,resMapList);
	}
	
	@Override
	public List<Map<String,Object>> getListMap() {
		List<Map<String,Object>> datalist = null;
		try {
			SearchResponse searchResponse = query();
			Aggregations data = searchResponse.getAggregations();
			datalist = new LinkedList<>();
			getData(data,null,datalist,this.selAliasMap);	
		} catch (Exception e) {
			return null;
		}
		return datalist;
	}
	
	@Override
	public List<Map<String,Object>> getListMap(String path) {
		List<Map<String,Object>> datalist = null;
		try {
			SearchResponse searchResponse = query();
			Aggregations data = searchResponse.getAggregations();
			datalist = new LinkedList<>();
			ResolveResponse.getSimpleSearchDataByPath(data,null,datalist,this.selAliasMap,path);
		} catch (Exception e) {
			logger.error("",e);
			return null;
		}
		return datalist;
	}
	
	@Override
	public List<Map<String, Object>> getListMap(int from, int to) {
		List<Map<String,Object>> resMapList = getListMap();
		return resMapList.subList(from, to);
	}
	
	@Override
	public List<Map<String, Object>> getListMap(PageHelper helper) {
		Objects.requireNonNull(helper);
		List<Map<String,Object>> resMapList = getListMap();
		helper.setTotalSize(resMapList.size());
		return resMapList.subList(helper.From(), helper.To());
	}

	@Override
	public <T> List<T> getEntityList(Class<T> eClass, int from, int to) {
		List<Map<String,Object>> resMapList = getListMap().subList(from, to);
		return getEntitys(eClass,resMapList);
	}
	
	@Override
	public <T> List<T> getEntityList(Class<T> eClass, PageHelper helper) {
		Objects.requireNonNull(helper);
		List<Map<String,Object>> resMapList = getListMap().subList(helper.From(), helper.To());
		helper.setTotalSize(resMapList.size());
		return getEntitys(eClass,resMapList);
	}
	
	private <T> List<T> getEntitys(Class<T> eClass,List<Map<String,Object>> resMapList){
		int len = resMapList.size();
		List<T> dataList = new LinkedList<T>();
		if(len > 0){
			BeanCopierUtil bcu = new BeanCopierUtil();
			Iterator<Map<String,Object>> iterator = resMapList.iterator();
			while(iterator.hasNext()){
				Map<String,Object> t = iterator.next();
				T p = bcu.mapToObject(t, eClass);
				dataList.add(p);
			}
		}
		return dataList;
	}
	
	@Override
	public double getSingleResult(String key) {
		try {
			SearchResponse searchResponse = query();
			Aggregations data = searchResponse.getAggregations();
			Iterator<Aggregation> ir = data.iterator();
			Aggregation temp;
			while(ir.hasNext()){
				temp = ir.next();
				if(temp instanceof SingleValue){
					SingleValue sv = (SingleValue) temp;
					if(sv.getName().equals(key)){
						return sv.value();
					}
				}
			}
		} catch (Exception e) {
			logger.error("", e);
		}
		return 0.0;
	}
	
	private SearchResponse query() throws Exception{
    	if(StringUtils.isEmpty(dataBase)){
    		throw new DataBaseNameNullException("库名不能为空");
    	}
    	
    	this.bodyCondition = new BodyCondition();
    	
    	SearchRequest sr = new SearchRequest(dataBase);
    	if(!StringUtils.isEmpty(table)){
    		sr.types(table);
    	}
    	this.bodyCondition.setNumOfRecords(0);
    	this.bodyCondition.addQueryCondtion(this.boolCondition);
    	this.bodyCondition.addAggCondition(this.aggCondition);
    	this.bodyCondition.addRootAggConditions(this.rootAggConditionList);
    	this.bodyCondition.addRootPipleAggConditions(this.rootPipleAggConditionList);
		sr.source(this.bodyCondition.getCondition());
		
		SearchResponse searchResponse = null;
		try {
			logger.debug("[ES Restful Request]: " + sr.toString());
			RestHighLevelClient client = getClient();
			searchResponse = client.search(sr,RequestOptions.DEFAULT);
			logger.debug("[ES Response]:" + searchResponse);
		} catch (IOException e) {
			logger.error(e.getLocalizedMessage(),e);
		} finally {
			returnBack();
		}
		return searchResponse;
	}

	@Override
	public HistogramGroupByTable Sum(String col, String alias, String script) {
		if(null == this.aggCondition){
			this.rootAggConditionList.add(new AggSumCondition(col,alias,script));
			logger.debug(String.format("add a root %s agg operation : col name %s with alias %s perform %s", "sum",col,alias,script));
			return this;
		}
		AggTimePeriodStatCondition temp = (AggTimePeriodStatCondition)this.aggCondition;
		temp.addMetricsAgg(new AggSumCondition(col,alias,script));
		logger.debug(String.format("add a metrics %s agg operation : col name %s with alias %s perform %s", "sum",col,alias,script));
		return this;
	}

	@Override
	public HistogramGroupByTable Sum(String col, String alias, String script, Map<String, Object> params) {
		if(null == this.aggCondition){
			this.rootAggConditionList.add(new AggSumCondition(col,alias,script,params));
			logger.debug(String.format("add a root %s agg operation : col name %s with alias %s perform %s", "sum",col,alias,script));
			return this;
		}
		AggTimePeriodStatCondition temp = (AggTimePeriodStatCondition)this.aggCondition;
		temp.addMetricsAgg(new AggSumCondition(col,alias,script,params));
		logger.debug(String.format("add a metrics %s agg operation : col name %s with alias %s perform %s", "sum",col,alias,script));
		return this;
	}

	@Override
	public HistogramGroupByTable Avg(String col, String alias, String script) {
		if(null == this.aggCondition){
			this.rootAggConditionList.add(new AggAvgCondition(col,alias,script));
			logger.debug(String.format("add a root %s agg operation : col name %s with alias %s perform %s", "avg",col,alias,script));
			return this;
		}
		AggTimePeriodStatCondition temp = (AggTimePeriodStatCondition)this.aggCondition;
		temp.addMetricsAgg(new AggAvgCondition(col,alias,script));
		logger.debug(String.format("add a metrics %s agg operation : col name %s with alias %s perform %s", "avg",col,alias,script));
		return this;
	}

	@Override
	public HistogramGroupByTable Avg(String col, String alias, String script, Map<String, Object> params) {
		if(null == this.aggCondition){
			this.rootAggConditionList.add(new AggAvgCondition(col,alias,script,params));
			logger.debug(String.format("add a root %s agg operation : col name %s with alias %s perform %s", "avg",col,alias,script));
			return this;
		}
		AggTimePeriodStatCondition temp = (AggTimePeriodStatCondition)this.aggCondition;
		temp.addMetricsAgg(new AggAvgCondition(col,alias,script,params));
		logger.debug(String.format("add a metrics %s agg operation : col name %s with alias %s perform %s", "avg",col,alias,script));
		return this;
	}

	@Override
	public HistogramGroupByTable Max(String col, String alias, String script) {
		if(null == this.aggCondition){
			this.rootAggConditionList.add(new AggMaxCondition(col,alias,script));
			logger.debug(String.format("add a root %s agg operation : col name %s with alias %s perform %s", "max",col,alias,script));
			return this;
		}
		AggTimePeriodStatCondition temp = (AggTimePeriodStatCondition)this.aggCondition;
		temp.addMetricsAgg(new AggMaxCondition(col,alias,script));
		logger.debug(String.format("add a metrics %s agg operation : col name %s with alias %s perform %s", "max",col,alias,script));
		return this;
	}

	@Override
	public HistogramGroupByTable Max(String col, String alias, String script, Map<String, Object> params) {
		if(null == this.aggCondition){
			this.rootAggConditionList.add(new AggMaxCondition(col,alias,script,params));
			logger.debug(String.format("add a root %s agg operation : col name %s with alias %s perform %s", "max",col,alias,script));
			return this;
		}
		AggTimePeriodStatCondition temp = (AggTimePeriodStatCondition)this.aggCondition;
		temp.addMetricsAgg(new AggMaxCondition(col,alias,script,params));
		logger.debug(String.format("add a metrics %s agg operation : col name %s with alias %s perform %s", "max",col,alias,script));
		return this;
	}

	@Override
	public HistogramGroupByTable Min(String col, String alias, String script) {
		if(null == this.aggCondition){
			this.rootAggConditionList.add(new AggMinCondition(col,alias,script));
			logger.debug(String.format("add a root %s agg operation : col name %s with alias %s perform %s", "min",col,alias,script));
			return this;
		}
		AggTimePeriodStatCondition temp = (AggTimePeriodStatCondition)this.aggCondition;
		temp.addMetricsAgg(new AggMinCondition(col,alias,script));
		logger.debug(String.format("add a metrics %s agg operation : col name %s with alias %s perform %s", "min",col,alias,script));
		return this;
	}

	@Override
	public HistogramGroupByTable Min(String col, String alias, String script, Map<String, Object> params) {
		if(null == this.aggCondition){
			this.rootAggConditionList.add(new AggMinCondition(col,alias,script,params));
			logger.debug(String.format("add a root %s agg operation : col name %s with alias %s perform %s", "min",col,alias,script));
			return this;
		}
		AggTimePeriodStatCondition temp = (AggTimePeriodStatCondition)this.aggCondition;
		temp.addMetricsAgg(new AggMinCondition(col,alias,script,params));
		logger.debug(String.format("add a metrics %s agg operation : col name %s with alias %s perform %s", "min",col,alias,script));
		return this;
	}

	@Override
	public HistogramGroupByTable SelectAggCols(String...cols) {
		if(null != this.aggCondition) {
			logger.debug("select:" + cols);
			AggTimePeriodStatCondition temp = (AggTimePeriodStatCondition)this.aggCondition;
			temp.top(cols);
		}
		
		return this;
	}
	
	@Override
	public HistogramGroupByTable SelectAggScriptCols(String alias, String Script) {
		if(null != this.aggCondition) {
			logger.debug("select custom script cols:" + alias);
			AggTimePeriodStatCondition temp = (AggTimePeriodStatCondition)this.aggCondition;
			temp.top(alias,Script);
		}
		
		return this;
	}
	
	@Override
	public HistogramGroupByTable OrderAggCols(String orderCol, boolean desc) {
		if(null != this.aggCondition) {
			logger.debug("select ordered custom cols:" + orderCol);
			AggTimePeriodStatCondition temp = (AggTimePeriodStatCondition)this.aggCondition;
			temp.topOrderBy(orderCol, desc);
		}
		return this;
	}
	
	@Override
	public HistogramGroupByTable AppendAggScriptCol(String alias, String script) {
		if(null != this.aggCondition) {
			logger.debug("select custom Script cols:" + alias);
			AggTimePeriodStatCondition temp = (AggTimePeriodStatCondition)this.aggCondition;
			temp.addScriptTop(alias, script);
		}
		return this;
	}
	
	@Override
	public HistogramGroupByTable EndSelectAggCols(){
		if(null != this.aggCondition) {
			logger.debug("finish custom cols");
			AggTimePeriodStatCondition temp = (AggTimePeriodStatCondition)this.aggCondition;
			temp.endTop();
		}
		return this;
	}

	@Override
	public HistogramGroupByTable setAliasMap(String col, String alias) {
		if(null == this.selAliasMap){
			this.selAliasMap = new HashMap<String, String>();
		}
		this.selAliasMap.put(col, alias);
		return this;
	}

	@Override
	public HistogramGroupByTable AggSize(Integer size) {
		AggTimePeriodStatCondition temp = (AggTimePeriodStatCondition)this.aggCondition;
		if(null != temp){
			temp.size(size);
			logger.debug("set limit:" + size);
		}
		return this;
	}

	@Override
	public HistogramGroupByTable AggCalc(String alias, String script, String... aggCols) {
		throw new RuntimeException("还不支持");
	}

	@Override
	public <T> HistogramGroupByTable Having(String aggCol, LogicRelation relation, T value) {
		throw new RuntimeException("还不支持");
	}

	@Override
	public HistogramGroupByTable OrderGroup(String aggCol, boolean isDesc) {
		throw new RuntimeException("还不支持");
	}

	@Override
	public HistogramGroupByTable OrderGroup(String[] aggCols, boolean[] isDescs) {
		throw new RuntimeException("还不支持");
	}
	
	@Override
	public HistogramGroupByTable AggTopSum(String alias, String path) {
		this.rootPipleAggConditionList.add(new AggTopSumCondition(alias,path));
		return this;
	}

	@Override
	public HistogramGroupByTable AggTopAvg(String alias, String path) {
		this.rootPipleAggConditionList.add(new AggTopAvgCondition(alias,path));
		return this;
	}

	@Override
	public HistogramGroupByTable AggTopMax(String alias, String path) {
		this.rootPipleAggConditionList.add(new AggTopMaxCondition(alias,path));
		return this;
	}

	@Override
	public HistogramGroupByTable AggTopMin(String alias, String path) {
		this.rootPipleAggConditionList.add(new AggTopMinCondition(alias,path));
		return this;
	}
	
	@Override
	public <T> HistogramGroupByTable WhereAnd(String k, Object... vs) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.And(k, vs);
		logger.debug(String.format("add %s condition: %s","and", k, vs));
		return this;
	}

	@Override
	public <T> HistogramGroupByTable WhereAnd(String k, Collection<?> vs) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.And(k, vs);
		logger.debug(String.format("add %s condition: %s","and", k, vs));
		return this;
	}

	@Override
	public <T> HistogramGroupByTable WhereOr(String k, Object... vs) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.Or(k, vs);
		logger.debug(String.format("add %s condition: %s","or", k, vs));
		return this;
	}

	@Override
	public <T> HistogramGroupByTable WhereOr(String k, Collection<?> vs) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.Or(k, vs);
		logger.debug(String.format("add %s condition: %s","or", k, vs));
		return this;
	}

	@Override
	public <T> HistogramGroupByTable WhereNot(String k, Object... vs) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.Not(k, vs);
		logger.debug(String.format("add %s condition: %s","not", k, vs));
		return this;
	}

	@Override
	public <T> HistogramGroupByTable WhereNot(String k, Collection<?> vs) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.Not(k, vs);
		logger.debug(String.format("add %s condition: %s","not", k, vs));
		return this;
	}

	@Override
	public <T> IWhere OrRange(String k, LogicRelation relation, T to) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> IWhere OrRange(String k, T from, T to) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> IWhere OrRange(String k, T from, T to, String timeZone) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public HistogramGroupByTable WhereAndPrefix(String k, String v) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.AndPrefix(k, v);
		logger.debug(String.format("add %s Prefix condition: %s -> %s","and", k,String.valueOf(v)));
		return this;
	}

	@Override
	public HistogramGroupByTable WhereAndWildcard(String k, String v) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.AndWildcard(k, v);
		logger.debug(String.format("add %s Wildcard condition: %s -> %s","and", k,String.valueOf(v)));
		return this;
	}

	@Override
	public HistogramGroupByTable WhereAndRegexp(String k, String v) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.AndRegexp(k, v);
		logger.debug(String.format("add %s Regexp condition: %s -> %s","and", k,String.valueOf(v)));
		return this;
	}

	@Override
	public HistogramGroupByTable WhereAndFuzzy(String k, String v) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.AndFuzzy(k, v);
		logger.debug(String.format("add %s Fuzzy condition: %s -> %s","and", k,String.valueOf(v)));
		return this;
	}

	@Override
	public HistogramGroupByTable WhereOrPrefix(String k, String v) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.OrPrefix(k, v);
		logger.debug(String.format("add %s prefix condition: %s -> %s","and", k,String.valueOf(v)));
		return this;
	}

	@Override
	public HistogramGroupByTable WhereOrWildcard(String k, String v) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.OrWildcard(k, v);
		logger.debug(String.format("add %s Wildcard condition: %s -> %s","and", k,String.valueOf(v)));
		return this;
	}

	@Override
	public HistogramGroupByTable WhereOrRegexp(String k, String v) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.OrRegexp(k, v);
		logger.debug(String.format("add %s Regexp condition: %s -> %s","and", k,String.valueOf(v)));
		return this;
	}

	@Override
	public HistogramGroupByTable WhereOrFuzzy(String k, String v) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.OrFuzzy(k, v);
		logger.debug(String.format("add %s Fuzzy condition: %s -> %s","and", k,String.valueOf(v)));
		return this;
	}
	public HistogramGroupByTable WhereNotPrefix(String k, String v) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.NotPrefix(k, v);
		logger.debug(String.format("add %s Prefix condition: %s -> %s","and", k,String.valueOf(v)));
		return this;
	}

	@Override
	public HistogramGroupByTable WhereNotWildcard(String k, String v) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.NotWildcard(k, v);
		logger.debug(String.format("add %s Wildcard condition: %s -> %s","and", k,String.valueOf(v)));
		return this;
	}

	@Override
	public HistogramGroupByTable WhereNotRegexp(String k, String v) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.NotRegexp(k, v);
		logger.debug(String.format("add %s Regexp condition: %s -> %s","and", k,String.valueOf(v)));
		return this;
	}

	@Override
	public HistogramGroupByTable WhereNotFuzzy(String k, String v) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.NotFuzzy(k, v);
		logger.debug(String.format("add %s Fuzzy condition: %s -> %s","and", k,String.valueOf(v)));
		return this;
	}

}
