package com.laozhang.es.oper.search;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.ParsedMultiBucketAggregation.ParsedBucket;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket;
import org.elasticsearch.search.aggregations.bucket.filter.Filters;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation.SingleValue;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHits;

public final class ResolveResponse {
	public final static void getSimpleSearchData(Aggregations aggs,Map<String,Object> mainMap,List<Map<String,Object>> refList,Map<String,String> aliasMap){
		boolean enterSubTerms = false;
		Terms next = null;
		Aggregations data = aggs;
		Iterator<Aggregation> ir = data.iterator();
		Map<String,Object> tempMap = new HashMap<String,Object>();
		while(ir.hasNext()){
			Object temp = ir.next();

			if(temp instanceof Terms){
				next = ((Terms) temp);
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
				
				Map<String,DocumentField> fieldsMap = earchHit[0].getFields();
				if(null != fieldsMap && fieldsMap.size() >0){
					for(DocumentField field :fieldsMap.values()){
						tempMap.put(field.getName(), field.getValues().get(0));
					}
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
				data = null;
				data = buket.getAggregations();
				if(null != data){
					enterSubTerms = true;
					getSimpleSearchData(data,tempMap,refList,aliasMap);
				}
			}
		}
		
		if(tempMap.size() > 0 && !enterSubTerms){
			refList.add(tempMap);
		}
	}
	
	public final static void getSimpleSearchDataByPath(Aggregations aggs,Map<String,Object> mainMap,List<Map<String,Object>> refList,Map<String,String> aliasMap,String path){
		boolean enterSubTerms = false;
		Terms next = null;
		Aggregations data = aggs;
		Iterator<Aggregation> ir = data.iterator();
		Map<String,Object> tempMap = new HashMap<String,Object>();
		while(ir.hasNext()){
			Object temp = ir.next();
			if(temp instanceof Terms){
				next = ((Terms) temp);
//				if(path != null && path.contains(">")){
//					String[] paths = path.split(">", 2);
//					if(next.getName().equals(paths[0])){
//						path = paths[1];
//					}else{
//						return;
//					}
//				}else{
//					path=null;
//				}
				String curName = next.getName();
				if(path != null){
					if(!path.startsWith(curName)){
						int index = path.indexOf('>');
						if(index != -1){
							path = path.substring(index + 1);
							if(!path.startsWith(curName)){
								return;
							}
						}else{
							path = null;
						}
					}
				}
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
				Map<String,DocumentField> fieldsMap = earchHit[0].getFields();
				if(null != fieldsMap && fieldsMap.size() >0){
					for(DocumentField field :fieldsMap.values()){
						tempMap.put(field.getName(), field.getValues().get(0));
					}
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
			if(StringUtils.isEmpty(path)){
				Bucket buket= list.get(0);
				data = null;
				data = buket.getAggregations();
				if(null != data){
					enterSubTerms = true;
					getSimpleSearchDataByPath(data,tempMap,refList,aliasMap,path);
				}
			}else{
				for(int i = 0,len = list.size(); i < len; i++){
					Bucket buket= list.get(i);
					data = null;
					data = buket.getAggregations();
					if(null != data){
						enterSubTerms = true;
						getSimpleSearchDataByPath(data,tempMap,refList,aliasMap,path);
					}
				}
			}
		}
		
		if(tempMap.size() > 0 && !enterSubTerms){
			refList.add(tempMap);
		}
	}
	
	public final static void getFilterSearchDataPerBucket(Aggregations aggs,List<Map<String,Object>> refList){
		Aggregations data = aggs;
		Map<String,Object> tempMap = new HashMap<String,Object>();
		
		Filters next = null;
		Iterator<Aggregation> ir = data.iterator();
		while(ir.hasNext()){
			Object filter = ir.next();
			if(filter instanceof Filters){
				next = ((Filters) filter);
				List<? extends Bucket> resultBuckets = next.getBuckets();
				for(Bucket result : resultBuckets){
					if(result instanceof ParsedBucket){
						Iterator<Aggregation> valueIr = result.getAggregations().iterator();
						while(valueIr.hasNext()){
							Aggregation value = valueIr.next();
							if(value instanceof SingleValue){
								SingleValue sv = (SingleValue) value;
								tempMap.put(result.getKeyAsString(), sv.value());
							}
						}
					}
				}
				refList.add(tempMap);
			}
		}
	}
}
