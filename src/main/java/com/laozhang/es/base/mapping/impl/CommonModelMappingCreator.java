package com.laozhang.es.base.mapping.impl;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.settings.Settings;

import com.laozhang.es.base.annotation.EntityMapping;
import com.laozhang.es.base.annotation.EntityPropertiysMapping;
import com.laozhang.es.base.mapping.ModelCreator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommonModelMappingCreator extends ModelCreator{
	private static Logger logger = LoggerFactory.getLogger(CommonModelMappingCreator.class);
	
	private CreateIndexRequest request;
	
	protected Map<String, Object> jsonMap;
	
	private String indexName;
	
	private String typeName;
	
	private int shards;
	
	private int replicas;
	
	public CommonModelMappingCreator(String packgeName){
		this.packgeName = packgeName;
	}

	@Override
	protected boolean preExcute(Class<?> clazz){
		if(clazz.isAnnotationPresent(EntityMapping.class)){
			EntityMapping entity = clazz.getAnnotation(EntityMapping.class);
			
			this.indexName = entity.Index();
			if(StringUtils.isEmpty(this.indexName)){
				this.indexName = clazz.getName();
			}
			this.typeName = entity.Type();
			this.shards = entity.NumOfShards();
			this.replicas = entity.NumOfReplicas();
			
			this.request = new CreateIndexRequest(this.indexName);
			this.jsonMap = new HashMap<>();
			return true;
		}
		return false;
	}

	@Override
	protected void addSettings(Class<?> clazz) {
		this.request.settings(Settings.builder() 
			    .put("index.number_of_shards", this.shards)
			    .put("index.number_of_replicas", this.replicas)
			);
	}
	@Override
	protected void addMappings(Class<?> clazz) {
		Map<String,Object> typeMap = new HashMap<>();
		{
			typeMap.put("dynamic", false);
			
			Map<String,Object> _all = new HashMap<>();
			_all.put("enabled", false);
			typeMap.put("_all", _all);
			
			Map<String,Object> properties = new HashMap<>();
			Field[] fields = clazz.getDeclaredFields();
			String fieldName;
			String dataType;
			Map<String,Object> fieldMap;
			for (Field field : fields) {
				if (field.isAnnotationPresent(EntityPropertiysMapping.class)) {
					fieldMap = new HashMap<>();
					EntityPropertiysMapping epm = field.getAnnotation(EntityPropertiysMapping.class);
					fieldName = field.getName();
					dataType = epm.DataType();
					fieldMap.put("type", dataType);
					if(dataType.equals("date")){
						fieldMap.put("format","yyyy-MM-dd HH:mm:ss||yyyy-MM-dd'T'HH:mm:ss||epoch_millis");
					}
					
//					if(dataType.equals("keyword")){
//						fieldMap.put("ignore_above",200);//加了以后索引速度慢了，不加万一数据超长可能会在底层lucence引起异常（ Lucene’s term byte-length limit of 32766，non-ASCII characters limit to 32766 / 4 = 8191）
//					}
					properties.put(fieldName, fieldMap);
				}
			}
			typeMap.put("properties", properties);
		}
		this.jsonMap.put(this.typeName,typeMap);
	}

	@Override
	protected void commit(Class<?> clazz) throws IOException {
		this.request.mapping(this.typeName, this.jsonMap);
		CreateIndexResponse createIndexResponse = this.client.indices().create(request, RequestOptions.DEFAULT);
		logger.info("Commit Mapping Acknowledged: " + createIndexResponse.isAcknowledged());
		logger.info("Commit Mapping ShardsAcknowledged: " + createIndexResponse.isShardsAcknowledged());
	}
}
