package com.laozhang.es.base.mapping.impl;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import com.laozhang.es.base.mapping.FileModelCreator;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.xcontent.XContentType;

import com.laozhang.es.base.utils.FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommonFileMappingCreator extends FileModelCreator {
	
	private static Logger logger = LoggerFactory.getLogger(CommonFileMappingCreator.class);
	
	private static String FILES_CLASSPATH = "/Mappings";
	
	private CreateIndexRequest request;
	
	public CommonFileMappingCreator(){
		this.dirPath = this.getClass().getResource(FILES_CLASSPATH).getPath();
	}
	
	public CommonFileMappingCreator(String dirPath){
		this.dirPath = dirPath;
	}

	@Override
	protected void commit(File file) throws IOException {
		if(!file.canRead()){
			return;
		}
		Map<String,String> data = FileUtil.readToString(file);
		String mappingIndexN = data.get("indexName");
		String mappingContent = data.get("indexData");
		mappingContent = mappingContent.replace("\r", "");
		mappingContent = mappingContent.replace("\t", "");
		if(!mappingContent.contains("mappings")){
			return;
		}
		request = new CreateIndexRequest(mappingIndexN);
		request.source(mappingContent, XContentType.JSON);

		CreateIndexResponse createIndexResponse = this.client.indices().create(request, RequestOptions.DEFAULT);
		logger.info("Commit Mapping Acknowledged: " + createIndexResponse.isAcknowledged());
		logger.info("Commit Mapping ShardsAcknowledged: " + createIndexResponse.isShardsAcknowledged());
	}
}
