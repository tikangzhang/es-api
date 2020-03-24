package com.laozhang.es.base.mapping.impl;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import com.laozhang.es.base.mapping.FileModelCreator;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.xcontent.XContentType;

import com.laozhang.es.base.utils.FileUtil;
import com.laozhang.es.base.utils.FunctionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommonFileMappingUpdator extends FileModelCreator {
	
	private static Logger logger = LoggerFactory.getLogger(CommonFileMappingUpdator.class);
	
	private static String FILES_CLASSPATH = "/Mappings";
	
	private PutMappingRequest  request;

	public CommonFileMappingUpdator(){
		this.dirPath = this.getClass().getResource(FILES_CLASSPATH).getPath();
	}
	
	public CommonFileMappingUpdator(String dirPath){
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
		if(!mappingContent.contains("properties")){
			return;
		}
		request = new PutMappingRequest(mappingIndexN).type(FunctionUtil.line2Hump(mappingIndexN));
		request.source(mappingContent, XContentType.JSON);

		AcknowledgedResponse putMappingResponse = this.client.indices().putMapping(request, RequestOptions.DEFAULT);
		logger.info("Update Mapping Acknowledged: " + putMappingResponse.isAcknowledged());
	}
}
