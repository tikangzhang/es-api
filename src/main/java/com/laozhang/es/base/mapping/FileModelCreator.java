package com.laozhang.es.base.mapping;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.pool2.ObjectPool;

import org.elasticsearch.client.RestHighLevelClient;

import com.laozhang.es.base.client.ClientDriver;
import com.laozhang.es.base.utils.FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class FileModelCreator implements IMappingCreator{
	private static Logger logger = LoggerFactory.getLogger(FileModelCreator.class);
	
	protected String dirPath;
	
	protected ObjectPool<RestHighLevelClient> pool;
	
	protected RestHighLevelClient client;
	
	protected List<File> list;

	/**
	 * commit
	 */
	protected abstract void commit(File file) throws IOException;
	
	/**
	 * 主控逻辑
	 */
	@Override
	public void excute(){
		try{
			this.pool = ClientDriver.getClientPool();
			this.client = this.pool.borrowObject();
			this.list = FileUtil.getFiles(dirPath);
			
			for(File file : this.list){
				logger.info("commit:" + file.getName());
				Excuting(file);
			}
		}catch(Exception e){
			logger.error("", e);
		}finally{
			try {
				pool.returnObject(this.client);
			} catch (Exception e) {
				logger.error("", e);
			}
			this.client = null;
		}
	}
	
	private void Excuting(File file) throws IOException{
		commit(file);
	}
}
