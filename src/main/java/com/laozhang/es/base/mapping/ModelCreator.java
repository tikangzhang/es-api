package com.laozhang.es.base.mapping;

import java.io.IOException;
import java.util.List;

import org.apache.commons.pool2.ObjectPool;

import org.elasticsearch.client.RestHighLevelClient;

import com.laozhang.es.base.client.ClientDriver;
import com.laozhang.es.base.utils.AnnotionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ModelCreator implements IMappingCreator{
	private static Logger logger = LoggerFactory.getLogger(ModelCreator.class);
	
	protected String packgeName;
	
	protected ObjectPool<RestHighLevelClient> pool;
	
	protected RestHighLevelClient client;
	
	protected List<Class<?>> list;
	
	/**
	 * 预处理
	 */
	protected abstract boolean preExcute(Class<?> clazz);
	
	/**
	 * 添加settings
	 */
	protected abstract void addSettings(Class<?> clazz);
	
	/**
	 * 添加mappings
	 */
	protected abstract void addMappings(Class<?> clazz);
	
	/**
	 * commit
	 */
	protected abstract void commit(Class<?> clazz) throws IOException;
	
	/**
	 * 主控逻辑
	 */
	@Override
	public void excute(){
		try{
			this.pool = ClientDriver.getClientPool();
			this.client = this.pool.borrowObject();
			this.list = AnnotionUtil.getClasses(this.packgeName);//"com.xuanyutech.es.standard.annotation"
			
			for(Class<?> clazz : this.list){
				Excuting(clazz);
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
	
	private void Excuting(Class<?> clazz) throws IOException{
		if(!preExcute(clazz)){
			return;
		}
		
		addSettings(clazz);
		
		addMappings(clazz);
		
		commit(clazz);
	}
}
