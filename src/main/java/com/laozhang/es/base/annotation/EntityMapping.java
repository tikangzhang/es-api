package com.laozhang.es.base.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface EntityMapping {
	/**
	 * 索引名
	 */
	String Index();
	
	/**
	 * 索引type名，未来7.0会逐步删除
	 */
	String Type() default "_doc";
	
	/**
	 * 主分片数
	 */
	int NumOfShards() default 5;
	
	/**
	 * 副本因子
	 */
	int NumOfReplicas() default 1;
}
