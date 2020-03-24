package com.laozhang.es.base.utils.convertor;

public interface IParamTypeConvertor {
	Object stringConvertTo(Object source,Class<?> clazz) throws Exception;
	
	Object convertToString(Object source) throws Exception;
}
