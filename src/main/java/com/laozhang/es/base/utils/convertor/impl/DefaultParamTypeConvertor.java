package com.laozhang.es.base.utils.convertor.impl;

import java.util.Date;
import java.text.SimpleDateFormat;

import com.laozhang.es.base.utils.convertor.IParamTypeConvertor;

public class DefaultParamTypeConvertor implements IParamTypeConvertor{

	@Override
	public Object stringConvertTo(Object source, Class<?> clazz) throws Exception {
		if(source.getClass() == clazz){
			return source;
		}
		
		if(clazz == Integer.class || clazz == int.class){
			if(source instanceof String){
				return Integer.parseInt(((String)source));
			}else{
				return ((Double)source).intValue();
			}
		}else if(clazz == Double.class || clazz == double.class){
			if(source instanceof String){
				return Double.parseDouble(((String)source));
			}else{
				return (Double)source;
			}
		}else if(clazz == Long.class || clazz == long.class){
			if(source instanceof String){
				return Long.parseLong(((String)source));
			}else if(source instanceof Integer){
				return ((Integer)source).longValue();
			}else{
				return ((Double)source).longValue();
			}
		}else if(clazz == Float.class || clazz == float.class){
			if(source instanceof String){
				return Float.parseFloat(((String)source));
			}else{
				return ((Double)source).floatValue();
			}
		}else if(clazz == Date.class){
			if(source.getClass() == Long.class){
				return new Date((Long)source);
			}else{
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
				return sdf.parse((String)source);
			}
		}
		
		return source;
	}

	@Override
	public Object convertToString(Object source) {
		Class<?> clazz = source.getClass();
		if(clazz == String.class){
			return source;
		}
		
		if(clazz == Date.class){
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
			return sdf.format(source);
		}else{
			return String.valueOf(source);
		}
	}
}
