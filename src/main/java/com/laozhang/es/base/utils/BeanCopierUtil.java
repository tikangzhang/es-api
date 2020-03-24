package com.laozhang.es.base.utils;

import java.lang.reflect.Method;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.laozhang.es.base.utils.convertor.IMechodNameConvertor;
import com.laozhang.es.base.utils.convertor.IParamTypeConvertor;
import com.laozhang.es.base.utils.convertor.impl.DefaultMethodNameConvertor;
import com.laozhang.es.base.utils.convertor.impl.DefaultParamTypeConvertor;

public class BeanCopierUtil{
	private IParamTypeConvertor pConvertor = new DefaultParamTypeConvertor();
	
	private IMechodNameConvertor mConvertor = new DefaultMethodNameConvertor();

	public <T> T mapToObject(Map<String,Object> map,Class<T> clazz,IMechodNameConvertor m){
		this.mConvertor = m;
		return mapToObject(map,clazz);
	}
	
	public <T> T mapToObject(Map<String,Object> map,Class<T> clazz,IParamTypeConvertor p){
		this.pConvertor = p;
		return mapToObject(map,clazz);
	}
	
	public <T> T mapToObject(Map<String,Object> map,Class<T> clazz,IParamTypeConvertor p,IMechodNameConvertor m){
		this.pConvertor = p;
		this.mConvertor = m;
		return mapToObject(map,clazz);
	}
	
	public <T> T mapToObject(Map<String,Object> map,Class<T> clazz){
		T instance = null;
		if(!(map != null && !map.isEmpty())){
			return null;
		}
		try {
			instance = clazz.newInstance();
			Method[] mechods = clazz.getDeclaredMethods();
			String mechodName;
			String paramName;
			for(int i = 0,len = mechods.length; i < len; i++){
				Method temp = mechods[i];
				mechodName = temp.getName();
				if(StringUtils.startsWith(mechodName, "set")){
					Class<?> paraclasses = temp.getParameterTypes()[0];
					paramName = mConvertor.setterConvertTo(mechodName);
					if(StringUtils.isEmpty(paramName)){
						continue;
					}
					Object source = map.get(paramName);
					if(source != null){
						Object o = pConvertor.stringConvertTo(source, paraclasses);
						temp.invoke(instance,o);
					}
				}else{
					continue;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		} 
		return instance;
	}
}
