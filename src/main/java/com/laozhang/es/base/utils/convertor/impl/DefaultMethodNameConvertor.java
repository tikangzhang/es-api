package com.laozhang.es.base.utils.convertor.impl;

import org.apache.commons.lang3.StringUtils;

import com.laozhang.es.base.utils.convertor.IMechodNameConvertor;

public class DefaultMethodNameConvertor implements IMechodNameConvertor{

	@Override
	public String convertToSetter(String source) throws Exception {
		return "set" + StringUtils.capitalize(source);
	}

	@Override
	public String setterConvertTo(String source) throws Exception {
		return StringUtils.uncapitalize(source.replace("set", ""));
	}
}
