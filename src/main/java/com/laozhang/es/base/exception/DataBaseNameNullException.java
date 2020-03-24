package com.laozhang.es.base.exception;

public class DataBaseNameNullException extends EsException{
	
	private static final long serialVersionUID = 123499002L;

	public DataBaseNameNullException(){
		super("该记录不存在！");
	}
	
	public DataBaseNameNullException(String errMsg){
		super(errMsg);
	}
}
