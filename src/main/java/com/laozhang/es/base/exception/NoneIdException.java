package com.laozhang.es.base.exception;

public class NoneIdException extends EsException{
	
	private static final long serialVersionUID = 123499002L;

	public NoneIdException(){
		super("_id is null");
	}
	
	public NoneIdException(String errMsg){
		super(errMsg);
	}
}
