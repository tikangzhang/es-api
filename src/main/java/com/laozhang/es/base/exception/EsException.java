package com.laozhang.es.base.exception;

public class EsException extends RuntimeException{

	private static final long serialVersionUID = 123499001L;

	public EsException(String msg){
		super(msg);
	}
}
