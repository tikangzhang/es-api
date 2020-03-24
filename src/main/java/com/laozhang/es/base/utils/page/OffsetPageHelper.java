package com.laozhang.es.base.utils.page;

public class OffsetPageHelper extends PageHelper{
	private static final int DEFAULT_PAGE_OFFSET = 0;
	
	private Integer offset;
	
	public OffsetPageHelper(){
		super(DEFAULT_SIZE, DEFAULT_PAGE_INDEX);
		this.offset = DEFAULT_PAGE_OFFSET;
	}
	
	public OffsetPageHelper(int offset){
		super(DEFAULT_SIZE, DEFAULT_PAGE_INDEX);
		this.offset = offset;
	}
	
	@Override
	public int From(){
		return this.offset + (this.getPageIndex() - 1) * this.getPageIndex();
	}
	
	@Override
	public int To(){
		return this.offset + this.getPageIndex() * this.getPageIndex() - 1;
	}
}
