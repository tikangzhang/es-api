package com.laozhang.es.base.utils.page;

/**
 * 基于List的分页帮助类
 *
 */
public class PageHelper {
	public static final int DEFAULT_SIZE = 15;
	
	public static final int DEFAULT_PAGE_INDEX = 1;
			
	private Integer pageSize;

	private Integer pageIndex;
	
	private Integer totalSize;

	public PageHelper(){
		this(DEFAULT_SIZE, DEFAULT_PAGE_INDEX);
	}
	
	public PageHelper(int pageSize){
		this(pageSize,DEFAULT_PAGE_INDEX);
	}
	
	public PageHelper(int pageSize,int pageIndex){
		this.pageIndex = pageIndex;
		this.pageSize = pageSize;
	}

	public int getCount() {
		return this.pageSize;
	}

	public Integer getPageSize() {
		return pageSize;
	}

	public void setPageSize(Integer pageSize) {
		this.pageSize = pageSize;
	}

	public Integer getPageIndex() {
		return pageIndex;
	}

	public void setPageIndex(Integer pageIndex) {
		this.pageIndex = pageIndex;
	}
	
	public void setTotalSize(Integer totalSize){
		this.totalSize = totalSize;
	}
	
	public int From(){
		int index = (this.pageIndex - 1) * this.pageSize;
		if(this.totalSize != null){
			if(index >= this.totalSize){
				return this.totalSize;
			}
		}
		return index;
	}
	
	public int To(){
		int index = this.pageIndex * this.pageSize;
		if(this.totalSize != null){
			if(index >= this.totalSize){
				return this.totalSize;
			}
		}
		return index;
	}
}
