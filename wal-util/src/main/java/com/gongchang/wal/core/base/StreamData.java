package com.gongchang.wal.core.base;

public interface StreamData {
	
	public abstract String sdToMementoStr();
	
	public abstract <T> T sdFromMementoStr(String str);
	
	public abstract StreamDataType getStreamDataType();
	
}
