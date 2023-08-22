package com.gongchang.wal.core.read;

import java.util.Iterator;

import com.gongchang.wal.core.base.WalEntry;

public interface ReadInstance {

	public Iterator<WalEntry> read();
	
	public String getBusinessName();
	
}
