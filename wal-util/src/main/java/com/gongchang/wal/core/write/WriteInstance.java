package com.gongchang.wal.core.write;

import java.io.IOException;

import com.gongchang.wal.core.base.WalEntry;

public interface WriteInstance {

	void writeLog(WalEntry walEntry) throws IOException;
	
}
