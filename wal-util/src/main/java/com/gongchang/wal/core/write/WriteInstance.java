package com.gongchang.wal.core.write;

import java.io.IOException;

public interface WriteInstance<T> {

	void writeLog(T value) throws IOException;
	
}
