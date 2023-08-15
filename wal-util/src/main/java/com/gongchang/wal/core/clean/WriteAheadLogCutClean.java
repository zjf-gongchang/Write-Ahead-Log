package com.gongchang.wal.core.clean;

import java.io.IOException;
import java.nio.file.Path;

public interface WriteAheadLogCutClean {

	public void cutLog(Integer logSize) throws IOException;
	
	public Boolean cleanLog(Path logParentPath);

}
