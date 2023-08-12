package com.gongchang.wal.core.write;

import java.io.IOException;
import java.util.List;

/**
 * 按统计条数切分日志
 */
public class WriteAheadLogForCount extends WriteAheadLogBase {

    public WriteAheadLogForCount(String logName) throws IOException {
        super(logName);
    }


    @Override
    public Boolean whetherToCut(String value) {
        return null;
    }

    @Override
    public List<String> getCleanLogName() {
        return null;
    }

    @Override
    public String getNextLogName() {
        return null;
    }
}
