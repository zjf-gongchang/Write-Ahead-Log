package com.gongchang.wal.core.write;

import java.io.IOException;

public interface WriteAheadLog<T> {

    void writeLog(T value) throws IOException;

    Boolean cleanLog();

}
