package com.gongchang.wal.core.sink;


public interface AsyncSink<V,T> {

    Boolean preCommit(T data);

    Boolean commit(V checkPoint);
}
