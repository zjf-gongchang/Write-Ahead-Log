package com.gongchang.wal.core.redo;

public interface RetryDoRecover<T> {

    RetryDo recover(T t);

}
