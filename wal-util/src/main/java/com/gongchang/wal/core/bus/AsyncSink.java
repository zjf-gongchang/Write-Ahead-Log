package com.gongchang.wal.core.bus;

import com.gongchang.wal.core.base.WalEntry;

public interface AsyncSink {

    Boolean sink(WalEntry walEntry);
    
}
