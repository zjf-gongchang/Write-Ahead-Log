package com.gongchang.wal.core.redo;

import com.gongchang.wal.core.base.WalEntry;

public interface RetryDo {

    Boolean redo(WalEntry walEntry);

    RetryDoRecoverBy getRecoverBy();

}
