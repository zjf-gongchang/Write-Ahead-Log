package com.gongchang.wal.example;

import com.gongchang.wal.core.base.WalEntry;
import com.gongchang.wal.core.redo.RetryDo;
import com.gongchang.wal.core.redo.RetryDoRecoverBy;

public class ReflectTestRetryDo implements RetryDo{

	@Override
	public Boolean redo(WalEntry walEntry) {
		System.out.println(walEntry.sdToMementoStr());
		return true;
	}

	@Override
	public RetryDoRecoverBy getRecoverBy() {
		return RetryDoRecoverBy.REFLECT;
	}
	
}
