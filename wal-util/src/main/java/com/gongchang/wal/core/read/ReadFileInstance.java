package com.gongchang.wal.core.read;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gongchang.wal.core.base.WalEntry;

public class ReadFileInstance implements ReadInstance {
	
	private static final Logger logger = LoggerFactory.getLogger(ReadFileInstance.class);
	 

	Iterator<String> iterator;
	
	
	public ReadFileInstance(Path walParentPath) {
		super();
		try {
			this.iterator = new ReadAheadLogFromFile(walParentPath).readLog();
		} catch (IOException e) {
			logger.error("构造读预写日志实例异常", e);
			throw new RuntimeException("构造读预写日志实例异常");
		}
	}



	@Override
	public Iterator<WalEntry> read() {
		return new Iterator<WalEntry>() {
			
			@Override
			public boolean hasNext() {
				return iterator.hasNext();
			}

			@Override
			public WalEntry next() {
				String metaMemtroStr = iterator.next();
				WalEntry walEntry = WalEntry.metaFromMementoStr(metaMemtroStr).metaToWalEntry();
				return walEntry;
			}
			
		};
	}

}
