package com.gongchang.wal.core.read;

import java.nio.file.Path;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gongchang.wal.core.base.WalEntry;

public class ReadFileInstance implements ReadInstance {
	
	private static final Logger logger = LoggerFactory.getLogger(ReadFileInstance.class);
	 

	private Iterator<String> iterator;
	
	private String businessName;
	
	
	public ReadFileInstance(Path walParentPath) {
		super();
		this.iterator = new ReadAheadLogFromFile(walParentPath).readLog();
		this.businessName = walParentPath.getFileName().toString();
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
				return new WalEntry().sdFromMementoStr(metaMemtroStr);
			}
			
		};
	}


	@Override
	public String getBusinessName() {
		return businessName;
	}

}
