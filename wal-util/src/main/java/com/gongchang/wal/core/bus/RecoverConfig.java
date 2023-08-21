package com.gongchang.wal.core.bus;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.gongchang.wal.core.base.PathUtils;
import com.gongchang.wal.core.read.ReadFileInstance;
import com.gongchang.wal.core.read.ReadInstance;
import com.gongchang.wal.exception.WalRecoverException;

public class RecoverConfig {
	
	private List<ReadInstance> readInstanceList;
	

	public RecoverConfig(List<ReadInstance> readInstanceList) {
		super();
		this.readInstanceList = readInstanceList;
	}
	

	public static RecoverConfigBuilder getRecoverConfigBuilder(){
		return new RecoverConfigBuilder();
	}
	
	public static class RecoverConfigBuilder{
		
		private List<ReadInstance> readInstanceList;
		
		
		public RecoverConfigBuilder() {
			super();
		}


		public void setReadInstanceList(List<ReadInstance> readInstanceList) {
			this.readInstanceList = readInstanceList;
		}


		public RecoverConfig build(){
				if(readInstanceList==null){
					try {
					readInstanceList = Files
							.list(PathUtils.getWalRootPath())
							.map(new Function<Path, ReadInstance>() {
								@Override
								public ReadInstance apply(Path path) {
									return new ReadFileInstance(path);
								}
							}).collect(Collectors.toList());
					} catch (IOException e) {
						throw new WalRecoverException("获取预写日志根路径异常", e);
					}
				}
			
			return new RecoverConfig(readInstanceList);
		}
	}

	
	public List<ReadInstance> getReadInstanceList() {
		return readInstanceList;
	}

	public void setReadInstanceList(List<ReadInstance> readInstanceList) {
		this.readInstanceList = readInstanceList;
	}
	
}
