package com.gongchang.wal.core.write;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gongchang.wal.core.base.PathUtils;
import com.gongchang.wal.core.clean.WriteAheadLogCutClean;


/**
 * 请求预写日志Base类
 */
public class SyncWriteAheadLogToFile extends AbstractWriteAheadLogToFile<String> {

    private static final Logger logger = LoggerFactory.getLogger(SyncWriteAheadLogToFile.class);
    
    
    private WriteAheadLogCutClean walcc;
    
    
    public SyncWriteAheadLogToFile(String logName) {
		super(logName);
	}
    
	public SyncWriteAheadLogToFile(String logName, WriteAheadLogCutClean walcc) {
		super(logName);
		this.walcc = walcc;
	}


	@Override
    public synchronized void writeLog(String value) throws IOException {
        Path curWalPath = PathUtils.getWalCurPath(getLogName());
        try(BufferedWriter bw = Files.newBufferedWriter(curWalPath, StandardCharsets.UTF_8, StandardOpenOption.APPEND)) {
            bw.write(value);
            bw.newLine();
            bw.flush();

            cutLog(value);
        }catch (IOException e) {
            logger.error("写入预写日志异常", e);
            throw e;
        }
    }

	private void cutLog(String value){
		 if(walcc!=null){
         	try {
				walcc.cutLog(value.getBytes().length);
			} catch (IOException e) {
				logger.error("日志切割异常", e);
			}
         }
	}


	public WriteAheadLogCutClean getWalcc() {
		return walcc;
	}
	
}
