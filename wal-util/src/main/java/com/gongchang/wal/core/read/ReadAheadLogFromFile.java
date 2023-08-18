package com.gongchang.wal.core.read;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 读预写日志
 */
public class ReadAheadLogFromFile implements ReadAheadLog<String> {

    private static final Logger logger = LoggerFactory.getLogger(ReadAheadLogFromFile.class);


    private Path walPath;

    public ReadAheadLogFromFile(Path walPath) {
        this.walPath = walPath;
    }


    @Override
    public Iterator<String> readLog() throws IOException {
        Iterator<String> iterator = new Iterator<String>() {
            private List<Path> pathList = new LinkedList<>();
            Iterator<String> curLines;

            {
                try {
                    Files.list(walPath)
                            .filter(path -> path.getFileName().toString().indexOf("-")>0)
                            .forEach(path -> pathList.add(path));
                } catch (IOException e) {
                    logger.error("获取预写日志路径异常：", e);
                }
            }

            @Override
            public boolean hasNext() {
                if(curLines==null || !curLines.hasNext()){
                    while(pathList.size()>0){
                        Path nextPath = pathList.remove(0);
                        try {
							curLines = Files.lines(nextPath).iterator();
						} catch (IOException e) {
							logger.error("读取预写日志异常", e);
						}
                        if(curLines.hasNext()){
                            return true;
                        }
                    }
                    return false;
                }else{
                    return true;
                }
            }

            @Override
            public String next() {
                return curLines.next();
            }

        };
        return iterator;
    }
}
