package com.gongchang.wal.exception;

/**
 * 预写日志初始化检查异常
 * 
 * @author gongchang
 *
 */
public class WALInitCheckException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	
	public WALInitCheckException() {
		super();
	}

	public WALInitCheckException(String arg0, Throwable arg1, boolean arg2, boolean arg3) {
		super(arg0, arg1, arg2, arg3);
	}

	public WALInitCheckException(String arg0, Throwable arg1) {
		super(arg0, arg1);
	}

	public WALInitCheckException(String arg0) {
		super(arg0);
	}

	public WALInitCheckException(Throwable arg0) {
		super(arg0);
	}

}
