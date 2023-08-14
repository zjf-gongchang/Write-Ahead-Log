package com.gongchang.wal.exception;

public class WALWriteException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	
	public WALWriteException() {
		super();
	}

	public WALWriteException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public WALWriteException(String message, Throwable cause) {
		super(message, cause);
	}

	public WALWriteException(String message) {
		super(message);
	}

	public WALWriteException(Throwable cause) {
		super(cause);
	}

}
