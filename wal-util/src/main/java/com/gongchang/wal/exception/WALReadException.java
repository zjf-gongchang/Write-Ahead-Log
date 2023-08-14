package com.gongchang.wal.exception;

public class WALReadException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	
	public WALReadException() {
		super();
	}

	public WALReadException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public WALReadException(String message, Throwable cause) {
		super(message, cause);
	}

	public WALReadException(String message) {
		super(message);
	}

	public WALReadException(Throwable cause) {
		super(cause);
	}
	
}
