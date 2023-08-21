package com.gongchang.wal.exception;

public class WalRecoverException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public WalRecoverException() {
		super();
	}

	public WalRecoverException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public WalRecoverException(String message, Throwable cause) {
		super(message, cause);
	}

	public WalRecoverException(String message) {
		super(message);
	}

	public WalRecoverException(Throwable cause) {
		super(cause);
	}

}
