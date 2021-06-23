package com.takeaway.task.exception;

public class ConnectionException extends Exception {
    /**
     * Instantiates a new connection exception.
     */
    public ConnectionException() {
        super();
    }

    /**
     * Instantiates a new connection exception.
     *
     * @param s the s
     * @param e the e
     */
    public ConnectionException(String s,Throwable e){
        super(s,e);
    }

    /**
     * Instantiates a new connection exception.
     *
     * @param s the s
     */
    public ConnectionException(String s) {
        super(s);
    }

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 1L;
}
