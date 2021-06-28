package com.amazonreview.task.exception;

public class DataAccessException extends Exception {
    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 1L;

    /** The error code. */
    private String errorCode = "";

    /**
     * Instantiates a new data access exception.
     */
    public DataAccessException() {
        super();
    }

    /**
     * Instantiates a new data access exception.
     *
     * @param s the s
     */
    public DataAccessException(String s) {
        super(s);
    }

    /**
     * Instantiates a new data access exception.
     *
     * @param s the s
     * @param errorCode the error code
     */
    public DataAccessException(String s,String errorCode) {
        super(s);
        this.errorCode = errorCode;
    }

    /**
     * Instantiates a new data access exception.
     *
     * @param s the s
     * @param e the e
     */
    public DataAccessException(String s, DataAccessException e) {
        super(s,e);
    }

    /**
     * Instantiates a new data access exception.
     *
     * @param s the s
     * @param e the e
     */
    public DataAccessException(String s, Exception e) {
        super(s,e);
    }

    /**
     * Instantiates a new data access exception.
     *
     * @param s the s
     * @param e the e
     */
    public DataAccessException(String s,Throwable e){
        super(s,e);
    }

    /**
     * Gets the error code.
     *
     * @return the error code
     */
    public String getErrorCode(){
        return errorCode;
    }
}
