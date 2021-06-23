package com.takeaway.task.error;

public enum TakeAwayError {
    TakeAwayError0("System Environment variable is not set"),
    TakeAwayError1("Failed to connect Cassandra");

    private String errMsg;

    private TakeAwayError(String errMsg){
        this.errMsg = errMsg;
    }
    public String getErrorMsg() {
        return name()+" : "+errMsg;
    }
}
