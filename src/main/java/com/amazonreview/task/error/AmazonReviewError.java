package com.amazonreview.task.error;

public enum AmazonReviewError {
    TakeAwayError0("System Environment variable is not set"),
    TakeAwayError1("Failed to connect Cassandra");

    private String errMsg;

    private AmazonReviewError(String errMsg){
        this.errMsg = errMsg;
    }
    public String getErrorMsg() {
        return name()+" : "+errMsg;
    }
}
