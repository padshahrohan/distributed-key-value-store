package com.distributedkeyvaluestore.models;

public class Response<T> {

    T data;
    String message;

    public Response(T data, String message) {
        this.data = data;
        this.message = message;
    }

    public T getData() {
        return data;
    }

    public String getMessage() {
        return message;
    }
}
