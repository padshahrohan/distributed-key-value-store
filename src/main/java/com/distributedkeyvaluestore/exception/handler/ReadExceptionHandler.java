package com.distributedkeyvaluestore.exception.handler;

import com.distributedkeyvaluestore.exception.ReadException;
import com.distributedkeyvaluestore.exception.WriteException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

@ControllerAdvice
public class ReadExceptionHandler extends ResponseEntityExceptionHandler {

    @ExceptionHandler(ReadException.class)
    protected ResponseEntity<Object> handleException(WriteException ex, WebRequest request) {
        return handleExceptionInternal(ex, ex.getMessage(), new HttpHeaders(), HttpStatus.INTERNAL_SERVER_ERROR, request);
    }


}