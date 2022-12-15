package com.distributedkeyvaluestore.exception.handler;

import com.distributedkeyvaluestore.exception.ConsistencyException;
import com.distributedkeyvaluestore.exception.WriteException;
import com.distributedkeyvaluestore.models.FileWithVectorClock;
import com.distributedkeyvaluestore.models.Response;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

import java.util.List;

@ControllerAdvice
public class ConsistencyExceptionHandler extends ResponseEntityExceptionHandler {

    @ExceptionHandler(ConsistencyException.class)
    protected ResponseEntity<Object> handleException(ConsistencyException ex, WebRequest request) {
        List<FileWithVectorClock> fileWithVectorClocks = ex.getFileWithVectorClocks();

        ResponseEntity<Response<List<FileWithVectorClock>>> response = ResponseEntity
                .badRequest()
                .body(new Response<>(fileWithVectorClocks, "Eventual consistency failed, Please retry "));

        return handleExceptionInternal(ex, response, new HttpHeaders(), HttpStatus.INTERNAL_SERVER_ERROR, request);
    }


}