package com.distributedkeyvaluestore.exception;

import com.distributedkeyvaluestore.models.FileWithVectorClock;

import java.util.List;

public class ConsistencyException extends RuntimeException{
    private List<FileWithVectorClock> fileWithVectorClocks;

    public ConsistencyException(List<FileWithVectorClock> fileWithVectorClocks) {
        this.fileWithVectorClocks = fileWithVectorClocks;
    }

    public List<FileWithVectorClock> getFileWithVectorClocks() {
        return fileWithVectorClocks;
    }
}
