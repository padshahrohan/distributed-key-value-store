package com.distributedkeyvaluestore.models;

import com.fasterxml.jackson.annotation.JsonProperty;

public class FileWithVectorClock {

    private String file;
    private VectorClock vectorClock;
    private String node;

    public FileWithVectorClock(String file, VectorClock vectorClock, String node) {
        this.file = file;
        this.vectorClock = vectorClock;
        this.node = node;
    }

    public String getFile() {
        return file;
    }

    public VectorClock getVectorClock() {
        return vectorClock;
    }

    public String getNode() {
        return node;
    }
}
