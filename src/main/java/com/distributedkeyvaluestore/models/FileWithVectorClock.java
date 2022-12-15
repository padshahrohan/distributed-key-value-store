package com.distributedkeyvaluestore.models;

import com.fasterxml.jackson.annotation.JsonProperty;

public class FileWithVectorClock {

    private final byte[] file;
    private final VectorClock vectorClock;
    private String node;

    public FileWithVectorClock(byte[] file, VectorClock vectorClock) {
        this.file = file;
        this.vectorClock = vectorClock;
        this.node = null;
    }

    public FileWithVectorClock(byte[] file, VectorClock vectorClock, String node) {
        this.file = file;
        this.vectorClock = vectorClock;
        this.node = node;
    }

    public void setNode(String node) {
        this.node = node;
    }

    @JsonProperty("file")
    public String getFile() {
        return new String(file);
    }


    @JsonProperty("vectorClock")
    public VectorClock getVectorClock() {
        return vectorClock;
    }

    @JsonProperty("node")
    public String getNode() {
        return node;
    }
}
