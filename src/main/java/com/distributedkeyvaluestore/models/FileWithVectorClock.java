package com.distributedkeyvaluestore.models;

public class FileWithVectorClock {

    private final byte[] file;
    private final VectorClock vectorClock;

    public FileWithVectorClock(byte[] file, VectorClock vectorClock) {
        this.file = file;
        this.vectorClock = vectorClock;
    }

    public byte[] getFile() {
        return file;
    }

    public VectorClock getVectorClock() {
        return vectorClock;
    }
}
