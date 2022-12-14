package com.distributedkeyvaluestore.models;

import java.util.Comparator;

public class VectorClock {

    private final int[] clock = new int[Quorum.getReplicas()];

    public VectorClock() {

    }

    public VectorClock(String clockAsString) {
        String[] individualClock = clockAsString.split("_");
        int i=0;
        while (i < Quorum.getReplicas()) {
            clock[i] = Integer.parseInt(individualClock[i]);
        }
    }

    public void incrementClockAtIndex(int index) {
        clock[index]++;
    }

    public int[] getClock() {
        return clock;
    }

    @Override
    public String toString() {
        StringBuilder clockAsString = new StringBuilder();

        for (int i = 0; i < clock.length; i++) {
            if (i != clock.length - 1) {
                clockAsString.append(clock[i]).append("_");
            } else {
                clockAsString.append(clock[i]);
            }
        }

        return clockAsString.toString();
    }

    //0 is equal
    //-1 is small
    //1 is greater
    public int compare(VectorClock vectorClock) {
        int[] thatClock = vectorClock.getClock();
        int compare = 0;

        if (this.clock[0] < thatClock[0] || this.clock[1] < thatClock[1]
                || this.clock[2] < thatClock[2]) {
            compare = -1;
        } else if (this.clock[0] > thatClock[0] || this.clock[1] > thatClock[1]
                || this.clock[2] > thatClock[2]) {
            compare = 1;
        }
        return compare;
    }

}
