/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.distributedkeyvaluestore.consistenthash;

import com.distributedkeyvaluestore.models.Node;
import com.distributedkeyvaluestore.models.Quorum;
import jakarta.annotation.Nonnull;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Class for managing the hashing of nodes and data objects into nodes in a consistent manner
 *
 * @param <T> An object that extends the {@link Node} interface
 */
@Component
public class HashManager<T extends Node> {

    private final TreeMap<String, VirtualNode<T>> ring;
    private final HashFunction hashFunction;
    private final int vNodeCount;
    private List<T> allNodes;

    public HashManager(@Nonnull HashFunction hashFunction) {
        this.hashFunction = hashFunction;
        this.ring = new TreeMap<>();

        this.vNodeCount = 100;
    }

    /**
     * Adds a new physical node to the hash ring, with specified number of replicas
     *
     * @param pNode the physical node to be added to the ring
     */
    public void addNode(T pNode) {
        if (vNodeCount < 0) {
            throw new IllegalArgumentException("Number of virtual nodes cannot be negative!");
        }
        int existingReplicas = getExistingReplicas(pNode);
        for (int i = 0; i < vNodeCount; i++) {
            VirtualNode<T> vNode = new VirtualNode<>(pNode, i + existingReplicas);
            ring.put(hashFunction.hash(vNode.getAddress()), vNode);
        }
        allNodes.add(pNode);
    }

    /**
     * Removes a physical node completely from the hash ring
     *
     * @param pNode the physical node to be removed form the hash ring
     */
    public void removeNode(T pNode) {
        ring.keySet().removeIf(key -> ring.get(key).isVirtualNodeOf(pNode));
    }

    /**
     * Method that returns all the nodes to which a data object will be hashed
     *
     * @param objectKey the key of the object to be hashed
     * @return list of nodes to which the object will be hashed
     */
    public ArrayList<T> getNodes(@Nonnull String objectKey) {
        if (ring.isEmpty()) {
            return null;
        }
        String hash = hashFunction.hash(objectKey);
        SortedMap<String, VirtualNode<T>> tailMap = ring.tailMap(hash);
        ArrayList<T> nodesList = new ArrayList<>();
        int i = 0;

        String nodeHash = (!tailMap.isEmpty()) ? tailMap.firstKey() : ring.firstKey();
        VirtualNode<T> firstNode = ring.get(nodeHash);
        while (i < Quorum.getReplicas()) {
            VirtualNode<T> node = ring.get(nodeHash);
            if (!nodesList.contains(node.getPhysicalNode())) {
                // if physical node was not already added, add it
                nodesList.add(node.getPhysicalNode());
                i++;
            } else if (node == firstNode) {
                break;
            }

            // get key for next node; loop around if higherKey does not exist
            nodeHash = (ring.higherKey(nodeHash) != null ? ring.higherKey(nodeHash) : ring.firstKey());
        }

        return nodesList;
    }

    /**
     * Returns the number of existing replicas of a physical node
     *
     * @param pNode the physical node whose number of replicas is required
     * @return (int) the number of existing replicas of the given physical node
     */
    private int getExistingReplicas(T pNode) {
        int replicas = 0;
        for (VirtualNode<T> vNode : ring.values()) {
            if (vNode.isVirtualNodeOf(pNode)) {
                replicas++;
            }
        }
        return replicas;
    }

    public boolean isRingCreated() {
        //TODO finalize health check definition
        return !ring.isEmpty();
    }

    public TreeMap<String, VirtualNode<T>> getRing() {
        return ring;
    }

    public List<T> getAllNodes() {
        return allNodes;
    }
}
