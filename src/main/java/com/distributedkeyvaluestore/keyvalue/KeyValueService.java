package com.distributedkeyvaluestore.keyvalue;

import com.distributedkeyvaluestore.client.DynamoClient;
import com.distributedkeyvaluestore.client.URIHelper;
import com.distributedkeyvaluestore.consistenthash.HashingManager;
import com.distributedkeyvaluestore.models.DynamoNode;
import com.distributedkeyvaluestore.models.FileWithVectorClock;
import com.distributedkeyvaluestore.models.Quorum;
import com.distributedkeyvaluestore.models.VectorClock;
import org.springframework.core.task.TaskExecutor;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.multipart.MultipartFile;

import java.io.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Component
public class KeyValueService {

    private final HashingManager<DynamoNode> hashingManager;
    private final DynamoClient dynamoClient;
    private final TaskExecutor taskExecutor;

    public KeyValueService(HashingManager<DynamoNode> hashingManager, DynamoClient dynamoClient, TaskExecutor taskExecutor) {
        this.hashingManager = hashingManager;
        this.dynamoClient = dynamoClient;
        this.taskExecutor = taskExecutor;
    }

    public ResponseEntity<String> store(MultipartFile file) {
        String fileName = file.getOriginalFilename();
        List<DynamoNode> nodes = hashingManager.getNodes(fileName).stream()
                .sorted(Comparator.comparing(DynamoNode::getNumber))
                .collect(Collectors.toList());

        Optional<DynamoNode> mayBeFirstNode = nodes.stream().filter(DynamoNode::isCoordinator).findFirst();

        if (mayBeFirstNode.isPresent()) {
            DynamoNode node = mayBeFirstNode.get();
            int vectorIndex = nodes.indexOf(node);
            VectorClock vectorClock = storeObjectInternal(file, node, vectorIndex);
            nodes.remove(mayBeFirstNode.get());
            int writeQuorum = Quorum.getWriteQuorum();
            writeQuorum--;
            storeToReplicas(file, nodes, writeQuorum, vectorClock);
            return ResponseEntity.ok(node.toString());
        } else {
            return forwardToNode(file, nodes.get(0));
        }
    }

    private VectorClock storeObjectInternal(MultipartFile file, DynamoNode node, int vectorIndex) {
        createFile(file, node.getAddress().replaceAll("\\.", "_"));
        return createVectorClockForFile(file, node.getAddress().replaceAll("\\.", "_"), vectorIndex);
    }

    public VectorClock createVectorClockForFile(MultipartFile file, String folder, int vectorIndex) {
        File dir = new File(System.getProperty("user.dir") + File.separator + folder);
        File vectorClockData = new File(dir.getAbsolutePath() + File.separator + "vector_clock_" + file.getOriginalFilename());
        try (BufferedOutputStream stream = new BufferedOutputStream(new FileOutputStream(vectorClockData))) {
            VectorClock vectorClock;
            if (vectorClockData.exists()) {
                BufferedReader bufferedReader = new BufferedReader(new FileReader(vectorClockData));
                vectorClock = new VectorClock(bufferedReader.readLine());
                vectorClock.incrementClockAtIndex(vectorIndex);
                stream.write(vectorClock.toString().getBytes());
            } else {
                vectorClock = new VectorClock();
                vectorClock.incrementClockAtIndex(vectorIndex);
                stream.write(vectorClock.toString().getBytes());
            }
            return vectorClock;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public void createVectorClockForFile(MultipartFile file, String folder, String vectorClockAsString) {
        File dir = new File(System.getProperty("user.dir") + File.separator + folder);
        File vectorClockData = new File(dir.getAbsolutePath() + File.separator + "vector_clock_" + file.getOriginalFilename());
        try (BufferedOutputStream stream = new BufferedOutputStream(new FileOutputStream(vectorClockData))) {
            VectorClock vectorClock = new VectorClock(vectorClockAsString);
            stream.write(vectorClock.toString().getBytes());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private ResponseEntity<String> forwardToNode(MultipartFile file, DynamoNode dynamoNode) {
        return dynamoClient.forwardToNode(URIHelper.createURI(dynamoNode.getAddress()), file);
    }

    public void storeToReplicas(MultipartFile file, List<DynamoNode> nodes,
                                int writeQuorum, VectorClock vectorClock) {
        final CountDownLatch latch = new CountDownLatch(writeQuorum);
        try {
            for (DynamoNode node : nodes) {
                String folder = node.getAddress().replaceAll("\\.", "_");
                CompletableFuture.runAsync(() -> {
                    try {
                        dynamoClient.storeToReplica(URIHelper.createURI(node.getAddress()), file, folder,
                                vectorClock.toString());
                        latch.countDown();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }, taskExecutor);
            }
            if (!latch.await(10, TimeUnit.SECONDS)) {
                throw new RuntimeException();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void createFile(MultipartFile file, String folder) {
        File dir = new File(System.getProperty("user.dir") + File.separator + folder);
        File fileToStore = new File(dir.getAbsolutePath() + File.separator + file.getOriginalFilename());
        try (BufferedOutputStream stream = new BufferedOutputStream(new FileOutputStream(fileToStore))) {

            if (!dir.exists()) {
                dir.mkdirs();
            }
            stream.write(file.getBytes());
        } catch (Exception e) {
            //TODO add exception behavior
            e.printStackTrace();
        }
    }

    public byte[] fetchFile(String fileName, String folder) {
        File file = new File(System.getProperty("user.dir") + File.separator + folder + File.separator + fileName);
        try {
            BufferedInputStream stream = new BufferedInputStream(new FileInputStream(file));
            byte[] bytes = stream.readAllBytes();
            stream.close();
            return bytes;
        } catch (Exception e) {
            return null;
        }
    }

    public VectorClock fetchVectorClock(String fileName, String folder) {
        File file = new File(System.getProperty("user.dir") + File.separator + folder + File.separator + "vector_clock_" + fileName);
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
            return new VectorClock(bufferedReader.readLine());
        } catch (Exception e) {
            return null;
        }
    }

    public ResponseEntity<List<FileWithVectorClock>> retrieve(String fileName) {
        ArrayList<DynamoNode> nodes = hashingManager.getNodes(fileName);
        Optional<DynamoNode> mayBeFirstNode = nodes.stream().filter(DynamoNode::isCoordinator).findFirst();
        final Map<FileWithVectorClock, DynamoNode> filesWithVectorClocks = Collections.synchronizedMap(new HashMap<>());
        if (mayBeFirstNode.isPresent()) {
            DynamoNode node = mayBeFirstNode.get();
            String folder = node.getAddress().replaceAll("\\.", "_");
            filesWithVectorClocks.put(retrieveObjectInternal(fileName, folder), node);
            nodes.remove(node);
            int readQuorum = Quorum.getReadQuorum();
            readQuorum--;
            filesWithVectorClocks.putAll(retrieveFromReplicas(fileName, nodes, readQuorum));

        } else {
            filesWithVectorClocks.putAll(retrieveFromReplicas(fileName, nodes, Quorum.getReadQuorum()));
        }

        List<FileWithVectorClock> fileWithVectorClocks = filesWithVectorClocks.keySet().stream().toList();

        //TODO: fix this response
        return ResponseEntity.ok(fileWithVectorClocks);
    }

    private FileWithVectorClock retrieveObjectInternal(String fileName, String folder) {
        byte[] file = fetchFile(fileName, folder);
        VectorClock vectorClock = fetchVectorClock(fileName, folder);
        return new FileWithVectorClock(file, vectorClock);
    }

    public Map<FileWithVectorClock, DynamoNode> retrieveFromReplicas(String fileName, ArrayList<DynamoNode> nodes, int readQuorum) {

        final CountDownLatch latch = new CountDownLatch(readQuorum);
        try {
            final Map<FileWithVectorClock, DynamoNode> files = Collections.synchronizedMap(new HashMap<>());
            for (DynamoNode node : nodes) {
                CompletableFuture.runAsync(() -> {
                    try {
                        String folder = node.getAddress().replaceAll("\\.", "_");
                        ResponseEntity<FileWithVectorClock> fileWithVectorClockResponseEntity = dynamoClient.retrieveFromReplica(URIHelper.createURI(node.getAddress()), fileName, folder);
                        files.put(fileWithVectorClockResponseEntity.getBody(), node);
                        latch.countDown();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }, taskExecutor);
            }
            if (!latch.await(10, TimeUnit.SECONDS)) {
                throw new RuntimeException();
            }
            return files;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}