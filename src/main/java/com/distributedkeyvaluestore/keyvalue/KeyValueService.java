package com.distributedkeyvaluestore.keyvalue;

import com.distributedkeyvaluestore.client.DynamoClient;
import com.distributedkeyvaluestore.client.URIHelper;
import com.distributedkeyvaluestore.consistenthash.HashManager;
import com.distributedkeyvaluestore.exception.ConsistencyException;
import com.distributedkeyvaluestore.exception.ReadException;
import com.distributedkeyvaluestore.exception.WriteException;
import com.distributedkeyvaluestore.models.DynamoNode;
import com.distributedkeyvaluestore.models.FileWithVectorClock;
import com.distributedkeyvaluestore.models.Quorum;
import com.distributedkeyvaluestore.models.VectorClock;
import org.jetbrains.annotations.NotNull;
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

    private final HashManager<DynamoNode> hashManager;
    private final DynamoClient dynamoClient;
    private final TaskExecutor taskExecutor;

    public KeyValueService(HashManager<DynamoNode> hashManager, DynamoClient dynamoClient, TaskExecutor taskExecutor) {
        this.hashManager = hashManager;
        this.dynamoClient = dynamoClient;
        this.taskExecutor = taskExecutor;
    }

    public ResponseEntity<String> store(MultipartFile file) {
        String fileName = file.getOriginalFilename();
        List<DynamoNode> nodes = hashManager.getNodes(fileName).stream()
                .sorted(Comparator.comparing(DynamoNode::getNumber))
                .collect(Collectors.toList());

        Optional<DynamoNode> mayBeFirstNode = nodes.stream().filter(DynamoNode::isSelfAware).findFirst();
        try {
            if (mayBeFirstNode.isPresent()) {
                DynamoNode node = mayBeFirstNode.get();
                int vectorIndex = nodes.indexOf(node);
                VectorClock vectorClock = storeObjectInternal(file, node, vectorIndex);
                nodes.remove(mayBeFirstNode.get());
                int writeQuorum = Quorum.getWriteQuorum();
                writeQuorum--;
                storeToReplicas(file, nodes, writeQuorum, vectorClock);
                return ResponseEntity.ok("Write operation succeeded on node number " + node.getNumber() +
                        " with ip " + node.getAddress());
            } else {
                return forwardToNode(file, nodes.get(0));
            }
        } catch (WriteException e) {
            throw e;
        }
    }

    private VectorClock storeObjectInternal(MultipartFile file, DynamoNode node, int vectorIndex) {
        createFile(file, node.getAddress().replaceAll("\\.", "_"));
        return createVectorClockForFile(file, node.getAddress().replaceAll("\\.", "_"), vectorIndex);
    }

    public VectorClock createVectorClockForFile(MultipartFile file, String folder, int vectorIndex) {
        File dir = new File(System.getProperty("user.dir") + File.separator + folder);
        File vectorClockData = new File(dir.getAbsolutePath() + File.separator + "vector_clock_" + file.getOriginalFilename());
        try {
            VectorClock vectorClock;
            if (vectorClockData.exists()) {
                BufferedReader bufferedReader = new BufferedReader(new FileReader(vectorClockData));
                String clockAsString = bufferedReader.readLine();
                vectorClock = new VectorClock(clockAsString);
                bufferedReader.close();
            } else {
                vectorClock = new VectorClock();
            }
            BufferedOutputStream stream = new BufferedOutputStream(new FileOutputStream(vectorClockData));
            vectorClock.incrementClockAtIndex(vectorIndex);
            stream.write(vectorClock.toString().getBytes());
            stream.close();
            return vectorClock;
        } catch (Exception e) {
            e.printStackTrace();
            throw new WriteException("Vector clock write failed");
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
            throw new WriteException("Vector clock write failed");
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
                throw new WriteException("Write quorum condition failed");
            }
        } catch (WriteException e) {
            throw e;
        } catch (Exception e) {
            e.printStackTrace();
            throw new WriteException("Write operation failed");
        }
    }

    public void createFile(MultipartFile file, String folder) {
        File dir = new File(System.getProperty("user.dir") + File.separator + folder);
        File fileToStore = new File(dir.getAbsolutePath() + File.separator + file.getOriginalFilename());
        if (!dir.exists()) {
            System.out.println("Trying to create dir");
            dir.mkdirs();
        }
        System.out.println("Directory created");
        try (BufferedOutputStream stream = new BufferedOutputStream(new FileOutputStream(fileToStore))) {
            System.out.println(fileToStore.getAbsolutePath());
            stream.write(file.getBytes());
        } catch (Exception e) {
            e.printStackTrace();
            throw new WriteException("File write failed");
        }
    }

    public byte[] fetchFile(String folder, String fileName) {
        File file = new File(System.getProperty("user.dir") + File.separator + folder + File.separator + fileName);
        try {
            BufferedInputStream stream = new BufferedInputStream(new FileInputStream(file));
            byte[] bytes = stream.readAllBytes();
            stream.close();
            return bytes;
        } catch (Exception e) {
            e.printStackTrace();
            throw new ReadException("File read failed");
        }
    }

    public VectorClock fetchVectorClock(String folder, String fileName) {
        File file = new File(System.getProperty("user.dir") + File.separator + folder + File.separator + "vector_clock_" + fileName);
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
            return new VectorClock(bufferedReader.readLine());
        } catch (Exception e) {
            e.printStackTrace();
            throw new ReadException("Reading from vector clock file failed");
        }
    }

    public ResponseEntity<List<FileWithVectorClock>> retrieve(String fileName) {
        ArrayList<DynamoNode> nodes = hashManager.getNodes(fileName);
        Optional<DynamoNode> mayBeFirstNode = nodes.stream().filter(DynamoNode::isSelfAware).findFirst();
        final Map<FileWithVectorClock, DynamoNode> filesWithVectorClocks = Collections.synchronizedMap(new HashMap<>());
        try {
            if (mayBeFirstNode.isPresent()) {
                DynamoNode node = mayBeFirstNode.get();
                String folder = node.getAddress().replaceAll("\\.", "_");
                FileWithVectorClock fileWithVectorClock = retrieveObjectInternal(folder, fileName);
                filesWithVectorClocks.put(fileWithVectorClock, node);
                System.out.println("Vector clock from own " + fileWithVectorClock);
                nodes.remove(node);
                int readQuorum = Quorum.getReadQuorum();
                readQuorum--;
                filesWithVectorClocks.putAll(retrieveFromReplicas(fileName, nodes, readQuorum));

            } else {
                filesWithVectorClocks.putAll(retrieveFromReplicas(fileName, nodes, Quorum.getReadQuorum()));
            }
        } catch (ReadException e) {
            throw e;
        }

        System.out.println("Before sort" + filesWithVectorClocks);

        List<FileWithVectorClock> fileWithVectorClocks = ensureEventualConsistency(fileName, filesWithVectorClocks);
        return ResponseEntity.ok(fileWithVectorClocks);
    }

    @NotNull
    private List<FileWithVectorClock> ensureEventualConsistency(String fileName, Map<FileWithVectorClock, DynamoNode> filesWithVectorClocks) {
        List<FileWithVectorClock> vectorClocks = filesWithVectorClocks.keySet().stream().toList();
        System.out.println("File clock" + filesWithVectorClocks);

        try {
            FileWithVectorClock clockOne = vectorClocks.get(0);
            FileWithVectorClock clockTwo = vectorClocks.get(1);
            if (clockOne.getVectorClock().compareTo(clockTwo.getVectorClock()) < 0) {
                updateNodesLaggingBehind(fileName, filesWithVectorClocks, clockTwo, clockOne);
            } else if (clockOne.getVectorClock().compareTo(clockTwo.getVectorClock()) > 0) {
                updateNodesLaggingBehind(fileName, filesWithVectorClocks, clockOne, clockTwo);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new ConsistencyException(vectorClocks);
        }

        return vectorClocks;
    }

    private void updateNodesLaggingBehind(String fileName, Map<FileWithVectorClock, DynamoNode> filesWithVectorClocks,
                                          FileWithVectorClock clockAhead, FileWithVectorClock clockBehind) {
        DynamoNode node = filesWithVectorClocks.get(clockBehind);
        String folder = node.getAddress().replaceAll("\\.", "_");
        MultipartFile file = new CommonMultipartFile(clockAhead.getFile().getBytes(), fileName);
        dynamoClient.storeToReplica(URIHelper.createURI(node.getAddress()), file, folder,
                clockAhead.getVectorClock().toString());
    }

    public FileWithVectorClock retrieveObjectInternal(String folder, String fileName) {
        byte[] file = fetchFile(folder, fileName);
        VectorClock vectorClock = fetchVectorClock(folder, fileName);
        return new FileWithVectorClock(new String(file), vectorClock);
    }

    public Map<FileWithVectorClock, DynamoNode> retrieveFromReplicas(String fileName, ArrayList<DynamoNode> nodes, int readQuorum) {

        final CountDownLatch latch = new CountDownLatch(readQuorum);
        try {
            final Map<FileWithVectorClock, DynamoNode> files = Collections.synchronizedMap(new HashMap<>());
            for (DynamoNode node : nodes) {
                CompletableFuture.runAsync(() -> {
                    try {
                        String folder = node.getAddress().replaceAll("\\.", "_");
                        ResponseEntity<FileWithVectorClock> fileWithVectorClockResponseEntity = dynamoClient.retrieveFromReplica(URIHelper.createURI(node.getAddress()), folder, fileName);
                        FileWithVectorClock fileWithVectorClock = fileWithVectorClockResponseEntity.getBody();
                        assert fileWithVectorClock != null;
                        fileWithVectorClock.setNode(node.getAddress());
                        files.put(fileWithVectorClock, node);
                        latch.countDown();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }, taskExecutor);
            }
            if (!latch.await(10, TimeUnit.SECONDS)) {
                throw new ReadException("Read quorum condition failed");
            }
            return files;
        } catch (ReadException e) {
            throw e;
        } catch (Exception e) {
            e.printStackTrace();
            throw new ReadException("Read operation failed");
        }
    }
}