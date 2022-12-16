package com.distributedkeyvaluestore.keyvalue;

import com.distributedkeyvaluestore.client.DynamoClient;
import com.distributedkeyvaluestore.client.URIHelper;
import com.distributedkeyvaluestore.consistenthash.HashManager;
import com.distributedkeyvaluestore.exception.ConsistencyException;
import com.distributedkeyvaluestore.exception.ReadException;
import com.distributedkeyvaluestore.exception.RingEmptyException;
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
        try {
            String fileName = file.getOriginalFilename();
            List<DynamoNode> nodes = hashManager.getNodes(fileName).stream()
                    .sorted(Comparator.comparing(DynamoNode::getNumber))
                    .collect(Collectors.toList());

            Optional<DynamoNode> mayBeFirstNode = nodes.stream().filter(DynamoNode::isSelfAware).findFirst();
            if (mayBeFirstNode.isPresent()) {
                DynamoNode node = mayBeFirstNode.get();
                int vectorIndex = nodes.indexOf(node);
                storeObjectInternal(file, node, vectorIndex);
                nodes.remove(mayBeFirstNode.get());
                int writeQuorum = Quorum.getWriteQuorum();
                writeQuorum--;
                storeToReplicas(file, nodes, writeQuorum, vectorIndex);
                return ResponseEntity.ok("Write operation succeeded on node number " + node.getNumber() +
                        " with ip " + node.getAddress());
            } else {
                return forwardToNode(file, nodes.get(0));
            }
        } catch (RingEmptyException e) {
          throw new WriteException("Write operation failed, " + e.getMessage());
        } catch (WriteException e) {
          throw e;
        } catch (Exception e) {
            e.printStackTrace();
            throw new WriteException("Write operation failed, " + e.getMessage());
        }
    }

    private void storeObjectInternal(MultipartFile file, DynamoNode node, int vectorIndex) {
        createFile(file, node.getAddress().replaceAll("\\.", "_"));
        createVectorClockForFile(file, node.getAddress().replaceAll("\\.", "_"), vectorIndex);
    }

    public void createVectorClockForFile(MultipartFile file, String folder, int vectorIndex) {
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
                                int writeQuorum, int vectorIndex) {
        final CountDownLatch latch = new CountDownLatch(writeQuorum);
        try {
            for (DynamoNode node : nodes) {
                String folder = node.getAddress().replaceAll("\\.", "_");
                CompletableFuture.runAsync(() -> {
                    try {
                        dynamoClient.storeToReplicaUsingVectorIndex(URIHelper.createURI(node.getAddress()), file, folder,
                                vectorIndex);
                        latch.countDown();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }, taskExecutor);
            }
            if (!latch.await(10, TimeUnit.SECONDS)) {
                throw new WriteException("Write operation failed: Write quorum condition failed");
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
            throw new WriteException("Write operation failed: File write failed");
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
            throw new ReadException("Read operation failed: File read failed");
        }
    }

    public VectorClock fetchVectorClock(String folder, String fileName) {
        File file = new File(System.getProperty("user.dir") + File.separator + folder + File.separator + "vector_clock_" + fileName);
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
            return new VectorClock(bufferedReader.readLine());
        } catch (Exception e) {
            e.printStackTrace();
            throw new ReadException("Read operation failed: Reading from vector clock file failed");
        }
    }

    public ResponseEntity<List<FileWithVectorClock>> retrieve(String fileName) {
        try {
            ArrayList<DynamoNode> nodes = hashManager.getNodes(fileName);
            Optional<DynamoNode> mayBeFirstNode = nodes.stream().filter(DynamoNode::isSelfAware).findFirst();
            final Map<FileWithVectorClock, DynamoNode> fileWithVectorClockToNode = Collections.synchronizedMap(new HashMap<>());

            if (mayBeFirstNode.isPresent()) {
                DynamoNode node = mayBeFirstNode.get();
                String folder = node.getAddress().replaceAll("\\.", "_");
                FileWithVectorClock fileWithVectorClock = retrieveObjectInternal(folder, fileName);
                fileWithVectorClockToNode.put(fileWithVectorClock, node);
                System.out.println("Vector clock from own " + fileWithVectorClock);
                nodes.remove(node);
                int readQuorum = Quorum.getReadQuorum();
                readQuorum--;
                fileWithVectorClockToNode.putAll(retrieveFromReplicas(fileName, nodes, readQuorum));

            } else {
                fileWithVectorClockToNode.putAll(retrieveFromReplicas(fileName, nodes, Quorum.getReadQuorum()));
            }
            System.out.println("Before sort" + fileWithVectorClockToNode);

            List<FileWithVectorClock> fileWithVectorClocks = ensureEventualConsistency(fileName, fileWithVectorClockToNode);
            return ResponseEntity.ok(fileWithVectorClocks);
        } catch (RingEmptyException e) {
            throw new ReadException("Read operation failed, " + e.getMessage());
        } catch (ReadException e) {
            throw e;
        } catch (Exception e) {
            e.printStackTrace();
            throw new ReadException("Read operation failed, " + e.getMessage());
        }
    }

    @NotNull
    private List<FileWithVectorClock> ensureEventualConsistency(String fileName, Map<FileWithVectorClock, DynamoNode> fileWithVectorClockToNode) {
        List<FileWithVectorClock> fileWithVectorClocks = fileWithVectorClockToNode.keySet().stream()
                .sorted(Comparator.comparing(FileWithVectorClock::getVectorClock)).toList();
        System.out.println("File clock" + fileWithVectorClockToNode);
        try {
            int size = fileWithVectorClocks.size();
            FileWithVectorClock latest = fileWithVectorClocks.get(size - 1);
            List<DynamoNode> nodesLaggingBehind = new ArrayList<>();

            int i = size - 1;
            while (i >= 0) {
                FileWithVectorClock fileWithVectorClock = fileWithVectorClocks.get(i);
                if (fileWithVectorClock.getVectorClock().compareTo(latest.getVectorClock()) < 0) {
                    nodesLaggingBehind.add(fileWithVectorClockToNode.get(fileWithVectorClock));
                }
                i--;
            }
            updateNodesLaggingBehind(fileName, latest, nodesLaggingBehind);
        } catch (Exception e) {
            e.printStackTrace();
            throw new ConsistencyException(fileWithVectorClocks);
        }

        return fileWithVectorClocks;
    }

    private void updateNodesLaggingBehind(String fileName, FileWithVectorClock clockAhead,
                                          List<DynamoNode> nodesLaggingBehind) {
        for (DynamoNode node : nodesLaggingBehind) {
            String folder = node.getAddress().replaceAll("\\.", "_");
            MultipartFile file = new CommonMultipartFile(clockAhead.getFile().getBytes(), fileName);
            dynamoClient.storeToReplicaUsingVectorClock(URIHelper.createURI(node.getAddress()), file, folder,
                    clockAhead.getVectorClock().toString());
        }
    }

    public FileWithVectorClock retrieveObjectInternal(String folder, String fileName) {
        Optional<DynamoNode> node = hashManager.getAllNodes().stream().filter(DynamoNode::isSelfAware).findFirst();

        if (node.isPresent()) {
            byte[] file = fetchFile(folder, fileName);
            VectorClock vectorClock = fetchVectorClock(folder, fileName);
            return new FileWithVectorClock(new String(file), vectorClock, node.get().getAddress());
        }

        throw new ReadException("Read operation failed: Unable to retrieve file with vector clock");
    }

    public Map<FileWithVectorClock, DynamoNode> retrieveFromReplicas(String fileName, ArrayList<DynamoNode> nodes, int readQuorum) {

        final CountDownLatch latch = new CountDownLatch(readQuorum);
        try {
            final Map<FileWithVectorClock, DynamoNode> fileWithVectorClockToNode = Collections.synchronizedMap(new HashMap<>());
            for (DynamoNode node : nodes) {
                CompletableFuture.runAsync(() -> {
                    try {
                        String folder = node.getAddress().replaceAll("\\.", "_");
                        ResponseEntity<FileWithVectorClock> fileWithVectorClockResponseEntity = dynamoClient.retrieveFromReplica(URIHelper.createURI(node.getAddress()), folder, fileName);
                        fileWithVectorClockToNode.put(fileWithVectorClockResponseEntity.getBody(), node);
                        latch.countDown();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }, taskExecutor);
            }
            if (!latch.await(10, TimeUnit.SECONDS)) {
                throw new ReadException("Read quorum condition failed");
            }
            return fileWithVectorClockToNode;
        } catch (ReadException e) {
            throw e;
        } catch (Exception e) {
            e.printStackTrace();
            throw new ReadException("Read operation failed");
        }
    }
}