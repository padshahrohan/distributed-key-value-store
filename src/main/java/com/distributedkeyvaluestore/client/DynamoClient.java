package com.distributedkeyvaluestore.client;

import com.distributedkeyvaluestore.models.FileWithVectorClock;
import feign.Param;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.cloud.openfeign.FeignClientsConfiguration;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.multipart.MultipartFile;

import java.net.URI;

@FeignClient(name = "dynamoClient", url = "http://this-is-just-a-placeholder")
public interface DynamoClient {

    @PostMapping(value = "/object/storeToReplica/{folder}/{vectorClock}", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    void storeToReplica(URI baseUrl, @Param("file") MultipartFile file,
                        @PathVariable("folder") String folder,
                        @PathVariable("vectorClock") String vectorClock);

    @GetMapping(value = "/object/retrieveFromReplica/{folder}/{fileName}")
    ResponseEntity<FileWithVectorClock> retrieveFromReplica(URI baseUrl, @PathVariable("fileName") String fileName,
                                            @PathVariable("folder") String folder);

    @PostMapping(value = "/object/store", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    ResponseEntity<String> forwardToNode(URI baseUrl, @Param("file") MultipartFile file);
}
