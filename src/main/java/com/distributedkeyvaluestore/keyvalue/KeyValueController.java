package com.distributedkeyvaluestore.keyvalue;

import com.distributedkeyvaluestore.models.FileWithVectorClock;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;

@RestController
@RequestMapping("/object")
public class KeyValueController {

    private final KeyValueService keyValueService;

    public KeyValueController(KeyValueService keyValueService) {
        this.keyValueService = keyValueService;
    }

    @PutMapping("/store")
    ResponseEntity<String> storeObject(@RequestParam("file") MultipartFile file) {
        return keyValueService.store(file);
    }

    @GetMapping("/retrieve/{fileName}")
    ResponseEntity<List<FileWithVectorClock>> retrieveObject(@PathVariable("fileName") String fileName) {
        return keyValueService.retrieve(fileName);
    }

    @PutMapping("/storeToReplica/{folderName}/{vectorClock}")
    ResponseEntity<String> storeToReplica(@RequestParam("file") MultipartFile file,
                                          @PathVariable("folderName") String folderName,
                                          @PathVariable("vectorClock") String vectorClockAsString) {
        keyValueService.createFile(file, folderName);
        keyValueService.createVectorClockForFile(file, folderName, vectorClockAsString);
        return ResponseEntity.ok("File created successfully");
    }

    @GetMapping("/retrieveFromReplica/{folder}/{fileName}")
    ResponseEntity<byte[]> retrieveFromReplica(@PathVariable("folder") String folder,
                                               @PathVariable("fileName") String fileName) {
        return ResponseEntity.ok(keyValueService.fetchFile(folder, fileName));
    }
}
