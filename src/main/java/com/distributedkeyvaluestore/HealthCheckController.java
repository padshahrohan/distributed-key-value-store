package com.distributedkeyvaluestore;

import com.distributedkeyvaluestore.consistenthash.HashingManager;
import com.distributedkeyvaluestore.models.DynamoNode;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/healthCheck")
public class HealthCheckController {

    private final HashingManager<DynamoNode> hashingManager;

    public HealthCheckController(HashingManager<DynamoNode> hashingManager) {
        this.hashingManager = hashingManager;
    }

    @GetMapping
    ResponseEntity<Boolean> healthCheck() {
        return ResponseEntity.ok(hashingManager.isRingCreated());
    }

}
