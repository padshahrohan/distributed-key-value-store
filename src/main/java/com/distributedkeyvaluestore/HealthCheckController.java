package com.distributedkeyvaluestore;

import com.distributedkeyvaluestore.consistenthash.HashManager;
import com.distributedkeyvaluestore.models.DynamoNode;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/healthCheck")
public class HealthCheckController {

    private final HashManager<DynamoNode> hashManager;

    public HealthCheckController(HashManager<DynamoNode> hashManager) {
        this.hashManager = hashManager;
    }

    @GetMapping
    ResponseEntity<Boolean> healthCheck() {
        return ResponseEntity.ok(hashManager.isRingCreated());
    }

}
