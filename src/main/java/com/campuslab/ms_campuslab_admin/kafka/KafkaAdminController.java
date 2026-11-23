package com.campuslab.ms_campuslab_admin.kafka;

import java.util.List;
import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import com.campuslab.ms_campuslab_admin.kafka.KafkaDtos.CreateTopicRequest;
import com.campuslab.ms_campuslab_admin.kafka.KafkaDtos.GroupInfo;
import com.campuslab.ms_campuslab_admin.kafka.KafkaDtos.PartitionsRequest;
import com.campuslab.ms_campuslab_admin.kafka.KafkaDtos.TopicInfo;
import com.campuslab.ms_campuslab_admin.kafka.KafkaDtos.UpdateConfigRequest;

import jakarta.validation.Valid;

@RestController
@RequestMapping("/api/admin/kafka")
public class KafkaAdminController {

    private final KafkaAdminService service;

    public KafkaAdminController(KafkaAdminService service) {
        this.service = service;
    }

    // ---- tópicos
    @GetMapping("/topics")
    public List<String> listTopics() {
        return service.listTopics();
    }

    @GetMapping("/topics/{name}")
    public TopicInfo describeTopic(@PathVariable String name) {
        return service.describeTopic(name);
    }

    @PostMapping("/topics")
    @ResponseStatus(HttpStatus.CREATED)
    public TopicInfo createTopic(@Valid @RequestBody CreateTopicRequest req) {
        service.createTopic(req);
        return service.describeTopic(req.name());
    }

    @PutMapping("/topics/{name}/config")
    public TopicInfo updateConfig(@PathVariable String name, @Valid @RequestBody UpdateConfigRequest req) {
        service.updateConfig(name, req.configs());
        return service.describeTopic(name);
    }

    @PutMapping("/topics/{name}/partitions")
    public TopicInfo increasePartitions(@PathVariable String name, @Valid @RequestBody PartitionsRequest req) {
        service.increasePartitions(name, req.total());
        return service.describeTopic(name);
    }

    @DeleteMapping("/topics/{name}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deleteTopic(@PathVariable String name) {
        service.deleteTopic(name);
    }

    // ---- consumer groups
    @GetMapping("/consumer-groups")
    public List<String> listGroups() {
        return service.listGroups();
    }

    @GetMapping("/consumer-groups/{groupId}")
    public GroupInfo describeGroup(@PathVariable String groupId) {
        return service.describeGroup(groupId);
    }

    @PostMapping("/consumer-groups/{groupId}/reset-offsets")
    public Map<String, String> resetOffsets(@PathVariable String groupId, @RequestParam String topic) {
        service.resetToEarliest(groupId, topic);
        return Map.of("groupId", groupId, "topic", topic, "resetTo", "earliest");
    }

    @DeleteMapping("/consumer-groups/{groupId}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deleteGroup(@PathVariable String groupId) {
        service.deleteGroup(groupId);
    }
}
