package com.campuslab.ms_campuslab_admin.kafka;

import java.util.List;
import java.util.Map;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public final class KafkaDtos {

    private KafkaDtos() {
    }

    public record CreateTopicRequest(
            @NotBlank String name,
            @Min(1) int partitions,
            @Min(1) int replicationFactor,
            Map<String, String> configs) {
    }

    public record UpdateConfigRequest(@NotNull Map<String, String> configs) {
    }

    public record PartitionsRequest(@Min(1) int total) {
    }

    public record PartitionInfo(int partition, Integer leader, List<Integer> replicas, List<Integer> isr) {
    }

    public record TopicInfo(String name, int partitions, int replicationFactor,
                            List<PartitionInfo> detail, Map<String, String> configs) {
    }

    public record MemberInfo(String consumerId, String clientId, String host, List<String> partitions) {
    }

    public record LagInfo(String topic, int partition, long committedOffset, long endOffset, long lag) {
    }

    public record GroupInfo(String groupId, String state, String coordinator, int members,
                            List<MemberInfo> memberDetail, List<LagInfo> lag, long totalLag) {
    }
}
