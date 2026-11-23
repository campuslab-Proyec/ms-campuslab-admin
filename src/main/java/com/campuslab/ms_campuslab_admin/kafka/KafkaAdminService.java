package com.campuslab.ms_campuslab_admin.kafka;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import com.campuslab.ms_campuslab_admin.kafka.KafkaDtos.CreateTopicRequest;
import com.campuslab.ms_campuslab_admin.kafka.KafkaDtos.GroupInfo;
import com.campuslab.ms_campuslab_admin.kafka.KafkaDtos.LagInfo;
import com.campuslab.ms_campuslab_admin.kafka.KafkaDtos.MemberInfo;
import com.campuslab.ms_campuslab_admin.kafka.KafkaDtos.PartitionInfo;
import com.campuslab.ms_campuslab_admin.kafka.KafkaDtos.TopicInfo;

@Service
public class KafkaAdminService {

    private static final long TIMEOUT_S = 15;

    /** Configuraciones de tópico que interesa mostrar. */
    private static final Set<String> RELEVANT_CONFIGS = Set.of(
            "retention.ms", "retention.bytes", "cleanup.policy", "min.insync.replicas",
            "segment.ms", "segment.bytes", "min.cleanable.dirty.ratio", "max.message.bytes",
            "compression.type", "delete.retention.ms");

    private final AdminClient admin;

    public KafkaAdminService(AdminClient admin) {
        this.admin = admin;
    }

    // ------------------------------------------------------------------ tópicos

    public List<String> listTopics() {
        return call(() -> admin.listTopics().names().get(TIMEOUT_S, TimeUnit.SECONDS)).stream()
                .filter(n -> !TopicRules.isInternal(n))
                .sorted()
                .toList();
    }

    public TopicInfo describeTopic(String name) {
        TopicDescription d = call(() -> admin.describeTopics(List.of(name))
                .allTopicNames().get(TIMEOUT_S, TimeUnit.SECONDS).get(name));

        List<PartitionInfo> detail = d.partitions().stream()
                .map(p -> new PartitionInfo(
                        p.partition(),
                        p.leader() == null ? null : p.leader().id(),
                        p.replicas().stream().map(n -> n.id()).toList(),
                        p.isr().stream().map(n -> n.id()).toList()))
                .toList();
        int rf = detail.isEmpty() ? 0 : detail.get(0).replicas().size();
        return new TopicInfo(name, detail.size(), rf, detail, topicConfigs(name));
    }

    public void createTopic(CreateTopicRequest req) {
        TopicRules.assertManageable(req.name());
        NewTopic topic = new NewTopic(req.name(), req.partitions(), (short) req.replicationFactor());
        if (req.configs() != null && !req.configs().isEmpty()) {
            topic.configs(req.configs());
        }
        call(() -> admin.createTopics(List.of(topic)).all().get(TIMEOUT_S, TimeUnit.SECONDS));
    }

    public void updateConfig(String name, Map<String, String> configs) {
        TopicRules.assertManageable(name);
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, name);
        Collection<AlterConfigOp> ops = configs.entrySet().stream()
                .map(e -> new AlterConfigOp(new ConfigEntry(e.getKey(), e.getValue()), AlterConfigOp.OpType.SET))
                .toList();
        call(() -> admin.incrementalAlterConfigs(Map.of(resource, ops)).all().get(TIMEOUT_S, TimeUnit.SECONDS));
    }

    public void increasePartitions(String name, int total) {
        TopicRules.assertManageable(name);
        call(() -> admin.createPartitions(Map.of(name, NewPartitions.increaseTo(total)))
                .all().get(TIMEOUT_S, TimeUnit.SECONDS));
    }

    public void deleteTopic(String name) {
        TopicRules.assertManageable(name);
        call(() -> admin.deleteTopics(List.of(name)).all().get(TIMEOUT_S, TimeUnit.SECONDS));
    }

    private Map<String, String> topicConfigs(String name) {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, name);
        Config config = call(() -> admin.describeConfigs(List.of(resource))
                .all().get(TIMEOUT_S, TimeUnit.SECONDS).get(resource));
        Map<String, String> result = new LinkedHashMap<>();
        for (ConfigEntry e : config.entries()) {
            if (RELEVANT_CONFIGS.contains(e.name())) {
                result.put(e.name(), e.value());
            }
        }
        return result;
    }

    // --------------------------------------------------------- consumer groups

    public List<String> listGroups() {
        return call(() -> admin.listConsumerGroups().all().get(TIMEOUT_S, TimeUnit.SECONDS)).stream()
                .map(g -> g.groupId())
                .sorted()
                .toList();
    }

    public GroupInfo describeGroup(String groupId) {
        ConsumerGroupDescription d = call(() -> admin.describeConsumerGroups(List.of(groupId))
                .all().get(TIMEOUT_S, TimeUnit.SECONDS).get(groupId));

        List<MemberInfo> members = new ArrayList<>();
        for (MemberDescription m : d.members()) {
            members.add(new MemberInfo(m.consumerId(), m.clientId(), m.host(),
                    m.assignment().topicPartitions().stream().map(TopicPartition::toString).sorted().toList()));
        }

        List<LagInfo> lag = lagOf(groupId);
        long total = lag.stream().mapToLong(LagInfo::lag).sum();
        String coordinator = d.coordinator() == null ? null : d.coordinator().idString();
        return new GroupInfo(groupId, String.valueOf(d.state()), coordinator, members.size(), members, lag, total);
    }

    public void deleteGroup(String groupId) {
        call(() -> admin.deleteConsumerGroups(List.of(groupId)).all().get(TIMEOUT_S, TimeUnit.SECONDS));
    }

    /** Reinicia el offset del grupo al inicio del tópico (el grupo debe estar sin consumidores activos). */
    public void resetToEarliest(String groupId, String topic) {
        TopicRules.assertManageable(topic);
        TopicDescription desc = call(() -> admin.describeTopics(List.of(topic))
                .allTopicNames().get(TIMEOUT_S, TimeUnit.SECONDS).get(topic));

        Map<TopicPartition, OffsetSpec> request = new HashMap<>();
        desc.partitions().forEach(p -> request.put(new TopicPartition(topic, p.partition()), OffsetSpec.earliest()));
        Map<TopicPartition, ListOffsetsResultInfo> earliest = call(
                () -> admin.listOffsets(request).all().get(TIMEOUT_S, TimeUnit.SECONDS));

        Map<TopicPartition, OffsetAndMetadata> newOffsets = new HashMap<>();
        earliest.forEach((tp, info) -> newOffsets.put(tp, new OffsetAndMetadata(info.offset())));
        call(() -> admin.alterConsumerGroupOffsets(groupId, newOffsets).all().get(TIMEOUT_S, TimeUnit.SECONDS));
    }

    private List<LagInfo> lagOf(String groupId) {
        Map<TopicPartition, OffsetAndMetadata> committed = call(() -> admin.listConsumerGroupOffsets(groupId)
                .partitionsToOffsetAndMetadata().get(TIMEOUT_S, TimeUnit.SECONDS));
        if (committed.isEmpty()) {
            return List.of();
        }
        Map<TopicPartition, OffsetSpec> request = new HashMap<>();
        committed.keySet().forEach(tp -> request.put(tp, OffsetSpec.latest()));
        Map<TopicPartition, ListOffsetsResultInfo> ends = call(
                () -> admin.listOffsets(request).all().get(TIMEOUT_S, TimeUnit.SECONDS));

        List<LagInfo> result = new ArrayList<>();
        committed.forEach((tp, om) -> {
            long end = ends.get(tp).offset();
            result.add(new LagInfo(tp.topic(), tp.partition(), om.offset(), end, Math.max(0, end - om.offset())));
        });
        result.sort((a, b) -> a.topic().equals(b.topic())
                ? Integer.compare(a.partition(), b.partition())
                : a.topic().compareTo(b.topic()));
        return result;
    }

    // ------------------------------------------------------------------ errores

    private <T> T call(Callable<T> action) {
        try {
            return action.call();
        } catch (ExecutionException e) {
            throw translate(e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Operación interrumpida");
        } catch (TimeoutException e) {
            throw new ResponseStatusException(HttpStatus.GATEWAY_TIMEOUT, "Kafka no respondió a tiempo");
        } catch (ResponseStatusException e) {
            throw e;
        } catch (Exception e) {
            throw new ResponseStatusException(HttpStatus.BAD_GATEWAY, String.valueOf(e.getMessage()));
        }
    }

    private ResponseStatusException translate(Throwable cause) {
        String msg = cause == null ? "Error de Kafka" : String.valueOf(cause.getMessage());
        if (cause instanceof UnknownTopicOrPartitionException || cause instanceof GroupIdNotFoundException) {
            return new ResponseStatusException(HttpStatus.NOT_FOUND, msg);
        }
        if (cause instanceof TopicExistsException || cause instanceof GroupNotEmptyException) {
            return new ResponseStatusException(HttpStatus.CONFLICT, msg);
        }
        if (cause instanceof InvalidTopicException || cause instanceof InvalidReplicationFactorException
                || cause instanceof InvalidPartitionsException || cause instanceof InvalidConfigurationException) {
            return new ResponseStatusException(HttpStatus.BAD_REQUEST, msg);
        }
        return new ResponseStatusException(HttpStatus.BAD_GATEWAY, msg);
    }
}
