package com.dataaggregator.api;

import com.dataaggregator.workflow.AsyncRunChangedEvent;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import org.springframework.transaction.event.TransactionalEventListener;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

@Service
@Profile("!worker")
public class AsyncEventStreamService {

    private final ConcurrentMap<RunScope, CopyOnWriteArrayList<SseEmitter>> emitters = new ConcurrentHashMap<>();
    private final OperationService operationService;

    public AsyncEventStreamService(OperationService operationService) {
        this.operationService = operationService;
    }

    public SseEmitter subscribe(String scopeType, String scopeId, String userId) throws IOException {
        List<Map<String, Object>> operations = operationService.operationsForScope(scopeType, scopeId, userId);
        RunScope scope = new RunScope(userId, scopeType, scopeId);
        SseEmitter emitter = new SseEmitter(0L);
        emitters.computeIfAbsent(scope, ignored -> new CopyOnWriteArrayList<>()).add(emitter);
        emitter.onCompletion(() -> remove(scope, emitter));
        emitter.onTimeout(() -> remove(scope, emitter));
        emitter.onError(ignored -> remove(scope, emitter));
        for (Map<String, Object> operation : operations) {
            send(scope, emitter, operation);
        }
        return emitter;
    }

    @TransactionalEventListener(fallbackExecution = true)
    public void asyncRunChanged(AsyncRunChangedEvent event) {
        RunScope scope = new RunScope(event.userId(), event.scopeType(), event.scopeId());
        List<SseEmitter> subscribers = emitters.get(scope);
        if (subscribers == null || subscribers.isEmpty()) {
            return;
        }

        Map<String, Object> operation = operationService.operation(event.operationId(), event.userId());
        for (SseEmitter emitter : subscribers) {
            send(scope, emitter, operation);
        }
    }

    private void send(RunScope scope, SseEmitter emitter, Map<String, Object> operation) {
        try {
            emitter.send(SseEmitter.event()
                    .name(operationService.sseEventName(operation))
                    .data(operationService.ssePayload(operation)));
            if (operationService.isSnapshotReady(operation)) {
                emitter.send(SseEmitter.event().name("snapshot_ready").data(operationService.ssePayload(operation)));
            }
        } catch (IOException | IllegalStateException exception) {
            remove(scope, emitter);
        }
    }

    private void remove(RunScope scope, SseEmitter emitter) {
        List<SseEmitter> scopedEmitters = emitters.get(scope);
        if (scopedEmitters == null) {
            return;
        }
        scopedEmitters.remove(emitter);
        if (scopedEmitters.isEmpty()) {
            emitters.remove(scope, scopedEmitters);
        }
    }

    private record RunScope(String userId, String scopeType, String scopeId) {}
}
