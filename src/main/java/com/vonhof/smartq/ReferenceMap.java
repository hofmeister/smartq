package com.vonhof.smartq;


import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public class ReferenceMap {
    private Map<String, TaskOrderInfo> firstTasks = new HashMap<>();
    private Map<String, TaskOrderInfo> lastTasks = new HashMap<>();
    private CountMap<String> refCounts = new CountMap<>();
    private AtomicLong orderCount = new AtomicLong();

    public void add(Task t) {
        if (StringUtils.isEmpty(t.getReferenceId())) {
            return;
        }

        refCounts.increment(t.getReferenceId(), 1);

        TaskOrderInfo newTask = new TaskOrderInfo(t, orderCount.incrementAndGet());

        TaskOrderInfo firstTask = firstTasks.get(t.getReferenceId());
        if (newTask.isBefore(firstTask)) {
            firstTasks.put(t.getReferenceId(), newTask);
        }
        TaskOrderInfo lastTask = lastTasks.get(t.getReferenceId());
        if (newTask.isAfter(lastTask)) {
            lastTasks.put(t.getReferenceId(), newTask);
        }
    }

    public void remove(Task t) {
        if (StringUtils.isEmpty(t.getReferenceId())) {
            return;
        }

        long count = refCounts.decrement(t.getReferenceId(), 1);
        if (count < 1) {
            firstTasks.remove(t.getReferenceId());
            lastTasks.remove(t.getReferenceId());
        }
    }

    public UUID getFirst(String referenceId) {
        TaskOrderInfo orderInfo = firstTasks.get(referenceId);
        return orderInfo != null ? orderInfo.getTaskId() : null;
    }

    public UUID getLast(String referenceId) {
        TaskOrderInfo orderInfo = lastTasks.get(referenceId);
        return orderInfo != null ? orderInfo.getTaskId() : null;
    }


    private static class TaskOrderInfo {
        private final long created;
        private final long priority;
        private final long order;
        private final UUID taskId;

        private TaskOrderInfo(Task t, long order) {
            this.created = t.getCreated();
            this.priority = t.getPriority();
            this.order = order;
            this.taskId = t.getId();
        }

        private long getCreated() {
            return created;
        }

        private long getPriority() {
            return priority;
        }

        private long getOrder() {
            return order;
        }

        private UUID getTaskId() {
            return taskId;
        }

        public boolean isBefore(TaskOrderInfo other) {
            if (other == null) {
                return true;
            }

            if (priority > other.priority) {
                return true;
            } else if (priority < other.priority) {
                return false;
            }

            if (created < other.created) {
                return true;
            } else if (created > other.created) {
                return false;
            }

            if (order > other.order) {
                return false;
            }

            return true;
        }


        public boolean isAfter(TaskOrderInfo other) {
            if (other == null) {
                return true;
            }

            return other.isBefore(this);
        }
    }
}
