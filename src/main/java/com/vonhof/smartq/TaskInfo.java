package com.vonhof.smartq;


import java.util.Arrays;
import java.util.UUID;

public class TaskInfo {
    private final UUID id;
    private final String[] tags;
    private final String type;
    private final String referenceId;
    private final boolean running;
    private long started;
    private long ended;

    public TaskInfo(Task t) {
        this.id = t.getId();
        this.tags = (String[]) t.getTagSet().toArray(new String[t.getTags().size()]);
        this.type = t.getType();
        this.running = t.isRunning();
        this.started = t.getStarted();
        this.ended = t.getEnded();
        this.referenceId = t.getReferenceId();
    }

    public UUID getId() {
        return id;
    }

    public String[] getTags() {
        return tags;
    }

    public String getType() {
        return type;
    }

    public boolean isRunning() {
        return running;
    }

    public long getStarted() {
        return started;
    }

    public void setStarted(long started) {
        this.started = started;
    }

    public long getEnded() {
        return ended;
    }

    public void setEnded(long ended) {
        this.ended = ended;
    }

    public String getReferenceId() {
        return referenceId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass() && !getClass().equals(Task.class)) return false;

        TaskInfo taskInfo = (TaskInfo) o;

        if (id != null ? !id.equals(taskInfo.id) : taskInfo.id != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return id != null ? id.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "TaskInfo{" +
                "id=" + id +
                ", tags=" + Arrays.toString(tags) +
                '}';
    }
}
