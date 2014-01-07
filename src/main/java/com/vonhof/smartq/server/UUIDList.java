package com.vonhof.smartq.server;

import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;

public class UUIDList extends ArrayList<UUID> {

    public UUIDList(int i) {
        super(i);
    }

    public UUIDList() {
    }

    public UUIDList(Collection<UUID> uuids) {
        super(uuids);
    }
}
