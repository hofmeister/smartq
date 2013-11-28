package com.vonhof.smartq;


import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;

public class PostgresTaskStore<T extends Task> implements TaskStore<T> {

    private static final Logger log = Logger.getLogger(PostgresTaskStore.class);

    private final int STATE_QUEUED = 1;
    private final int STATE_RUNNING = 2;

    private final String url;  //jdbc:postgresql://host:port/database
    private final String username;
    private final String password;
    private volatile boolean isolated = false;

    private String tableName = "queue";

    private ThreadLocal<PostgresClient> client = new ThreadLocal<PostgresClient>() {
        @Override
        protected PostgresClient initialValue() {
            try {
                PostgresClient client = new PostgresClient();
                client.startListening();
                return client;
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    };

    private final Class<T> taskClass;

    private DocumentSerializer documentSerializer = new JacksonDocumentSerializer();


    public PostgresTaskStore(Class<T> taskClass) throws SQLException {
        this(taskClass, "jdbc:postgresql://localhost/smartq", "porta", "porta");
    }

    public PostgresTaskStore(Class<T> taskClass, String url, String username, String password) throws SQLException {
        this.taskClass = taskClass;
        this.url = url;
        this.username = username;
        this.password = password;
        connect();
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    private void connect() throws SQLException {
        client.get();
    }

    private synchronized PostgresClient client() {
        return client.get();
    }

    @Override
    public T get(UUID id) {
        try {
            return client().queryOne(String.format("SELECT * FROM \"%s\" WHERE id = ?", tableName), id);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void remove(T task) {
        remove(task.getId());
    }

    @Override
    public void remove(UUID id) {
        try {
            client().update(String.format("DELETE FROM \"%s\" WHERE id = ?", tableName), id);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void queue(T task) {
        try {
            client().update(
                String.format("INSERT INTO \"%s\" (id, content, state, type, priority, estimate) VALUES (?, ?, ?, ?, ?, ?)", tableName),
                    task.getId(),
                    documentSerializer.serialize(task).getBytes("UTF-8"),
                    STATE_QUEUED,
                    task.getType(),
                    task.getPriority(),
                    task.getEstimatedDuration()
                    );
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void reset() {
        try {
            client().update(String.format("DELETE FROM \"%s\"", tableName));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void run(T task) {
        try {
            client().update(
                String.format("UPDATE \"%s\" SET content = ?,state = ? WHERE id = ?", tableName),
                    documentSerializer.serialize(task).getBytes("UTF-8"),
                    STATE_RUNNING,
                    task.getId());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Iterator<T> getQueued() {
        return client().getList(STATE_QUEUED);
    }

    @Override
    public Iterator<T> getQueued(String type) {
        return client().getList(STATE_QUEUED, type);
    }

    @Override
    public Iterator<T> getRunning() {
        return client().getList(STATE_RUNNING);
    }

    @Override
    public Iterator<T> getRunning(String type) {
        return client().getList(STATE_RUNNING, type);
    }

    @Override
    public long queueSize() {
        return client().count(STATE_QUEUED);
    }

    @Override
    public long runningCount() {
        return client().count(STATE_RUNNING);
    }

    @Override
    public long queueSize(String type) {
        return client().count(STATE_QUEUED, type);
    }

    @Override
    public long runningCount(String type) {
        return client().count(STATE_RUNNING, type);
    }

    @Override
    public Set<String> getTypes() {
        try {
            return client().queryStringSet(String.format("SELECT DISTINCT type from \"%s\"", tableName));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long getQueuedETA(String type) {
        return client().sum(STATE_QUEUED, type);
    }

    @Override
    public long getQueuedETA() {
        return client().sum(STATE_QUEUED);
    }

    @Override
    public synchronized <U> U isolatedChange(Callable<U> callable) throws InterruptedException {
        try {
            boolean isolatedInThis = false;
            if (!isolated) {
                client().connection.setAutoCommit(false);
                client().lockTable();
                isolated = isolatedInThis = true;
            }

            U result = callable.call();
            if (isolatedInThis) {
                client().connection.commit();
                client().connection.setAutoCommit(true);
            }
            return result;
        } catch (Exception e) {
            try {
                client().connection.rollback();
            } catch (SQLException e1) {}
            throw new RuntimeException(e);
        }
    }

    @Override
    public synchronized void waitForChange() throws InterruptedException {
        this.wait();
    }

    @Override
    public synchronized void signalChange() {
        try {
            client().pgNotify();
            log.debug("Send PG notification");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void createTable() throws IOException, SQLException {
        try {
            client().query(String.format("select 1 from \"%s\" limit 1",tableName));
            return;
        } catch (SQLException ex) {
            //Do nothing - above is just a check if the table exists
        }

        String sql = IOUtils.toString(getClass().getResource("/pgtable.sql")).replaceAll("%tableName%", tableName);
        client().update(sql);
    }

    public void dropTable() throws SQLException {
        client().update(String.format("DROP TABLE \"%s\"",tableName));
    }

    public void close() throws SQLException {
        client().connection.close();
    }

    private class PostgresClient {
        private final Connection connection;
        private final PGConnection pgconn;
        private final Listener listener = new Listener();

        private PostgresClient() throws SQLException {
            connection = DriverManager.getConnection(url, username, password);
            connection.setAutoCommit(true);
            pgconn = (PGConnection) connection;
        }

        public void startListening() throws SQLException {
            listener.start();
        }

        private void lockTable() throws SQLException {
            execute(String.format("LOCK TABLE \"%s\" IN ACCESS EXCLUSIVE MODE",tableName));
            log.info("Locking table");
        }

        private List<Map<String,Object>> query(String sql, Object ... args) throws SQLException {
            try {
                PreparedStatement stmt = stmt(sql, args);

                ResultSet result = stmt.executeQuery();

                List<Map<String,Object>> out = new ArrayList<Map<String, Object>>();
                while (result.next()) {
                    Map<String,Object> row = new HashMap<String, Object>();
                    ResultSetMetaData md = stmt.getMetaData();
                    for(int j = 0; j < md.getColumnCount(); j++) {
                        String name = md.getColumnName(j + 1);
                        Object value = result.getObject(j + 1);
                        row.put(name,value);
                    }
                    out.add(row);
                }
                stmt.close();
                return out;

            } catch (SQLException e) {
                log.debug("SQL: "+ sql,e);
                throw e;
            }
        }

        private T queryOne(String sql, Object ... args) throws SQLException {
            List<T> rows = queryAll(sql, args);
            if (rows.isEmpty()) {
                return null;
            }
            return rows.get(0);
        }

        private long queryForLong(String sql, Object... args) throws SQLException {
            PreparedStatement stmt = stmt(sql, args);

            ResultSet result = stmt.executeQuery();
            List<Map<String,Object>> out = new ArrayList<Map<String, Object>>();
            while (result.next()) {
                return result.getLong(1);
            }
            return 0;
        }

        private List<T> queryAll(String sql, Object... args) throws SQLException {
            List<Map<String, Object>> rows = query(sql, args);
            List<T> out = new ArrayList<T>();
            if (rows.isEmpty()) {
                return out;
            }

            for(Map<String,Object> row : rows) {
                byte[] content = (byte[]) rows.get(0).get("content");

                try {
                    T task = documentSerializer.deserialize(new String(content, "UTF-8"), taskClass);
                    out.add(task);
                } catch (IOException e) {
                    log.warn(String.format("Failed to deserialize task: %s",row.get("id")), e);
                }
            }

            return out;
        }

        private void execute(String sql, Object ... args) throws SQLException {
            try {
                PreparedStatement stmt = stmt(sql, args);
                stmt.execute();
                stmt.close();
            } catch (SQLException e) {
                log.debug("SQL: "+ sql,e);
                throw e;
            }
        }

        private void update(String sql, Object ... args) throws SQLException {
            try {
                PreparedStatement stmt = stmt(sql, args);
                stmt.executeUpdate();
                stmt.close();
            } catch (SQLException e) {
                log.debug("SQL: "+ sql,e);
                throw e;
            }
        }

        private PreparedStatement stmt(String sql, Object ... args) {
            try {
                PreparedStatement stmt = connection.prepareStatement(sql);
                int i = 1;
                for(Object arg : args) {
                    stmt.setObject(i, arg);
                    i++;
                }
                return stmt;
            } catch (SQLException e) {
                throw new RuntimeException(sql, e);
            }
        }

        public Iterator<T> getList(int state) {
            return getList(state, null);
        }

        public Iterator<T> getList(int state, String type) {
            try {
                if (type != null && !type.isEmpty()) {
                    return client()
                            .queryAll(
                                String.format("SELECT * FROM \"%s\" WHERE state = ? and type = ? ORDER BY priority DESC LIMIT 100", tableName),state,type
                            ).iterator();
                } else {
                    return client()
                            .queryAll(
                                String.format("SELECT * FROM \"%s\" WHERE state = ? ORDER BY priority DESC LIMIT 100", tableName), state
                            ).iterator();
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        public long count(int state) {
            return count(state, null);
        }

        public long count(int state, String type) {
            try {
                if (type != null && !type.isEmpty()) {
                    return client().queryForLong(
                            String.format("SELECT count(*) as count FROM \"%s\" WHERE state = ? and type = ?", tableName), state, type);
                } else {
                    return client().queryForLong(
                            String.format("SELECT count(*) as count FROM \"%s\" WHERE state = ?", tableName), state);
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        public long sum(int state) {
            return sum(state, null);
        }

        public long sum(int state, String type) {
            try {
                if (type != null && !type.isEmpty()) {
                    return client().queryForLong(String.format("SELECT sum(estimate) from \"%s\" WHERE state = ? AND type = ?", tableName),
                            state,type);
                } else {
                    return client().queryForLong(String.format("SELECT sum(estimate) from \"%s\" WHERE state = ?", tableName),
                            state,type);
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }



        public Set<String> queryStringSet(String sql, Object ... args) throws SQLException {
            PreparedStatement stmt = stmt(sql, args);
            ResultSet result = stmt.executeQuery();
            Set<String> out = new HashSet<String>();
            while(result.next()) {
                out.add(result.getString(1));
            }
            return out;
        }

        public void pgListen() throws SQLException {
            execute(String.format("LISTEN \"%s_event\"",tableName));
        }

        public void pgNotify() throws SQLException {
            execute(String.format("NOTIFY \"%s_event\"",tableName));
        }

        public boolean hasNotifications() throws SQLException {
            query("SELECT 1"); //Trigger notification push
            PGNotification[] notifications = pgconn.getNotifications();
            if (notifications != null) {
                return notifications.length > 0;
            }
            return false;
        }

        public <T> T callInTransaction(Callable<T> callable) throws SQLException {
            try {
                T out = callable.call();
                connection.commit();
                return out;
            } catch (Exception e) {
                connection.rollback();
            }
            return null;
        }

        public void doInTransaction(Callable callable) throws SQLException {
            try {
                callable.call();
                connection.commit();
            } catch (Exception e) {
                connection.rollback();
            }
        }
    }

    private class Listener extends Thread {
        private Listener() {
            super("pg-queue-listener");

        }

        @Override
        public void run() {
            final PostgresTaskStore store = PostgresTaskStore.this;
            final PostgresClient client;
            try {
                client = new PostgresClient();
                client.pgListen();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }

            while(true) {
                try {
                    if (client.hasNotifications()) {
                        log.debug("PG returned notifications");
                        synchronized (store) {
                            store.notifyAll();
                        }
                    }
                } catch (SQLException e) {
                    synchronized (store) {
                        store.notifyAll();
                    }
                    throw new RuntimeException(e);
                }

                synchronized (this) {
                    try {
                        wait(1000);
                    } catch (InterruptedException e) {
                        return;
                    }
                }
            }
        }
    }
}
