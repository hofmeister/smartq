package com.vonhof.smartq;


import com.vonhof.smartq.Task.State;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;

import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.Callable;

public class PostgresTaskStore<T extends Task> implements TaskStore<T> {

    private static final Logger log = Logger.getLogger(PostgresTaskStore.class);

    private final int STATE_QUEUED = 1;
    private final int STATE_RUNNING = 2;
    private final int STATE_ERROR = 3;

    private final String url;  //jdbc:postgresql://host:port/database
    private final String username;
    private final String password;
    private String tableName = "queue";

    private final ThreadLocal<PostgresClient> client = new ThreadLocal<PostgresClient>() {
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
        this(taskClass, "jdbc:postgresql://localhost/smartq", "henrik", "");
    }

    public PostgresTaskStore(Class<T> taskClass, String url, String username, String password) throws SQLException {
        this.taskClass = taskClass;
        this.url = url;
        this.username = username;
        this.password = password;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void connect() {
        client.get();
    }

    private PostgresClient client() {
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
    public void queue(final T task) {
        try {
            task.setState(State.PENDING);
            withinTransaction(new Callable<Void>() {

                @Override
                public Void call() throws Exception {
                    client().update(
                            String.format("INSERT INTO \"%s\" (id, content, state, priority, type) VALUES (?, ?, ?, ?, ?)", tableName),
                            task.getId(),
                            documentSerializer.serialize(task).getBytes("UTF-8"),
                            STATE_QUEUED,
                            task.getPriority(),
                            task.getType()
                    );

                    for (String tag : (Set<String>) task.getTags().keySet()) {
                        client().update(
                                String.format("INSERT INTO \"%s_tags\" (id, tag) VALUES (?, ?)", tableName),
                                task.getId(), tag
                        );
                    }

                    return null;
                }
            });
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
            task.setState(State.RUNNING);
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
    public void failed(T task) {
        try {
            task.setState(State.ERROR);
            client().update(
                    String.format("UPDATE \"%s\" SET content = ?,state = ? WHERE id = ?", tableName),
                    documentSerializer.serialize(task).getBytes("UTF-8"),
                    STATE_ERROR,
                    task.getId());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Iterator<T> getFailed() {
        return (Iterator<T>) client().getList(STATE_ERROR);
    }

    @Override
    public Iterator<T> getQueued() {
        return (Iterator<T>) client().getList(STATE_QUEUED);
    }

    @Override
    public Iterator<T> getPending() {
        return (Iterator<T>) client().getPending();
    }

    @Override
    public Iterator<T> getPending(String tag) {
        return (Iterator<T>) client().getPending(tag);
    }

    @Override
    public long getTaskTypeEstimate(String type) {
        try {
            return client().queryForLong(String.format("SELECT sum(duration) / count(*) from \"%s_estimates\" WHERE type = ?", tableName), type);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void addTaskTypeDuration(String type, long duration) {
        try {

            client().update(
                    String.format("INSERT INTO \"%s_estimates\" (type, duration) VALUES (?, ?)", tableName),
                    type,
                    duration
            );
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setTaskTypeEstimate(final String type, final long estimate) {
        try {

            withinTransaction(new Callable() {
                @Override
                public Object call() throws Exception {

                    client().update(
                            String.format("DELETE FROM \"%s_estimates\" WHERE type = ?", tableName),
                            type
                    );

                    client().update(
                            String.format("INSERT INTO \"%s_estimates\" (type, duration) VALUES (?, ?)", tableName),
                            type,
                            estimate
                    );

                    return null;  //To change body of implemented methods use File | Settings | File Templates.
                }
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Iterator<T> getQueued(String type) {
        return (Iterator<T>) client().getList(STATE_QUEUED, type);
    }

    @Override
    public Iterator<UUID> getQueuedIds() {
        return client().getIds(STATE_QUEUED);
    }

    @Override
    public Iterator<UUID> getQueuedIds(String type) {
        return client().getIds(STATE_QUEUED, type);
    }

    @Override
    public Iterator<T> getRunning() {
        return (Iterator<T>) client().getList(STATE_RUNNING);
    }

    @Override
    public Iterator<T> getRunning(String type) {
        return (Iterator<T>) client().getList(STATE_RUNNING, type);
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
    public long queueSize(final String type)  {
        return client().count(STATE_QUEUED, type);
    }

    @Override
    public long runningCount(final String type) {
        return client().count(STATE_RUNNING, type);
    }

    @Override
    public Set<String> getTags() throws InterruptedException {
        try {
            return client().queryStringSet(String.format("SELECT DISTINCT tag from \"%s_tags\"", tableName));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    public void withinTransaction(Callable callable) {
        try {
            client().connection.setAutoCommit(false);
            callable.call();
            client().connection.commit();
        } catch (Exception e) {
             try {
                client().connection.rollback();
                log.warn("Rolled back transaction", e);
            } catch (SQLException e1) {
                 log.debug("Failed to roll back transaction", e1);
             }
        } finally {
            try {
                client().connection.setAutoCommit(true);
            } catch (SQLException e) {
                log.error("Failed to reactivate auto commit", e);
            }
        }
    }


    @Override
    public  <U> U isolatedChange(Callable<U> callable) throws InterruptedException {
        boolean isolationStartedInThisCall = false;
        boolean transactionDone = true;
        try {
            if (!client().isolated) {
                client().connection.setAutoCommit(false);
                client().lockTable();
                client().isolated = isolationStartedInThisCall = true;
                transactionDone = false;
            }

            U result = callable.call();
            if (isolationStartedInThisCall) {
                client().connection.commit();
                transactionDone = true;
                log.trace("Committed transaction and unlocked table");
            }
            return result;
        } catch (Exception e) {
            if (isolationStartedInThisCall) {
                try {
                    client().connection.rollback();
                    transactionDone = true;
                    log.warn("Rolled back transaction", e);
                } catch (SQLException e1) {
                    log.error("Failed to roll back transaction", e);
                }
            }
            throw new RuntimeException(e);
        } finally {
            if (isolationStartedInThisCall) {
                client().isolated = false;
                if (!transactionDone) {
                    log.error("A transaction was never committed! Doing so now.");
                    try {
                        client().connection.commit();
                    } catch (SQLException e) {
                        log.error("Failed to recover transaction commit.", e);
                    }
                }

                try {
                    client().connection.setAutoCommit(true);
                } catch (SQLException e) {
                    log.error("Failed to reactivate auto commit", e);
                }
            }
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
            log.trace("Send PG notification");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void createTable() throws IOException, SQLException {
        try {
            client().execute(String.format("select 1 from \"%s\" limit 1", tableName));
            return;
        } catch (Exception ex) {
            //Do nothing - above is just a check if the table exists
        }

        withinTransaction(new Callable() {

            @Override
            public Object call() throws Exception {
                String statements = IOUtils.toString(PostgresTaskStore.class.getResource("/pgtable.sql")).replaceAll("%tableName%", tableName);
                String[] statementArr = statements.split(";");
                for(String sql : statementArr) {
                    client().update(sql);
                }
                return null;
            }
        });
    }

    public void dropTable() throws SQLException {
        withinTransaction(new Callable() {
            @Override
            public Object call() throws Exception {
                client().update(String.format("DROP TABLE \"%s_tags\"", tableName));
                client().update(String.format("DROP TABLE \"%s\"", tableName));
                return null;
            }
        });

    }

    @Override
    public void close() throws Exception {
        client().close();
    }

    public void setDocumentSerializer(DocumentSerializer documentSerializer) {
        this.documentSerializer = documentSerializer;
    }

    private class PostgresClient {
        private final Connection connection;
        private final PGConnection pgconn;
        private volatile boolean isolated = false;
        private Listener listener = new Listener();

        private PostgresClient() throws SQLException {
            connection = DriverManager.getConnection(url, username, password);
            connection.setAutoCommit(true);
            pgconn = (PGConnection) connection;
        }

        public void startListening() {
            listener.start();
            try {
                pgListen();
            } catch (SQLException e) {
                log.error("Failed to listen on postgres", e);
            }
        }

        public void close() throws SQLException {
            log.trace("Closing client connection");
            connection.close();

            if (listener.isAlive()) {
                listener.interrupt();
            }
        }

        @Override
        protected void finalize() throws Throwable {

            close();
        }

        private void lockTable() throws SQLException {
            execute(String.format("LOCK TABLE \"%s\" IN EXCLUSIVE MODE", tableName));
            log.trace("Locking table");
        }

        private <T> List<T> query(RowMapper<T> mapper, String sql, Object... args) throws SQLException {
            PreparedStatement stmt = stmt(sql, args);
            try {
                ResultSet result = stmt.executeQuery();

                List<T> out = new ArrayList();
                while (result.next()) {
                    out.add(mapper.mapRow(stmt, result));
                }

                result.close();

                return out;

            } catch (SQLException e) {
                log.debug("SQL: " + sql, e);
                throw e;
            } finally {
                stmt.close();
            }
        }

        private <T> Iterator<T> queryIterator(RowMapper<T> mapper, final String sql, final Object... args) throws SQLException {
            return new DBIterator(mapper, sql, args);
        }

        private T queryOne(String sql, Object... args) throws SQLException {
            List<T> rows = queryAll(sql, args);
            if (rows.isEmpty()) {
                return null;
            }
            return rows.get(0);
        }

        private long queryForLong(String sql, Object... args) throws SQLException {
            PreparedStatement stmt = stmt(sql, args);
            try {

                ResultSet result = stmt.executeQuery();
                if (result.next()) {
                    long out = result.getLong(1);
                    result.close();
                    return out;
                }
                return 0;
            } finally {
                stmt.close();
            }
        }

        private List<T> queryAll(String sql, Object... args) throws SQLException {
            return query(TASK_ROW_MAPPER, sql, args);
        }


        private void execute(String sql, Object... args) throws SQLException {
            PreparedStatement stmt = stmt(sql, args);
            try {
                stmt.execute();
            } catch (SQLException e) {
                log.debug("SQL: " + sql, e);
                throw e;
            } finally {
                stmt.close();
            }
        }

        private void update(String sql, Object... args) throws SQLException {
            PreparedStatement stmt = stmt(sql, args);
            try {
                stmt.executeUpdate();
            } catch (SQLException e) {
                log.debug("SQL: " + sql, e);
                throw e;
            } finally {
                stmt.close();
            }
        }

        private PreparedStatement stmt(String sql, Object... args) {
            try {
                PreparedStatement stmt = connection.prepareStatement(sql);
                int i = 1;
                for (Object arg : args) {
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
                            .queryIterator(TASK_ROW_MAPPER,
                                    String.format("SELECT task.id, task.content " +
                                            "FROM \"%1$s\" task, \"%1$s_tags\" tag " +
                                            "WHERE tag.id = task.id AND task.state = ? AND tag.tag = ? " +
                                            "GROUP BY task.id " +
                                            "ORDER BY task.priority DESC, task.created DESC, task.order ASC ", tableName), state, type
                            );
                } else {
                    return client()
                            .queryIterator(TASK_ROW_MAPPER,
                                    String.format("SELECT task.id, task.content " +
                                            "FROM \"%s\" task, \"%1$s_tags\" tag " +
                                            "WHERE task.state = ? " +
                                            "GROUP BY task.id " +
                                            "ORDER BY task.priority DESC, task.created ASC, task.order ASC ", tableName), state
                            );
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        public Iterator<T> getPending() {
            return getPending(null);
        }

        public Iterator<T> getPending(String tag) {
            try {
                if (tag != null && !tag.isEmpty()) {
                    return client()
                            .queryIterator(TASK_ROW_MAPPER,
                                    String.format("SELECT task.id, task.content " +
                                            "FROM \"%s\" task, \"%1$s_tags\" tag " +
                                            "WHERE state IN (?,?) AND tag.id = task.id and tag.tag = ? " +
                                            "GROUP BY task.id " +
                                            "ORDER BY task.state DESC, task.priority DESC, task.created DESC, task.order ASC ", tableName),
                                    STATE_RUNNING,
                                    STATE_QUEUED,
                                    tag
                            );
                } else {
                    return client()
                            .queryIterator(TASK_ROW_MAPPER,
                                    String.format("SELECT task.id, task.content " +
                                            "FROM \"%s\" task " +
                                            "WHERE task.state IN (?,?) " +
                                            "GROUP BY task.id " +
                                            "ORDER BY task.state DESC, task.priority DESC, task.created DESC, task.order ASC ", tableName),
                                    STATE_RUNNING,
                                    STATE_QUEUED
                            );
                }

            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        public long count(int state) {
            return count(state, null);
        }

        public long count(int state, String tag) {


            try {
                if (tag != null && !tag.isEmpty()) {
                    return client().queryForLong(
                            String.format("SELECT count(task.*) as count FROM \"%1$s\" task, \"%1$s_tags\" tag " +
                                    "WHERE tag.id = task.id AND task.state = ? AND tag.tag = ? ", tableName), state, tag);
                } else {
                    return client().queryForLong(
                            String.format("SELECT count(*) as count FROM \"%s\" WHERE state = ?", tableName), state);
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        public long countType(int state) {
            return countType(state, null);
        }

        public long countType(int state, String type) {

            try {
                if (type != null && !type.isEmpty()) {
                    return client().queryForLong(String.format("SELECT count(*)  FROM \"%1$s\" task, \"%1$s_tags\" tag " +
                            "WHERE task.state = ? AND task.type = ? ", tableName),
                            state, type);
                } else {
                    return client().queryForLong(String.format("SELECT count(*) from \"%s\" WHERE state = ?", tableName), state);
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }


        private CountMap<String> queryForCountMap(String sql, Object... args) throws SQLException {
            PreparedStatement stmt = stmt(sql, args);
            try {
                ResultSet result = stmt.executeQuery();
                CountMap<String> out = new CountMap<String>();
                while (result.next()) {
                    out.increment(result.getString(1), result.getInt(2));
                }
                return out;
            } finally {
                stmt.close();
            }
        }


        public Set<String> queryStringSet(String sql, Object... args) throws SQLException {
            PreparedStatement stmt = stmt(sql, args);
            try {
                ResultSet result = stmt.executeQuery();
                Set<String> out = new HashSet<String>();
                while (result.next()) {
                    out.add(result.getString(1));
                }
                return out;
            } finally {
                stmt.close();
            }
        }

        public void pgListen() throws SQLException {
            if (log.isTraceEnabled()) {
                log.trace(String.format("Listening on PG : \"%s_event\"", tableName));
            }
            execute(String.format("LISTEN \"%s_event\"", tableName));
        }

        public void pgNotify() throws SQLException {
            if (log.isTraceEnabled()) {
                log.trace(String.format("Notifying on PG : \"%s_event\"", tableName));
            }

            execute(String.format("NOTIFY \"%s_event\"", tableName));
        }

        public boolean hasNotifications() throws SQLException {
            execute("SELECT 1"); //Trigger notification push
            PGNotification[] notifications = pgconn.getNotifications();
            return notifications != null && notifications.length > 0;
        }


        public Iterator<UUID> getIds(int state) {
            return getIds(state, null);
        }

        public Iterator<UUID> getIds(int state, String type) {
            try {
                if (type != null && !type.isEmpty()) {
                    return client()
                            .queryIterator(UUID_ROW_MAPPER,
                                    String.format("SELECT task.id " +
                                            "FROM \"%1$s\" task, \"%1$s_tags\" tag " +
                                            "WHERE tag.id = task.id AND task.state = ? AND tag.tag = ? " +
                                            "GROUP BY task.id " +
                                            "ORDER BY task.priority DESC, task.created DESC, task.order ASC ", tableName), state, type
                            );
                } else {
                    return client()
                            .queryIterator(UUID_ROW_MAPPER,
                                    String.format("SELECT task.id " +
                                            "FROM \"%s\" task " +
                                            "WHERE task.state = ? " +
                                            "ORDER BY task.priority DESC, task.created ASC, task.order ASC ", tableName), state
                            );
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }


        public class DBIterator<T> implements Iterator<T> {

            private final String sql;
            private final Object[] args;
            private final RowMapper<T> rowMapper;
            private int offset = 0;
            private int lastOffset = -1;
            private T nextValue = null;

            public DBIterator(RowMapper<T> rowMapper, String sql, Object ... args) {
                this.rowMapper = rowMapper;
                this.sql = sql;
                this.args = args;
            }

            @Override
            public boolean hasNext() {
                if (lastOffset == offset) {
                    return nextValue != null;
                }

                long timeStart = System.currentTimeMillis();
                PreparedStatement stmt = stmt(String.format("%s OFFSET %s LIMIT 1", sql, offset), args);
                lastOffset = offset;
                nextValue = null;
                try {
                    ResultSet result = stmt.executeQuery();
                    if (result.next()) {
                        nextValue = rowMapper.mapRow(stmt,result);
                        return true;
                    }
                    result.close();
                } catch (SQLException e) {
                    log.warn("Failed to execute query", e);
                } finally {
                    long timeTaken = System.currentTimeMillis() - timeStart;
                    if (log.isDebugEnabled() && timeTaken > 200) {
                        log.debug(String.format("Query finished in %s ms: %s", timeTaken, sql));
                    }
                    try {
                        stmt.close();
                    } catch (SQLException e) {}
                }
                return false;
            }

            @Override
            public T next() {
                offset++;
                return nextValue;
            }

            @Override
            public void remove() {
                throw new RuntimeException("Method not implemented");
            }
        }
    }

    private static interface RowMapper<T> {

        T mapRow(PreparedStatement stmt, ResultSet result) throws SQLException;
    }

    private final RowMapper<UUID> UUID_ROW_MAPPER = new RowMapper<UUID>() {
        @Override
        public UUID mapRow(PreparedStatement stmt, ResultSet result) throws SQLException {
            return (UUID) result.getObject(1);
        }
    };

    private final RowMapper<T> TASK_ROW_MAPPER = new RowMapper<T>() {
        @Override
        public T mapRow(PreparedStatement stmt, ResultSet result) throws SQLException {
            UUID id = (UUID) result.getObject("id");
            try {
                String contents = IOUtils.toString(result.getBinaryStream("content"), "UTF-8");
                return documentSerializer.deserialize(contents, taskClass);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    };

    private class Listener extends Thread {
        private Listener() {
            super("pg-queue-listener");

        }

        @Override
        public void run() {

            final PostgresClient client;

            try {
                client = new PostgresClient();
                client.pgListen();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }

            while (true) {
                try {
                    if (client.hasNotifications()) {
                        log.trace("PG returned notifications");
                        synchronized (PostgresTaskStore.this) {
                            PostgresTaskStore.this.notifyAll();
                        }
                    }
                } catch (SQLException e) {
                    synchronized (PostgresTaskStore.this) {
                        PostgresTaskStore.this.notifyAll();
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
