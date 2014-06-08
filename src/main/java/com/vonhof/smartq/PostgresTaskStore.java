package com.vonhof.smartq;


import com.vonhof.smartq.Task.State;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.postgresql.copy.CopyIn;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.*;
import java.util.*;
import java.util.concurrent.Callable;

public class PostgresTaskStore implements TaskStore {

    private static final Logger log = Logger.getLogger(PostgresTaskStore.class);

    private final int STATE_QUEUED = 1;
    private final int STATE_RUNNING = 2;
    private final int STATE_ERROR = 3;

    private final String url;  //jdbc:postgresql://host:port/database
    private final String username;
    private final String password;
    private String tableName = "queue";
    private volatile boolean closed = false;

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

    private final Class<Task> taskClass;

    private DocumentSerializer documentSerializer = new JacksonDocumentSerializer();


    public PostgresTaskStore(Class<Task> taskClass) throws SQLException {
        this(taskClass, "jdbc:postgresql://localhost/smartq", "henrik", "");
    }

    public PostgresTaskStore(Class<Task> taskClass, String url, String username, String password) throws SQLException {
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
    public Task get(UUID id) {
        try {
            return client().queryOne(String.format("SELECT * FROM \"%s\" WHERE id = ?", tableName), id);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void remove(Task task) {
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
    public void queue(final Task... tasks) {
        try {
            withinTransaction(new Callable() {
                @Override
                public Object call() throws Exception {
                    CopyManager taskCopy = new CopyManager((BaseConnection) client().connection);
                    CopyIn taskCopyIn = taskCopy.copyIn(String.format("COPY \"%s\"(id, content, state, priority, type, referenceid, created) FROM STDIN WITH DELIMITER '|'", tableName));

                    for (Task task : tasks) {
                        task = new Task(task);//Copy
                        task.setState(State.PENDING);

                        StringBuilder taskRowBuilder = new StringBuilder();
                        taskRowBuilder.append(task.getId());
                        taskRowBuilder.append("|");
                        taskRowBuilder.append(documentSerializer.serialize(task));
                        taskRowBuilder.append("|");
                        taskRowBuilder.append(STATE_QUEUED);
                        taskRowBuilder.append("|");
                        taskRowBuilder.append(task.getPriority());
                        taskRowBuilder.append("|");
                        taskRowBuilder.append(task.getType());
                        taskRowBuilder.append("|");
                        taskRowBuilder.append(task.getReferenceId());
                        taskRowBuilder.append("|");
                        taskRowBuilder.append(task.getCreated());
                        taskRowBuilder.append("\n");

                        byte[] taskRow = taskRowBuilder.toString().getBytes(Charset.forName("UTF-8"));
                        taskCopyIn.writeToCopy(taskRow, 0, taskRow.length);
                    }

                    taskCopyIn.endCopy();

                    CopyManager tagCopy = new CopyManager((BaseConnection) client().connection);
                    CopyIn tagCopyIn = tagCopy.copyIn(String.format("COPY \"%s_tags\"(id, tag) FROM STDIN WITH DELIMITER '|'", tableName));

                    for (Task task : tasks) {
                        for (String tag : (Set<String>) task.getTags().keySet()) {
                            StringBuilder tagRowBuilder = new StringBuilder();
                            tagRowBuilder.append(task.getId());
                            tagRowBuilder.append("|");
                            tagRowBuilder.append(tag);
                            tagRowBuilder.append("\n");
                            byte[] tagRow = tagRowBuilder.toString().getBytes(Charset.forName("UTF-8"));

                            tagCopyIn.writeToCopy(tagRow, 0, tagRow.length);
                        }
                    }
                    tagCopyIn.endCopy();

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
    public void run(Task task) {
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
    public void failed(Task task) {
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
    public Iterator<Task> getFailed() {
        return client().getList(STATE_ERROR);
    }

    @Override
    public Iterator<Task> getQueued() {
        return client().getList(STATE_QUEUED);
    }

    @Override
    public ParallelIterator<Task> getPending() {
        return client().getPending();
    }

    @Override
    public ParallelIterator<Task> getPending(String tag) {
        return client().getPending(tag);
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
    public Iterator<Task> getQueued(String type) {
        return (Iterator<Task>) client().getList(STATE_QUEUED, type);
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
    public Iterator<Task> getRunning() {
        return (Iterator<Task>) client().getList(STATE_RUNNING);
    }

    @Override
    public Iterator<Task> getRunning(String type) {
        return (Iterator<Task>) client().getList(STATE_RUNNING, type);
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
    public long queueSize(final String type) {
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
                if (e instanceof BatchUpdateException) {
                    e = ((BatchUpdateException) e).getNextException();
                }
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
    public <U> U isolatedChange(Callable<U> callable) throws InterruptedException {
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
                for (String sql : statementArr) {
                    client().update(sql);
                }
                return null;
            }
        });
    }

    public synchronized void dropTable() throws SQLException {
        client().update(String.format("DROP TABLE \"%s_tags\"", tableName));
        client().update(String.format("DROP TABLE \"%s\"", tableName));
    }

    @Override
    public void close() throws Exception {
        closed = true;
        client().close();
    }

    @Override
    public Task getFirstTaskWithReference(String referenceId) {
        try {
            return client().queryOne(String.format("SELECT task.id, task.content " +
                    "FROM \"%1$s\" task " +
                    "WHERE task.referenceid = ? " +
                    "ORDER BY task.priority DESC, task.created ASC, task.order ASC ", tableName),
                    referenceId);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Task getLastTaskWithReference(String referenceId) {
        try {
            return client().queryOne(String.format("SELECT task.id, task.content " +
                    "FROM \"%1$s\" task " +
                    "WHERE task.referenceid = ? " +
                    "ORDER BY task.priority ASC, task.created DESC, task.order DESC", tableName),
                    referenceId);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void setDocumentSerializer(DocumentSerializer documentSerializer) {
        this.documentSerializer = documentSerializer;
    }

    @Override
    protected void finalize() throws Throwable {
        close();
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

        public void close() throws SQLException, InterruptedException {
            log.trace("Closing client connection");
            connection.close();

            if (listener.isAlive()) {
                listener.interrupt();
                listener.join();
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

        private <Task> List<Task> query(RowMapper<Task> mapper, String sql, Object... args) throws SQLException {
            PreparedStatement stmt = stmt(sql, args);
            try {
                ResultSet result = stmt.executeQuery();

                List<Task> out = new ArrayList();
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

        private <Task> DBIterator<Task> queryIterator(RowMapper<Task> mapper, final String countSql, final String sql, final Object... args) throws SQLException {
            return new DBIterator(mapper, countSql, sql, args);
        }

        private Task queryOne(String sql, Object... args) throws SQLException {
            List<Task> rows = queryAll(sql, args);
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

        private List<Task> queryAll(String sql, Object... args) throws SQLException {
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

        private PreparedStatement prepared(String sql) {
            try {
                return connection.prepareStatement(sql);
            } catch (SQLException e) {
                throw new RuntimeException(sql, e);
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

        public DBIterator<Task> getList(int state) {
            return getList(state, null);
        }

        public DBIterator<Task> getList(int state, String type) {
            try {
                if (type != null && !type.isEmpty()) {
                    return client()
                            .queryIterator(TASK_ROW_MAPPER,
                                    String.format("SELECT COUNT(DISTINCT task.id) " +
                                            "FROM \"%1$s\" task, \"%1$s_tags\" tag " +
                                            "WHERE tag.id = task.id AND task.state = ? AND tag.tag = ? ", tableName),
                                    String.format("SELECT task.id, task.content " +
                                            "FROM \"%1$s\" task, \"%1$s_tags\" tag " +
                                            "WHERE tag.id = task.id AND task.state = ? AND tag.tag = ? " +
                                            "GROUP BY task.id " +
                                            "ORDER BY task.priority DESC, task.created ASC, task.order ASC ", tableName), state, type
                            );
                } else {
                    return client()
                            .queryIterator(TASK_ROW_MAPPER,
                                    String.format("SELECT count(*) " +
                                            "FROM \"%s\" task " +
                                            "WHERE task.state = ? ", tableName),
                                    String.format("SELECT task.id, task.content " +
                                            "FROM \"%s\" task " +
                                            "WHERE task.state = ? " +
                                            "ORDER BY task.priority DESC, task.created ASC, task.order DESC ", tableName), state
                            );
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        public DBIterator<Task> getPending() {
            return getPending(null);
        }

        public DBIterator<Task> getPending(String tag) {
            try {
                if (tag != null && !tag.isEmpty()) {
                    return client()
                            .queryIterator(TASK_ROW_MAPPER,
                                    String.format("SELECT count(distinct task.id) " +
                                            "FROM \"%s\" task, \"%1$s_tags\" tag " +
                                            "WHERE state IN (?,?) AND tag.id = task.id and tag.tag = ? ", tableName),
                                    String.format("SELECT task.id, task.content " +
                                            "FROM \"%s\" task, \"%1$s_tags\" tag " +
                                            "WHERE state IN (?,?) AND tag.id = task.id and tag.tag = ? " +
                                            "GROUP BY task.id " +
                                            "ORDER BY task.state DESC, task.priority DESC, task.created ASC, task.order ASC ", tableName),
                                    STATE_RUNNING,
                                    STATE_QUEUED,
                                    tag
                            );
                } else {
                    return client()
                            .queryIterator(TASK_ROW_MAPPER,
                                    String.format("SELECT count(*) " +
                                            "FROM \"%s\" task " +
                                            "WHERE task.state IN (?,?) ", tableName),
                                    String.format("SELECT task.id, task.content " +
                                            "FROM \"%s\" task " +
                                            "WHERE task.state IN (?,?) " +
                                            "ORDER BY task.state DESC, task.priority DESC, task.created ASC, task.order ASC ", tableName),
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


        public DBIterator<UUID> getIds(int state) {
            return getIds(state, null);
        }

        public DBIterator<UUID> getIds(int state, String type) {
            try {
                if (type != null && !type.isEmpty()) {
                    return client()
                            .queryIterator(UUID_ROW_MAPPER,
                                    String.format("SELECT count(distinct task.id) " +
                                            "FROM \"%1$s\" task, \"%1$s_tags\" tag " +
                                            "WHERE tag.id = task.id AND task.state = ? AND tag.tag = ? ", tableName),
                                    String.format("SELECT task.id " +
                                            "FROM \"%1$s\" task, \"%1$s_tags\" tag " +
                                            "WHERE tag.id = task.id AND task.state = ? AND tag.tag = ? " +
                                            "GROUP BY task.id " +
                                            "ORDER BY task.priority DESC, task.created ASC, task.order ASC ", tableName),
                                    state, type
                            );
                } else {
                    return client()
                            .queryIterator(UUID_ROW_MAPPER,
                                    String.format("SELECT count(*) " +
                                            "FROM \"%s\" task " +
                                            "WHERE task.state = ? ", tableName),
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


        public class DBIterator<Task> implements ParallelIterator<Task> {

            private final String sql;
            private final String countSql;
            private final Object[] args;
            private final RowMapper<Task> rowMapper;
            private int offset = 0;
            private int lastOffset = -1;
            private long bufferOffset = 0;
            private long bufferSize = 40000;
            private PreparedStatement stmt = null;
            private ResultSet result = null;
            private boolean lastResult = false;
            private long totalCount = -1;
            private boolean thisChunkOnly = false;

            public DBIterator(RowMapper<Task> rowMapper, String countSql, String sql, Object... args) {
                this.rowMapper = rowMapper;
                this.sql = sql;
                this.countSql = countSql;
                this.args = args;
            }

            private DBIterator(RowMapper<Task> rowMapper, String countSql, String sql, long offset, long size, Object... args) {
                this.rowMapper = rowMapper;
                this.sql = sql;
                this.args = args;
                this.bufferOffset = offset;
                this.bufferSize = size;
                this.countSql = countSql;
                this.thisChunkOnly = true;
            }

            @Override
            public long size() {
                if (totalCount < 0 && countSql != null) {
                    String sqlStr = countSql;
                    if (thisChunkOnly) {
                        sqlStr = String.format("%s OFFSET %s LIMIT %s", countSql, bufferOffset, bufferSize);
                    }
                    try {
                        totalCount = client().queryForLong(sqlStr, args);
                    } catch (SQLException e) {
                        totalCount = 0;
                        log.error("Failed to count rows", e);
                    }
                }

                return totalCount;
            }

            @Override
            public boolean canDoParallel() {
                long total = size();
                if (total < 10000) {
                    return false;
                }
                return true;
            }

            @Override
            public ParallelIterator[] getParallelIterators() {
                final long total = size();
                if (total < 10000) {
                    return new ParallelIterator[]{this};
                }
                int chunks = total > 40000 ? 4 : 2;
                long chunkSize = (long) Math.ceil((double) total / (double) chunks);

                DBIterator<Task>[] iterators = new DBIterator[chunks];
                for (int i = 0; i < chunks; i++) {
                    iterators[i] = new DBIterator<Task>(rowMapper, countSql, sql, i * chunkSize, chunkSize, args) {
                        @Override
                        public boolean canDoParallel() {
                            return false;
                        }
                    };
                }

                return iterators;
            }

            @Override
            public boolean hasNext() {
                try {

                    if (lastOffset == offset) {
                        return lastResult;
                    }

                    lastOffset = offset;

                    if (result == null || result.isAfterLast()) {


                        if (result != null && thisChunkOnly) {
                            ensureClosed();
                            return lastResult = false;
                        }

                        if (result != null && offset > 0) {
                            bufferOffset += bufferSize;
                        }

                        ensureClosed();


                        stmt = stmt(String.format("%s OFFSET %s LIMIT %s", sql, bufferOffset, bufferSize), args);

                        result = stmt.executeQuery();
                        result.setFetchSize(1);
                        lastResult = result.next();
                        if (!lastResult) {
                            ensureClosed();
                        }
                        return lastResult;
                    }

                } catch (SQLException e) {
                    log.warn("Failed to execute query", e);
                }

                return lastResult = true;
            }

            private void ensureClosed() throws SQLException {
                if (stmt != null) {
                    stmt.close();
                    stmt = null;
                }
                if (result != null) {
                    result.close();
                    result = null;
                }
            }

            @Override
            public Task next() {
                offset++;
                try {
                    Task out = rowMapper.mapRow(stmt, result);
                    result.next();
                    return out;
                } catch (SQLException e) {
                    log.error("Failed to map row", e);
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void close() {
                try {
                    ensureClosed();
                } catch (SQLException e) {
                }
            }

            @Override
            public void remove() {
                throw new RuntimeException("Method not implemented");
            }


        }
    }

    private static interface RowMapper<Task> {

        Task mapRow(PreparedStatement stmt, ResultSet result) throws SQLException;
    }

    private final RowMapper<UUID> UUID_ROW_MAPPER = new RowMapper<UUID>() {
        @Override
        public UUID mapRow(PreparedStatement stmt, ResultSet result) throws SQLException {
            return (UUID) result.getObject(1);
        }
    };

    private final RowMapper<Task> TASK_ROW_MAPPER = new RowMapper<Task>() {
        @Override
        public Task mapRow(PreparedStatement stmt, ResultSet result) throws SQLException {
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

            try {
                while (!interrupted() && !closed) {
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
                            break;
                        }
                    }
                }

            } finally {
                try {
                    client.close();
                } catch (Exception e) {
                    log.warn("Failed to shut down client", e);
                }
            }


        }
    }
}
