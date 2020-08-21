package com.iamabug;

import com.alibaba.druid.pool.DruidDataSource;
import kafka.common.KafkaException;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.plain.PlainAuthenticateCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.login.AppConfigurationEntry;
import java.sql.*;
import java.util.List;
import java.util.Map;

public class MySQLAuthCallback implements AuthenticateCallbackHandler {
    private static final Logger log = LoggerFactory.getLogger("plugin");
    private DruidDataSource dataSource = null;

    public MySQLAuthCallback() {
        this.dataSource = new DruidDataSource();
        this.dataSource.setDriverClassName("com.mysql.jdbc.Driver");
        this.dataSource.setUrl("jdbc:mysql://localhost:3306/kafka_user_info");
        this.dataSource.setUsername("username");
        this.dataSource.setPassword("password");
        this.dataSource.setInitialSize(5);
        this.dataSource.setMinIdle(1);
        this.dataSource.setMaxActive(10);
        this.dataSource.setPoolPreparedStatements(false);
    }

    public void configure(Map<String, ?> configs, String mechanism, List<AppConfigurationEntry> jaasConfigEntries) {
    }

    public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
        String username = null;
        for (Callback callback: callbacks) {
            if (callback instanceof NameCallback) {
                username = ((NameCallback) callback).getDefaultName();
            }
            else if (callback instanceof PlainAuthenticateCallback) {
                PlainAuthenticateCallback plainCallback = (PlainAuthenticateCallback) callback;
                boolean authenticated = authenticate(username, plainCallback.password());
                plainCallback.authenticated(authenticated);
            } else
                throw new UnsupportedCallbackException(callback);
        }
    }

    protected boolean authenticate(String username, char[] password) {
        return userExists(username, new String(password));
    }

    public boolean userExists(String username, String password) {
        Connection conn = null;
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        try {
            conn = getConnection();
            statement = conn.prepareStatement("select * from users where username=? and password=?");
            statement.setString(1, username);
            statement.setString(2, password);
            resultSet = statement.executeQuery();
            while (resultSet.next()) {
                boolean b = username.equals(resultSet.getString("username")) &&
                        password.equals(resultSet.getString("password"));
                log.info("user {} (password: {}) authentication status: {}.", username, password, b);
                return b;
            }
        } catch (SQLException e) {
            log.info("sql exception occurred: {}", e.getMessage());

        } finally {
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (SQLException sqlEx) {
                }
                resultSet = null;
            }

            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException sqlEx) {
                }
                statement = null;
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                }
                conn = null;
            }
        }
        return false;

    }

    private Connection getConnection() throws SQLException {
        Connection conn = null;
        try {
            synchronized (dataSource) {
                conn = dataSource.getConnection();
            }
        } catch (SQLException e) {
            throw e;
        }
        return conn;
    }

    public void close() throws KafkaException {
    }

}
