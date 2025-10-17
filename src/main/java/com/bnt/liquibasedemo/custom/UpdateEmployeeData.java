package com.bnt.liquibasedemo.custom;

import liquibase.change.custom.CustomTaskChange;
import liquibase.database.Database;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.CustomChangeException;
import liquibase.exception.SetupException;
import liquibase.exception.ValidationErrors;
import liquibase.resource.Resource;
import liquibase.resource.ResourceAccessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Scanner;

public class UpdateEmployeeData implements CustomTaskChange {

    private static final Logger logger = LoggerFactory.getLogger(LoadEmployeeData.class);

    private String id;
    private String firstname;
    private String lastname;
    private String email;
    private String skills;
    private String operationType;

    private ResourceAccessor resourceAccessor;

    public void setOperationType(String operationType) {this.operationType = operationType;}
    public void setFirstname(String firstname) {this.firstname = firstname;}
    public void setLastname(String lastname) {this.lastname = lastname;}
    public void setEmail(String email) {this.email = email;}
    public void setSkills(String skills) {this.skills = skills;}
    public void setId(String id) {this.id = id;}

    @Override
    public void execute(Database database) throws CustomChangeException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;

        try {
            connection = getJdbcConnection(database);
            String sql = getSql();
            preparedStatement = connection.prepareStatement(sql);

            String fileContent = readFileContent();
            setStatementParameters(preparedStatement, fileContent);

            int rowsAffected = preparedStatement.executeUpdate();
            logger.info("Successfully executed {} row(s) with JSON content from file: {}",
                    rowsAffected, skills);

        } catch (SQLException e) {
            throw new CustomChangeException("Failed to execute SQL insert: " + e.getMessage(), e);
        } catch (IOException e) {
            throw new CustomChangeException("Failed to read JSON file: " + e.getMessage(), e);
        } finally {
            closeResources(preparedStatement);
        }
    }

    private String getSql() {
        if ("update".equalsIgnoreCase(operationType)) {
            return "UPDATE employees SET firstname = ?, lastname = ?, email = ?, skills = CAST(? AS JSON) WHERE id = ?";
        } else {
            return "INSERT INTO employees (id, firstname, lastname, email, skills) VALUES (?, ?, ?, ?, CAST(? AS JSON))";
        }
    }

    private String readFileContent() throws IOException, CustomChangeException {
        Resource resource = resourceAccessor.get(skills);

        if (resource == null || !resource.exists()) {
            throw new CustomChangeException("File not found at path: " + skills);
        }

        try (InputStream inputStream = resource.openInputStream()) {
            String content = streamToString(inputStream);

            if (content.trim().isEmpty()) {
                throw new CustomChangeException("File is empty: " + skills);
            }

            return content;
        }
    }

    private void setStatementParameters(PreparedStatement ps, String fileContent) throws SQLException {
        if ("update".equalsIgnoreCase(operationType)) {
            ps.setString(1, firstname);
            ps.setString(2, lastname);
            ps.setString(3, email);
            ps.setString(4, fileContent);
            ps.setString(5, id);
        } else {
            ps.setString(1, id);
            ps.setString(2, firstname);
            ps.setString(3, lastname);
            ps.setString(4, email);
            ps.setString(5, fileContent);
        }
    }

    private Connection getJdbcConnection(Database database) throws CustomChangeException {
        try {
            JdbcConnection jdbcConnection = (JdbcConnection) database.getConnection();
            return jdbcConnection.getUnderlyingConnection();
        } catch (ClassCastException e) {
            throw new CustomChangeException("Database connection is not a JDBC connection", e);
        }
    }

    private void closeResources(PreparedStatement ps) {
        if (ps != null) {
            try {
                ps.close();
            } catch (SQLException e) {
                logger.warn("Failed to close PreparedStatement: {}", e.getMessage());
            }
        }
    }

    @Override
    public String getConfirmationMessage() {
        return String.format("Inserted row into table employees (ID: %s)", id);
    }

    @Override
    public void setUp() throws SetupException {
        // No setup required
    }

    @Override
    public void setFileOpener(ResourceAccessor resourceAccessor) {
        this.resourceAccessor = resourceAccessor;
    }

    @Override
    public ValidationErrors validate(Database database) {
        ValidationErrors errors = new ValidationErrors();

        validateRequired(errors, "id", id);
        validateRequired(errors, "firstName", firstname);
        validateRequired(errors, "lastName", lastname);
        validateRequired(errors, "email", email);
        validateRequired(errors, "skills", skills);

        return errors;
    }

    private void validateRequired(ValidationErrors errors, String paramName, String paramValue) {
        if (paramValue == null || paramValue.trim().isEmpty()) {
            errors.addError(paramName + " is required and cannot be empty");
        }
    }

    private String streamToString(InputStream inputStream) {
        try (Scanner scanner = new Scanner(inputStream, StandardCharsets.UTF_8)) {
            scanner.useDelimiter("\\A");
            return scanner.hasNext() ? scanner.next() : "";
        }
    }
}