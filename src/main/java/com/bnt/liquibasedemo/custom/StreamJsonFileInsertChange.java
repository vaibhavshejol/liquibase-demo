package com.bnt.liquibasedemo.custom;

import liquibase.change.custom.CustomTaskChange;
import liquibase.database.Database;
import liquibase.exception.CustomChangeException;
import liquibase.exception.ValidationErrors;
import liquibase.resource.Resource;
import liquibase.resource.ResourceAccessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.Connection;
import java.util.Scanner;

public class StreamJsonFileInsertChange implements CustomTaskChange {
    private String tableName;
    private String idColumn;
    private String idValue;
    private String employeeIdColumn;
    private String employeeIdValue;
    private String jsonColumn;
    private String primarySkill;
    private String secondarySkill;
    private String jsonFilePath;
    private ResourceAccessor resourceAccessor;
    private final Logger logger = LoggerFactory.getLogger(StreamJsonFileInsertChange.class);

    public void setTableName(String tableName) { this.tableName = tableName; }
    public void setIdColumn(String idColumn) { this.idColumn = idColumn; }
    public void setIdValue(String idValue) { this.idValue = idValue; }
    public void setJsonColumn(String jsonColumn) { this.jsonColumn = jsonColumn; }
    public void setPrimarySkill(String primarySkill) { this.primarySkill = primarySkill; }
    public void setSecondarySkill(String secondarySkill) { this.secondarySkill = secondarySkill; }
    public void setJsonFilePath(String jsonFilePath) { this.jsonFilePath = jsonFilePath; }
    public void setEmployeeIdColumn(String employeeIdColumn) { this.employeeIdColumn = employeeIdColumn; }
    public void setEmployeeIdValue(String employeeIdValue) { this.employeeIdValue = employeeIdValue; }

    @Override
    public void execute(Database database) throws CustomChangeException {
        try {
            Connection connection = ((liquibase.database.jvm.JdbcConnection) database.getConnection()).getUnderlyingConnection();
            String sql = String.format(
                    "INSERT INTO %s (%s, primary_skill, secondary_skill, %s, %s) VALUES (?, ?, ?, CAST(? AS JSON),?)",
                    tableName, idColumn, jsonColumn, employeeIdColumn
            );
            PreparedStatement ps = connection.prepareStatement(sql);

            Resource resource = resourceAccessor.get(jsonFilePath);
            if (resource == null || !resource.exists()) {
                throw new CustomChangeException("JSON file " + jsonFilePath + " not found");
            }

            InputStream jsonInputStream = resource.openInputStream();

            if (jsonInputStream == null) {
                throw new CustomChangeException("JSON file " + jsonFilePath + " not found");
            }
            String jsonContent = streamToString(jsonInputStream);

            if (jsonContent == null || jsonContent.trim().isEmpty()) {
                throw new CustomChangeException("JSON file is empty: " + jsonFilePath);
            }

            ps.setString(1, idValue);
            ps.setString(2, primarySkill);
            ps.setString(3, secondarySkill);
            ps.setString(4, jsonContent);
            ps.setString(5, employeeIdValue);
            ps.executeUpdate();

            logger.info("Inserted JSON content from file: {}", jsonFilePath);
            ps.close();
            jsonInputStream.close();

        } catch (Exception e) {
            throw new CustomChangeException("Error inserting JSON from file: "+e.getMessage());
        }
    }

    private String streamToString(InputStream inputStream) {
        try (Scanner scanner = new Scanner(inputStream, StandardCharsets.UTF_8)) {
            scanner.useDelimiter("\\A");
            return scanner.hasNext() ? scanner.next() : "";
        }
    }

    @Override
    public String getConfirmationMessage() {
        return "Inserted JSON from file " + jsonFilePath + " into table " + tableName;
    }

    @Override
    public void setUp() { /* no setup required */ }
    @Override
    public void setFileOpener(ResourceAccessor resourceAccessor) { this.resourceAccessor = resourceAccessor; }
    @Override
    public ValidationErrors validate(Database database) {
        ValidationErrors errors = new ValidationErrors();
        if (tableName == null) errors.addError("tableName is required");
        if (idColumn == null) errors.addError("idColumn is required");
        if (idValue == null) errors.addError("idValue is required");
        if (primarySkill == null) errors.addError("primarySkill is required");
        if (secondarySkill == null) errors.addError("secondarySkill is required");
        if (jsonColumn == null) errors.addError("jsonColumn is required");
        if (jsonFilePath == null) errors.addError("jsonFilePath is required");
        if (employeeIdColumn == null) errors.addError("employeeIdColumn is required");
        if (employeeIdValue == null) errors.addError("employeeIdValue is required");
        return errors;
    }
}

