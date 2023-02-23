/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.snowflake;

import static io.airbyte.db.jdbc.JdbcConstants.INTERNAL_COLUMN_NAME;
import static io.airbyte.db.jdbc.JdbcConstants.INTERNAL_COLUMN_TYPE;
import static io.airbyte.db.jdbc.JdbcConstants.INTERNAL_COLUMN_TYPE_NAME;
import static io.airbyte.db.jdbc.JdbcConstants.INTERNAL_SCHEMA_NAME;
import static io.airbyte.db.jdbc.JdbcConstants.INTERNAL_TABLE_NAME;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airbyte.db.jdbc.DateTimeConverter;
import io.airbyte.db.jdbc.JdbcSourceOperations;
import io.airbyte.protocol.models.JsonSchemaType;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.format.DateTimeParseException;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnowflakeSourceOperations extends JdbcSourceOperations {

  private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeSourceOperations.class);
  // HACK: See https://linear.app/calixa/issue/CAL-2656/happy-scribe-snowflake-sync-failing
  // and https://github.com/airbytehq/airbyte/issues/23103
  private static final Set<JDBCType> EXTRA_ALLOWED_CURSOR_TYPES = Set.of(JDBCType.TIME_WITH_TIMEZONE, JDBCType.TIMESTAMP_WITH_TIMEZONE);

  @Override
  protected void putDouble(final ObjectNode node, final String columnName, final ResultSet resultSet, final int index) {
    try {
      final double value = resultSet.getDouble(index);
      node.put(columnName, value);
    } catch (final SQLException e) {
      node.put(columnName, (Double) null);
    }
  }

  @Override
  public JDBCType getDatabaseFieldType(final JsonNode field) {
    try {
      final String typeName = field.get(INTERNAL_COLUMN_TYPE_NAME).asText().toLowerCase();
      return "TIMESTAMPLTZ".equalsIgnoreCase(typeName)
          ? JDBCType.TIMESTAMP_WITH_TIMEZONE
          : JDBCType.valueOf(field.get(INTERNAL_COLUMN_TYPE).asInt());
    } catch (final IllegalArgumentException ex) {
      LOGGER.warn(String.format("Could not convert column: %s from table: %s.%s with type: %s. Casting to VARCHAR.",
          field.get(INTERNAL_COLUMN_NAME),
          field.get(INTERNAL_SCHEMA_NAME),
          field.get(INTERNAL_TABLE_NAME),
          field.get(INTERNAL_COLUMN_TYPE)));
      return JDBCType.VARCHAR;
    }
  }

  @Override
  protected void putBigInt(final ObjectNode node, final String columnName, final ResultSet resultSet, final int index) {
    try {
      final var value = resultSet.getBigDecimal(index);
      node.put(columnName, value);
    } catch (final SQLException e) {
      node.put(columnName, (BigDecimal) null);
    }
  }

  @Override
  protected void setTimestamp(final PreparedStatement preparedStatement, final int parameterIndex, final String value) throws SQLException {
    preparedStatement.setString(parameterIndex, value);
  }

  @Override
  public JsonSchemaType getAirbyteType(final JDBCType jdbcType) {
    return switch (jdbcType) {
      case BIT, BOOLEAN -> JsonSchemaType.BOOLEAN;
      case REAL, FLOAT, DOUBLE, NUMERIC, DECIMAL -> JsonSchemaType.NUMBER;
      case TINYINT, SMALLINT, INTEGER, BIGINT -> JsonSchemaType.INTEGER;
      case CHAR, NCHAR, NVARCHAR, VARCHAR, LONGVARCHAR -> JsonSchemaType.STRING;
      case DATE -> JsonSchemaType.STRING_DATE;
      case TIME -> JsonSchemaType.STRING_TIME_WITHOUT_TIMEZONE;
      case TIMESTAMP -> JsonSchemaType.STRING_TIMESTAMP_WITHOUT_TIMEZONE;
      case TIMESTAMP_WITH_TIMEZONE -> JsonSchemaType.STRING_TIMESTAMP_WITH_TIMEZONE;
      case TIME_WITH_TIMEZONE -> JsonSchemaType.STRING_TIME_WITH_TIMEZONE;
      case BLOB, BINARY, VARBINARY, LONGVARBINARY -> JsonSchemaType.STRING_BASE_64;
      case ARRAY -> JsonSchemaType.ARRAY;
      // since column types aren't necessarily meaningful to Airbyte, liberally convert all unrecgonised
      // types to String
      default -> JsonSchemaType.STRING;
    };
  }

  @Override
  public void copyToJsonField(final ResultSet resultSet, final int colIndex, final ObjectNode json) throws SQLException {
    final String columnName = resultSet.getMetaData().getColumnName(colIndex);
    final String columnTypeName = resultSet.getMetaData().getColumnTypeName(colIndex).toLowerCase();

    // TIMESTAMPLTZ data type detected as JDBCType.TIMESTAMP which is not correct
    if ("TIMESTAMPLTZ".equalsIgnoreCase(columnTypeName)) {
      putTimestampWithTimezone(json, columnName, resultSet, colIndex);
      return;
    }
    super.copyToJsonField(resultSet, colIndex, json);
  }

  @Override
  protected void setDate(final PreparedStatement preparedStatement, final int parameterIndex, final String value) throws SQLException {
    final LocalDate date = LocalDate.parse(value);
    preparedStatement.setDate(parameterIndex, Date.valueOf(date));
  }

  @Override
  protected void putTimestampWithTimezone(final ObjectNode node, final String columnName, final ResultSet resultSet, final int index)
      throws SQLException {
    final Timestamp timestamp = resultSet.getTimestamp(index);
    node.put(columnName, DateTimeConverter.convertToTimestampWithTimezone(timestamp));
  }

  @Override
  protected void putTimestamp(final ObjectNode node, final String columnName, final ResultSet resultSet, final int index) throws SQLException {
    final Timestamp timestamp = resultSet.getTimestamp(index);
    node.put(columnName, DateTimeConverter.convertToTimestamp(timestamp));
  }

  @Override
  protected void putDate(final ObjectNode node, final String columnName, final ResultSet resultSet, final int index) throws SQLException {
    final Date date = resultSet.getDate(index);
    node.put(columnName, DateTimeConverter.convertToDate(date));
  }

  @Override
  protected void putTime(final ObjectNode node, final String columnName, final ResultSet resultSet, final int index) throws SQLException {
    // resultSet.getTime() will lose nanoseconds precision
    final LocalTime localTime = resultSet.getTimestamp(index).toLocalDateTime().toLocalTime();
    node.put(columnName, DateTimeConverter.convertToTime(localTime));
  }

  @Override
  public boolean isCursorType(JDBCType type) {
    return super.isCursorType(type) || EXTRA_ALLOWED_CURSOR_TYPES.contains(type);
  }

  @Override
  public void setCursorField(PreparedStatement preparedStatement, int parameterIndex,
      JDBCType cursorFieldType, String value) throws SQLException {
    switch (cursorFieldType) {
      case TIME_WITH_TIMEZONE -> setTimeWithTimezone(preparedStatement, parameterIndex, value);
      case TIMESTAMP_WITH_TIMEZONE -> setTimestampWithTimezone(preparedStatement, parameterIndex, value);
      default -> super.setCursorField(preparedStatement, parameterIndex, cursorFieldType, value);
    }
  }

  // HACK: copied from io.airbyte.integrations.source.postgres.PostgresSourceOperations.setTimeWithTimezone
  private void setTimeWithTimezone(final PreparedStatement preparedStatement, final int parameterIndex, final String value) throws SQLException {
    try {
      preparedStatement.setObject(parameterIndex, OffsetTime.parse(value));
    } catch (final DateTimeParseException e) {
      // attempt to parse the time w/o timezone. This can be caused by schema created with a different
      // version of the connector
      preparedStatement.setObject(parameterIndex, LocalTime.parse(value));
    }
  }

  // HACK: copied from io.airbyte.integrations.source.postgres.PostgresSourceOperations.setTimestampWithTimezone
  private void setTimestampWithTimezone(final PreparedStatement preparedStatement, final int parameterIndex, final String value) throws SQLException {
    try {
      preparedStatement.setObject(parameterIndex, OffsetDateTime.parse(value));
    } catch (final DateTimeParseException e) {
      // attempt to parse the datetime w/o timezone. This can be caused by schema created with a different
      // version of the connector
      preparedStatement.setObject(parameterIndex, LocalDateTime.parse(value));
    }
  }
}
