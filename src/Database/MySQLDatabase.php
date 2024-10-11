<?php

namespace Quill\Database;

use mysqli;
use Quill\Helpers\TypeConverter;

class MySQLDatabase implements DatabaseInterface
{
    private $connection;

    public function connect(array $config)
    {
        mysqli_report(MYSQLI_REPORT_ERROR | MYSQLI_REPORT_STRICT);
        $this->connection = new mysqli('p:' . $config['host'], $config['user'], $config['password'], $config['database'], $config['port'] ?? '3306');
        return $this->connection;
    }

    public function disconnect()
    {
        $this->connection->close();
    }

    public function query(string $query): array
    {
        try {
            $result = $this->connection->query($query);
            if ($result === false) {
                // Handle query error
                // Note: may never be reached since an exception is thrown on error
                echo "Error: " . $this->connection->error;
            } else {
                if ($result === true) {
                    // no data returned
                    return [
                        'fields' => [],
                        'rows' => [],
                    ];
                }
                // Process the result
                $fields = $result->fetch_fields();

                foreach ($fields as $field) {
                    $mappedFields[] = [
                        'name' => $field->name,
                        'dataTypeID' => TypeConverter::mysqlDataTypeIdToPostgresType($field->type)
                    ];
                }

                $processRows = [];
                if ($result) {
                    while ($row = $result->fetch_assoc()) {
                        foreach ($row as $key => $value) {
                            if ($value === null) {
                                $row[$key] = null;
                            } else {
                                // Attempt to decode the value as JSON
                                $decodedValue = json_decode($value, true);

                                // Check if the decoding was successful and if the result is an array or object
                                if (json_last_error() === JSON_ERROR_NONE && (is_array($decodedValue) || is_object($decodedValue))) {
                                    $row[$key] = $decodedValue;
                                }
                            }
                        }
                        $processRows[] = $row;
                    }
                }
                return [
                    'fields' => $mappedFields,
                    'rows' => $processRows
                ];
            }
        } catch (\mysqli_sql_exception $e) {
            $error = $this->connection->error;
            error_log("mysqli query error: $error");
            throw $e;
        }
    }

    public function getSchemas(): array
    {
        $sql = `SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA
    WHERE schema_name != 'information_schema' 
    AND schema_name != 'performance_schema'
    and schema_name != 'sys';`;
        $results = $this->query($sql);
        $schemaNames = array_map(function ($row) {
            return $row['SCHEMA_NAME'];
        }, $results);
        return $schemaNames;
    }

    public function getTablesBySchema(array $schemaNames): array
    {
        // Implement getTablesBySchema method
        $allColumns = [];

        foreach ($schemaNames as $schema) {
            $sql = "SELECT TABLE_NAME, TABLE_SCHEMA FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '$schema'";
            $result = mysqli_query($this->connection, $sql);

            if ($result) {
                while ($row = mysqli_fetch_assoc($result)) {
                    $allColumns[] = [
                        'tableName' => $row['TABLE_NAME'],
                        'schemaName' => $row['TABLE_SCHEMA']
                    ];
                }
            }
        }

        return $allColumns;
    }

    public function getColumnInfoBySchema(string $schemaName, array $tables): array
    {
        $allColumns = [];
        foreach ($tables as $tableName) {
            $query = "
              SELECT COLUMN_NAME AS columnName, DATA_TYPE AS dataType 
              FROM INFORMATION_SCHEMA.COLUMNS 
              WHERE TABLE_SCHEMA = '{$tableName['schemaName']}' 
              AND TABLE_NAME = '{$tableName['tableName']}'
          ";

            $result = mysqli_query($this->connection, $query);

            if ($result) {
                $columns = [];
                while ($row = mysqli_fetch_assoc($result)) {
                    $columns[] = [
                        'columnName' => $row['columnName'],
                        'displayName' => $row['columnName'],
                        'dataTypeID' => TypeConverter::mysqlTextDataTypeToPostgresOID($row['dataType']),
                        'fieldType' => $row['dataType']
                    ];
                }

                $allColumns[] = [
                    'tableName' => "{$tableName['schemaName']}.{$tableName['tableName']}",
                    'displayName' => "{$tableName['schemaName']}.{$tableName['tableName']}",
                    'columns' => $columns
                ];
            }
        }
        return $allColumns;
    }
}
