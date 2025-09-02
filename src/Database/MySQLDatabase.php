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
                return [
                    'fields' => [],
                    'rows' => [],
                ];
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
                $mappedFields = [];

                foreach ($fields as $field) {
                    $mappedFields[] = [
                        'name' => strtolower($field->name),
                        'dataTypeID' => TypeConverter::mysqlDataTypeIdToPostgresType($field->type)
                    ];
                }

                $processRows = [];
                if ($result) {
                    while ($row = $result->fetch_assoc()) {
                        // Normalize field names to lowercase for consistency
                        $normalizedRow = [];
                        foreach ($row as $key => $value) {
                            $normalizedKey = strtolower($key);
                            if ($value === null) {
                                $normalizedRow[$normalizedKey] = null;
                            } else {
                                // Attempt to decode the value as JSON
                                $decodedValue = json_decode($value, true);

                                // Check if the decoding was successful and if the result is an array or object
                                if (json_last_error() === JSON_ERROR_NONE && (is_array($decodedValue) || is_object($decodedValue))) {
                                    $normalizedRow[$normalizedKey] = $decodedValue;
                                } else {
                                    $normalizedRow[$normalizedKey] = $value;
                                }
                            }
                        }
                        $processRows[] = $normalizedRow;
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
        $sql = "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA
                WHERE schema_name != 'information_schema' 
                AND schema_name != 'performance_schema'
                AND schema_name != 'sys'";
        $results = $this->query($sql);
        $schemaNames = array_map(function ($row) {
            return strtolower($row['SCHEMA_NAME']);
        }, $results['rows']);
        return $schemaNames;
    }

    public function getTablesBySchema(array $schemaNames): array
    {
        $allTables = [];

        foreach ($schemaNames as $schema) {
            $sql = "SELECT TABLE_NAME, TABLE_SCHEMA FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '$schema'";
            $results = $this->query($sql);

            foreach ($results['rows'] as $row) {
                $allTables[] = [
                    'tableName' => strtolower($row['TABLE_NAME']),
                    'schemaName' => strtolower($row['TABLE_SCHEMA'])
                ];
            }
        }

        return $allTables;
    }

    public function getColumnInfoBySchema(string $schemaName, array $tables): array
    {
        $allColumns = [];
        foreach ($tables as $tableInfo) {
            $tableName = $tableInfo['tableName'];
            $schema = $tableInfo['schemaName'];
            
            $sql = "SELECT COLUMN_NAME AS column_name, DATA_TYPE AS data_type 
                    FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_SCHEMA = '$schema' 
                    AND TABLE_NAME = '$tableName'";

            $results = $this->query($sql);

            $columns = [];
            foreach ($results['rows'] as $row) {
                $columns[] = [
                    'columnName' => strtolower($row['column_name']),
                    'displayName' => strtolower($row['column_name']),
                    'dataTypeID' => TypeConverter::mysqlTextDataTypeToPostgresOID($row['data_type']),
                    'fieldType' => $row['data_type']
                ];
            }

            $allColumns[] = [
                'tableName' => "{$schema}.{$tableName}",
                'displayName' => "{$schema}.{$tableName}",
                'columns' => $columns
            ];
        }
        return $allColumns;
    }
}
