<?php

namespace Quill\Database;

use PDO;

class PostgreSQLDatabase implements DatabaseInterface
{
    private $connection;

    public function connect(array $config)
    {
        $dsn = sprintf(
            'pgsql:host=%s;port=%s;dbname=%s',
            $config['host'],
            $config['port'] ?? '5432',
            $config['database']
        );

        $this->connection = new PDO($dsn, $config['user'], $config['password'], [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
            PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
            PDO::ATTR_EMULATE_PREPARES => false
        ]);

        return $this->connection;
    }

    public function disconnect()
    {
        $this->connection = null;
    }

    public function query(string $query): array
    {
        try {
            $stmt = $this->connection->query($query);
            
            if ($stmt === false) {
                return [
                    'fields' => [],
                    'rows' => [],
                ];
            }

            $rows = $stmt->fetchAll();
            $fieldCount = $stmt->columnCount();
            
            $fields = [];
            for ($i = 0; $i < $fieldCount; $i++) {
                $meta = $stmt->getColumnMeta($i);
                $fields[] = [
                    'name' => strtolower($meta['name']),
                    'dataTypeID' => $meta['native_type'] ?? 0
                ];
            }

            // Process rows to handle JSON values and normalize field names
            $processedRows = [];
            foreach ($rows as $row) {
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
                $processedRows[] = $normalizedRow;
            }

            return [
                'fields' => $fields,
                'rows' => $processedRows
            ];
        } catch (\PDOException $e) {
            error_log("PostgreSQL query error: " . $e->getMessage());
            throw $e;
        }
    }

    public function getSchemas(): array
    {
        $sql = "SELECT schema_name FROM information_schema.schemata 
                WHERE schema_name NOT LIKE 'pg_%' AND schema_name != 'information_schema'";
        $results = $this->query($sql);
        return array_map(function ($row) {
            return $row['schema_name'];
        }, $results['rows']);
    }

    public function getTablesBySchema(array $schemaNames): array
    {
        $allTables = [];

        foreach ($schemaNames as $schema) {
            $sql = "SELECT table_name, table_schema 
                    FROM information_schema.tables 
                    WHERE table_schema = '$schema'
                    
                    UNION
                    
                    SELECT c.relname as table_name, n.nspname as table_schema
                    FROM pg_class c
                    JOIN pg_namespace n ON c.relnamespace = n.oid
                    WHERE n.nspname = '$schema' 
                    AND c.relkind = 'm'";
            
            $results = $this->query($sql);

            foreach ($results['rows'] as $row) {
                $allTables[] = [
                    'tableName' => $row['table_name'],
                    'schemaName' => $row['table_schema']
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
            
            $sql = "SELECT 
                        a.attname AS column_name,
                        pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type
                    FROM pg_catalog.pg_attribute a
                    JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
                    JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
                    WHERE n.nspname = '$schema' 
                    AND c.relname = '$tableName'
                    AND a.attnum > 0 
                    AND NOT a.attisdropped
                    ORDER BY a.attnum";
            
            $results = $this->query($sql);

            $columns = [];
            foreach ($results['rows'] as $row) {
                $columns[] = [
                    'columnName' => $row['column_name'],
                    'displayName' => $row['column_name'],
                    'dataTypeID' => $this->getPostgresTypeOID($row['data_type']),
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

    private function getPostgresTypeOID(string $dataType): int
    {
        // Map PostgreSQL data types to OIDs
        $typeMap = [
            'integer' => 23,
            'bigint' => 20,
            'smallint' => 21,
            'text' => 25,
            'character varying' => 1043,
            'character' => 1042,
            'boolean' => 16,
            'real' => 700,
            'double precision' => 701,
            'numeric' => 1700,
            'date' => 1082,
            'timestamp without time zone' => 1114,
            'timestamp with time zone' => 1184,
            'time without time zone' => 1083,
            'time with time zone' => 1266,
            'json' => 114,
            'jsonb' => 3802,
            'uuid' => 2950,
            'bytea' => 17
        ];

        // Extract base type name (remove size constraints)
        $baseType = preg_replace('/\([^)]*\)/', '', strtolower($dataType));
        
        return $typeMap[$baseType] ?? 25; // Default to text (25) if type not found
    }
}
