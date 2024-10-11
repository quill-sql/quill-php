<?php

namespace Quill\Helpers;

class TypeConverter
{
    public static function mysqlDataTypeIdToPostgresType(int $type): int
    {
        switch ($type) {
            case 0: // DECIMAL
            case 1: // TINY
            case 2: // SHORT
            case 3: // LONG
            case 8: // LONGLONG
            case 9: // INT24
            case 13: // YEAR
                return 23;  // INTEGER

            case 4: // FLOAT
            case 5: // DOUBLE
                return 701; // DOUBLE PRECISION

            case 7: // TIMESTAMP
            case 12: // DATETIME
                return 1114; // TIMESTAMP

            case 10: // DATE
            case 242: // NEWDATE
                return 1082; // DATE

            case 15: // VARCHAR
            case 25: // VAR_STRING
            case 250: // STRING
            case 254: // ENUM
            case 255: // SET
                return 1043; // VARCHAR

            case 16: // BIT
            case 245: // TINY_BLOB
            case 246: // MEDIUM_BLOB
            case 247: // LONG_BLOB
            case 248: // BLOB
            case 251: // GEOMETRY
                return 17; // BYTEA

            default:
                return 1043; // Default to VARCHAR if unknown
        }
    }

    public static function mysqlTextDataTypeToPostgresOID(string $type): int
    {
        switch ($type) {
            case "bigint": // BIGINT in MySQL
                return 20; // BIGINT in PostgreSQL
            case "tinyint": // TINYINT in MySQL
                return 21; // SMALLINT in PostgreSQL
            case "smallint": // SMALLINT in MySQL
                return 21; // SMALLINT in PostgreSQL
            case "int": // INT in MySQL
                return 23; // INTEGER in PostgreSQL
            case "float": // FLOAT in MySQL
                return 701; // DOUBLE PRECISION in PostgreSQL
            case "double": // DOUBLE in MySQL
                return 701; // DOUBLE PRECISION in PostgreSQL
            case "varchar": // VARCHAR in MySQL
                return 1043; // VARCHAR in PostgreSQL
            case "char": // CHAR in MySQL
                return 1042; // CHAR in PostgreSQL
            case "timestamp": // TIMESTAMP in MySQL
                return 1114; // TIMESTAMP in PostgreSQL
            case "date": // DATE in MySQL
                return 1082; // DATE in PostgreSQL
            case "datetime": // DATETIME in MySQL
                return 1114; // TIMESTAMP in PostgreSQL
            case "time": // TIME in MySQL
                return 1083; // TIME in PostgreSQL
            case "year": // YEAR in MySQL
                return 23; // INTEGER in PostgreSQL
            case "binary": // BINARY in MySQL
                return 17; // BYTEA in PostgreSQL
            case "blob": // BLOB in MySQL
                return 17; // BYTEA in PostgreSQL
            case "text": // TEXT in MySQL
                return 25; // TEXT in PostgreSQL
            case "json": // JSON in MySQL
                return 114; // JSON in PostgreSQL
            default:
                return 1043; // VARCHAR in PostgreSQL (fallback)
        }
    }

    public static function convertTypeToPostgres(int $dataTypeId): string
    {
        // Path to your JSON file
        $jsonFilePath = __DIR__ . '/../pgtypes.json';
        // Read the contents of the JSON file
        global $PG_TYPES;

        if (!file_exists($jsonFilePath)) {
            die('The file does not exist.');
        }

        $jsonContent = file_get_contents($jsonFilePath);

        if ($jsonContent === false) {
            die('Error reading the JSON file.');
        }

        $PG_TYPES = json_decode($jsonContent, true);

        // Check if JSON decoding was successful
        if (json_last_error() !== JSON_ERROR_NONE) {
            die('Error decoding JSON: ' . json_last_error_msg());
        }

        // Ensure $data_type_id is an integer
        $dataTypeId = (int)$dataTypeId;

        // Filter the PG_TYPES array
        $type = array_filter($PG_TYPES, function ($item) use ($dataTypeId) {
            return (int)$item["oid"] === $dataTypeId;
        });

        $type = array_values($type);

        $type = count($type) > 0 ? $type[0]['typname'] : null;
        if (!$type) {
            return "varchar";
        }
        return $type;
    }
}
