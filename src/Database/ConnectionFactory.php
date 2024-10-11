<?php

namespace Quill\Database;

class ConnectionFactory
{
    public static function createConnection(string $databaseType, array $config): DatabaseInterface
    {
        switch (strtolower($databaseType)) {
            case 'mysql':
                $database = new MySQLDatabase();
                $database->connect($config);
                return $database;
            default:
                throw new \InvalidArgumentException("Unsupported database type: {$databaseType}");
        }
    }

    public static function formatMysqlConfig(string $connectionString): array
    {
        $parsedUrl = parse_url($connectionString);

        return [
            'host' => $parsedUrl['host'] ?? '',
            'user' => $parsedUrl['user'] ?? '',
            'password' => $parsedUrl['pass'] ?? '',
            'database' => ltrim($parsedUrl['path'], '/') ?? '',
            'port' => $parsedUrl['port'] ?? '3306'
        ];
    }
}
