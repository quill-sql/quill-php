<?php

namespace Quill\Database;

interface DatabaseInterface
{
    public function connect(array $config);
    public function disconnect();
    public function query(string $query): array;
    public function getSchemas(): array;
    public function getTablesBySchema(array $schemaName): array;
    public function getColumnInfoBySchema(string $schemaName, array $tables): array;
}
