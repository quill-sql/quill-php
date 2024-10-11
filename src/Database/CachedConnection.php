<?php

namespace Quill\Database;

class CachedConnection
{
    public $databaseType;
    public $pool;
    public $orgId;

    public function __construct(string $databaseType, array $config, ?array $cacheConfig = null)
    {
        $this->databaseType = $databaseType;
        $this->pool = ConnectionFactory::createConnection($databaseType, $config);
    }

    public function query(string $text): array
    {
        try {
            $newResult = $this->pool->query($text);
            return $newResult;
        } catch (\Exception $e) {
            // propagate the error for Quill to catch
            throw $e;
        }
    }

    public function close(): void
    {
        if ($this->pool instanceof \mysqli) {
            $this->pool->close();
        }
    }

    public function __destruct()
    {
        $this->close();
    }
}
