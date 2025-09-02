<?php

namespace Quill;

use Quill\Database\ConnectionFactory;
use Quill\Database\CachedConnection;
use Quill\Config\Constants;
use Quill\Helpers\QueryHelper;
use Quill\Helpers\TenantHelper;
use Quill\Helpers\TypeConverter;

class Quill
{
    private $targetConnection;
    private $baseUrl;
    private $config;

    public const SINGLE_TENANT = 'QUILL_SINGLE_TENANT';
    public const ALL_TENANTS = 'QUILL_ALL_TENANTS';

    public function __construct(string $privateKey, string $databaseType, ?string $databaseConnectionString = null, ?array $databaseConfig = null, ?string $metadataServerURL = null)
    {
        if (!$privateKey) {
            throw new \Exception("Private key is required");
        }

        if (!$databaseType) {
            throw new \Exception("Database type is required"); 
        }

        if (!$databaseConnectionString && !$databaseConfig) {
            throw new \Exception("You must provide either DatabaseConnectionString or DatabaseConfig");
        }

        $this->baseUrl = $metadataServerURL ?: Constants::HOST;
        $this->config = "Authorization: Bearer {$privateKey}";

        $credentials = $databaseConfig ?: $this->formatDatabaseConfig($databaseType, $databaseConnectionString);

        $this->targetConnection = new CachedConnection($databaseType, $credentials);
    }

    public function query(array $params): array
    {
        $tenants = $params['tenants'] ?? null;
        $flags = $params['flags'] ?? null;
        $metadata = $params['metadata'];
        $filters = $params['filters'] ?? null;

        if ($tenants && empty($tenants)) {
            throw new \Exception("You may not pass an empty tenants array.");
        }

        if ($flags && empty($flags)) {
            throw new \Exception("You may not pass an empty flags array.");
        }

        if (!isset($metadata['task'])) {
            return ['error' => 'Missing task.', 'status' => 'error', 'data' => new \stdClass()];
        }

        $metadata['databaseType'] = $metadata['databaseType'] ?? null;

        $this->targetConnection->tenantIds = $tenants ? TenantHelper::extractTenantIds($tenants) : null;
        $responseMetadata = [];

        try {
            $tenantFlags = null;
            $flagTasks = ['dashboard', 'report', 'item', 'report-info', 'filter-options'];

            // Handle flag tasks
            if (
                in_array($metadata['task'], $flagTasks) &&
                $tenants[0] !== 'QUILL_ALL_TENANTS' &&
                $tenants[0] !== 'QUILL_SINGLE_TENANT'
            ) {
                $response = $this->postQuill('tenant-mapped-flags', [
                    'reportId' => $metadata['reportId'] ?? $metadata['dashboardItemId'] ?? null,
                    'dashboardName' => $metadata['name'] ?? null,
                    'clientId' => $metadata['clientId'] ?? null,
                    'tenants' => $tenants,
                    'flags' => $flags
                ]);

                if (isset($response['error'])) {
                    return [
                        'status' => 'error',
                        'error' => $response['error'],
                        'data' => $response['metadata'] ?? new \stdClass()
                    ];
                }

                $flagQueryResults = $this->runQueries(
                    $response['queries'],
                    $this->targetConnection->databaseType
                );

                $tenantFlags = array_map(function ($tenantField, $index) use ($flagQueryResults) {
                    $uniqueFlags = array_unique(array_map(function ($row) {
                        return $row['quill_flag'];
                    }, $flagQueryResults['queryResults'][$index]['rows']));

                    return [
                        'tenantField' => $tenantField,
                        'flags' => array_values($uniqueFlags)
                    ];
                }, $response['metadata']['queryOrder'], array_keys($response['metadata']['queryOrder']));
            } elseif ($tenants[0] === 'QUILL_SINGLE_TENANT' && $flags) {
                if (!empty($flags) && !isset($flags[0]['tenantField'])) {
                    $tenantFlags = [[
                        'tenantField' => 'QUILL_SINGLE_TENANT',
                        'flags' => $flags
                    ]];
                } else {
                    $tenantFlags = $flags;
                }
            }

            $preQueryResults = isset($metadata['preQueries'])
                ? $this->runQueries(
                    $metadata['preQueries'],
                    $this->targetConnection->databaseType,
                    $metadata['databaseType'],
                    isset($metadata['runQueryConfig']) ? $metadata['runQueryConfig'] : null
                )
                : [];

            if (isset($metadata['runQueryConfig']['overridePost']) && $metadata['runQueryConfig']['overridePost']) {
                return [
                    'data' => ['queryResults' => $preQueryResults ?? new \stdClass()],
                    'status' => 'success'
                ];
            }

            $response = $this->postQuill($metadata['task'], array_merge(
                $metadata,
                $preQueryResults,
                [
                    'sdkFilters' => $filters ?? [],
                    'tenants' => $tenants,
                    'flags' => $tenantFlags,
                    'viewQuery' => $metadata['preQueries'][0] ?? null
                ]
            ));

            if (isset($response['error'])) {
                return ['status' => 'error', 'error' => $response['error'], 'data' => new \stdClass()];
            }

            if (isset($response['metadata'])) {
                $responseMetadata = $response['metadata'];
            }

            if (!isset($response['queries'])) {
                $response['queries'] = null;
            }

            $results = $this->runQueries(
                $response['queries'],
                $this->targetConnection->databaseType,
                $metadata['databaseType'],
                isset($responseMetadata['runQueryConfig']) ? $responseMetadata['runQueryConfig'] : null
            );

            if (isset($results['mappedArray']) && isset($responseMetadata['runQueryConfig']['arrayToMap'])) {
                $arrayToMap = $responseMetadata['runQueryConfig']['arrayToMap'];
                foreach ($results['mappedArray'] as $index => $array) {
                    $responseMetadata[$arrayToMap['arrayName']][$index][$arrayToMap['field']] = $array;
                }
                unset($results['mappedArray']);
            }

            if (isset($results['queryResults']) && count($results['queryResults']) === 1) {
                $queryResults = $results['queryResults'][0];
                if (isset($queryResults['rows'])) {
                    $responseMetadata['rows'] = $queryResults['rows'];
                }
                if (isset($queryResults['fields'])) {
                    $responseMetadata['fields'] = $queryResults['fields'];
                }
            }

            return [
                'data' => $responseMetadata ? $responseMetadata : new \stdClass(),
                'queries' => $results,
                'status' => 'success'
            ];
        } catch (\Exception $e) {
            return [
                'status' => 'error',
                'error' => $e->getMessage(),
                'data' => $responseMetadata ? $responseMetadata : new \stdClass(),
            ];
        }
    }

    private function runQueries(?array $queries, string $pkDatabaseType, ?string $databaseType = null, ?array $runQueryConfig = null): array
    {
        $results = [];
        if (!isset($queries) || $queries === null) {
            return array_merge($results, ['queryResults' => []]);
        }
        if ($databaseType && strtolower($databaseType) !== strtolower($pkDatabaseType)) {
            return [
                'dbMismatched' => true,
                'backendDatabaseType' => $pkDatabaseType,
                'queryResults' => [],
            ];
        }
        if (isset($runQueryConfig['arrayToMap']) && $runQueryConfig['arrayToMap']) {
            $mappedArray = QueryHelper::mapQueries($queries, $this->targetConnection);
            return array_merge($results, ['queryResults' => [], 'mappedArray' => $mappedArray]);
        } elseif (isset($runQueryConfig['getColumns']) && $runQueryConfig['getColumns']) {
            $queryResult = $this->targetConnection->query(
                preg_replace('/;/', '', $queries[0]) . ' limit 1000'
            );
            $columns = isset($queryResult['fields']) && is_array($queryResult['fields']) ? array_map(function ($field) {
                return [
                    'fieldType' => TypeConverter::convertTypeToPostgres($field['dataTypeID']),
                    'name' => $field['name'],
                    'displayName' => $field['name'],
                    'isVisible' => true,
                    'field' => $field['name'],
                ];
            }, $queryResult['fields']) : [];
            return ['columns' => $columns];
        } elseif (isset($runQueryConfig['getColumnsForSchema']) && $runQueryConfig['getColumnsForSchema']) {
            $queryResults = array_map(function ($table) use ($runQueryConfig) {
                if (empty($table['viewQuery']) || (!$table['isSelectStar'] && empty($table['customFieldInfo']))) {
                    return $table;
                }
                $limit = isset($runQueryConfig['limitBy']) ? ' limit ' . $runQueryConfig['limitBy'] : '';
                try {
                    $queryResult = $this->targetConnection->query(
                        preg_replace('/;/', '', $table['viewQuery']) . $limit
                    );
                    $columns = array_map(function ($field) {
                        return [
                            'fieldType' => TypeConverter::convertTypeToPostgres($field['dataTypeID']),
                            'name' => $field['name'],
                            'displayName' => $field['name'],
                            'isVisible' => true,
                            'field' => $field['name'],
                        ];
                    }, $queryResult['fields']);
                    return array_merge($table, ['columns' => $columns, 'rows' => $queryResult['rows']]);
                } catch (\Exception $e) {
                    return array_merge($table, ['error' => 'Error fetching columns']);
                }
            }, $queries);

            $results = array_merge($results, ['queryResults' => $queryResults]);

            if (isset($runQueryConfig['fieldsToRemove'])) {
                $results['queryResults'] = array_map(function ($table) use ($runQueryConfig) {
                    $removedColumns = array_filter($table['columns'], function ($column) use ($runQueryConfig) {
                        return !in_array($column['name'], $runQueryConfig['fieldsToRemove']);
                    });
                    return array_merge($table, ['columns' => $removedColumns]);
                }, $queryResults);
            }
        } elseif (isset($runQueryConfig['getTables']) && $runQueryConfig['getTables']) {
            $queryResult = $this->targetConnection->pool->getTablesBySchema(
                $runQueryConfig['schemaNames'] ?? $runQueryConfig['schema']
            );
            $schemaInfo = $this->targetConnection->pool->getColumnInfoBySchema(
                $runQueryConfig['schema'],
                $queryResult
            );
            return $schemaInfo;
        } elseif (isset($runQueryConfig['runIndividualQueries']) && $runQueryConfig['runIndividualQueries']) {
            // so that one query doesn't fail the whole thing
            // the only reason this isn't the default behavior is for backwards compatibility
            $queryResults = [];
            foreach ($queries as $query) {
                try {
                    $runQuery = $query;
                    if (isset($runQueryConfig['limitBy'])) {
                        $runQuery = $this->applyLimit($query, $runQueryConfig['limitBy']);
                    }
                    $queryResult = $this->targetConnection->query($runQuery);
                    $queryResults[] = $queryResult;
                } catch (\Exception $e) {
                    $queryResults[] = [
                        'query' => $query,
                        'error' => $e->getMessage(),
                    ];
                }
            }
            $results['queryResults'] = $queryResults;
        } else {
            if (isset($runQueryConfig['limitThousand']) && $runQueryConfig['limitThousand']) {
                $queries = array_map(function ($query) {
                    return preg_replace('/;/', '', $query) . ' limit 1000;';
                }, $queries);
            } elseif (isset($runQueryConfig['limitBy'])) {
                $queries = array_map(function ($query) use ($runQueryConfig) {
                    return preg_replace('/;/', '', $query) . ' limit ' . $runQueryConfig['limitBy'];
                }, $queries);
            }

            $queryResults = array_map(function ($query) {
                return $this->targetConnection->query($query);
            }, $queries);

            // filter out nulls
            $queryResults = array_filter($queryResults, function ($item) {
                return $item !== null;
            });

            $queryResults = array_values($queryResults);

            $results = array_merge($results, ['queryResults' => $queryResults]);
            if (isset($runQueryConfig['fieldsToRemove'])) {
                $results['queryResults'] = array_map(function ($queryResult) use ($runQueryConfig) {

                    return QueryHelper::removeFields($queryResult, $runQueryConfig['fieldsToRemove']);
                }, $queryResults);
            }
            if (isset($runQueryConfig['convertDatatypes']) && $runQueryConfig['convertDatatypes']) {
                $results = array_map(function ($result) {
                    return [
                        'fields' => array_map(function ($field) {
                            return [
                                'dataTypeID' => $field['dataTypeID'],
                                'fieldType' => TypeConverter::convertTypeToPostgres($field['dataTypeID']),
                                'isVisible' => true,
                                'field' => $field['name'],
                                'displayName' => $field['name'],
                                'name' => $field['name'],
                            ];
                        }, $result['fields']),
                        'rows' => $result['rows'],
                    ];
                }, $queryResults);
            }
        }
        return $results;
    }

    private function postQuill(string $path, array $payload): array
    {
        // Build the full URL
        $url = $this->baseUrl . '/sdk/' . $path;

        // Initialize cURL
        $curl = curl_init($url);

        // Convert payload to JSON (if needed)
        $jsonPayload = json_encode($payload);

        // Prepare headers and include additional configurations from $this->config
        $headers = array_merge(
            ['Content-Type: application/json'],
            [$this->config] // Assuming headers are in a sub-array named 'headers'
        );

        // Set cURL options for a POST request
        curl_setopt($curl, CURLOPT_HTTPHEADER, $headers);
        curl_setopt($curl, CURLOPT_RETURNTRANSFER, true);
        curl_setopt($curl, CURLOPT_POST, true);
        curl_setopt($curl, CURLOPT_POSTFIELDS, $jsonPayload);

        // Execute the cURL session
        $response = curl_exec($curl);
        if ($response === false) {
            $error = curl_error($curl);
            curl_close($curl);
            throw new \Exception("cURL Error: $error");
        }

        // Optionally, get additional info about the HTTP request
        $info = curl_getinfo($curl);
        curl_close($curl);
        
        $responseData = json_decode($response, false); // Decode as object first
        $responseData = $this->preserveEmptyObjectsInArray($responseData);
        
        return $responseData;
    }
    

    private function preserveEmptyObjectsInArray($data) {
        if (is_object($data)) {
            // Convert object to an associative array and recurse
            $data = (array)$data;
            foreach ($data as $key => $value) {
                $data[$key] = $this->preserveEmptyObjectsInArray($value);
            }
            return count($data) === 0 ? (object)[] : $data; // Preserve empty objects
        } elseif (is_array($data)) {
            // Check if the array is associative
            $isAssoc = array_keys($data) !== range(0, count($data) - 1);
    
            foreach ($data as $key => $value) {
                $data[$key] = $this->preserveEmptyObjectsInArray($value);
            }
    
            return $isAssoc ? $data : array_values($data); // Keep it an indexed array if needed
        }
    
        return $data;
    }

    public function applyLimit($query, $limit)
    {
        // Simple logic: if query already has a limit, don't add another
        if (stripos($this->targetConnection->databaseType, 'mssql') !== false) {
            if (preg_match('/SELECT TOP \\d+/i', $query)) {
                return $query;
            }
            return preg_replace('/select/i', 'SELECT TOP ' . $limit, $query, 1);
        } else {
            if (stripos($query, 'limit ') !== false) {
                return $query;
            }
            return rtrim($query, ';') . ' limit ' . $limit;
        }
    }

    private function formatDatabaseConfig(string $databaseType, string $connectionString): array
    {
        switch (strtolower($databaseType)) {
            case 'mysql':
                return ConnectionFactory::formatMysqlConfig($connectionString);
            case 'postgresql':
            case 'postgres':
                return ConnectionFactory::formatPostgresConfig($connectionString);
            default:
                throw new \InvalidArgumentException("Unsupported database type: {$databaseType}");
        }
    }
}
