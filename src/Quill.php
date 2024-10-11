<?php

namespace Quill;

use Quill\Database\ConnectionFactory;
use Quill\Database\CachedConnection;
use Quill\Config\Constants;
use Quill\Helpers\QueryHelper;
use Quill\Helpers\TypeConverter;

class Quill
{
    private $targetConnection;
    private $baseUrl;
    private $config;

    public function __construct(string $privateKey, string $databaseType, ?string $databaseConnectionString = null, ?array $databaseConfig = null, ?string $metadataServerURL = null)
    {
        $this->baseUrl = $metadataServerURL ?: Constants::HOST;
        $this->config = "Authorization: Bearer {$privateKey}";

        $credentials = $databaseConfig ?: ConnectionFactory::formatMysqlConfig($databaseConnectionString);

        $this->targetConnection = new CachedConnection($databaseType, $credentials);
    }

    public function query(array $params): array
    {
        $orgId = $params['orgId'];
        $metadata = $params['metadata'];

        $metadata['databaseType'] = $metadata['databaseType'] ?? null;

        $this->targetConnection->orgId = $orgId;
        $responseMetadata = [];

        try {
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

            if (!isset($metadata['task'])) {
                return ['error' => 'Missing task.', 'status' => 'error', 'data' => new \stdClass()];
            }
            $response = $this->postQuill($metadata['task'], array_merge(
                $metadata,
                $preQueryResults,
                ['orgId' => $orgId, 'viewQuery' => $metadata['preQueries'][0] ?? null]
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

        // Assuming the response is JSON
        $responseData = json_decode($response, true);

        return $responseData;
    }
}
