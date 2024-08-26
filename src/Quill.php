<?php

namespace Quill;

$ENV = 'production'; // You can set the environment variable dynamically in your actual implementation
define('HOST', $ENV === 'development' ? 'http://localhost:8080' : 'https://quill-344421.uc.r.appspot.com');

function connectToMysql($config)
{
  $host = $config['host'] ?? '';
  $username = $config['user'] ?? '';
  $password = $config['password'] ?? '';
  $database = $config['database'] ?? '';
  $port = $config['port'] ?? '3306';
  $pool = new \mysqli('p:' . $host, $username, $password, $database, $port);
  return $pool;
}

function mysqlDataTypeIdToPostgresType($type)
{
  switch ($type) {
    case 8: // int
    case 3: // int
    case 2: // int
      return 23;
    case 5: // float
      return 700;
    case 253: // varchar
      return 1043;
    case 7: // date
      return 1082;
    case 10: // date
      return 1082;
    case 12: // date
      return 1082;
    default: // varchar
      return 1043;
  }
}

function mysqlTextDataTypeToPostgresOID($type)
{
  switch ($type) {
    case "bigint": // int
      return 23;
    case "tinyint": // int
      return 23;
    case "float": // float
      return 700;
    case "varchar": // varchar
      return 1043;
    case "timestamp": // date
      return 1082;
    case "date": // date
      return 1082;
    case "datetime": // date
      return 1082;
    default: // varchar
      return 1043;
  }
}

function runQueryMysql($query, $pool)
{
  $sql = "SELECT * FROM table_name"; // Your SQL query
  $result = $pool->query($query);

  if ($result === false) {
    // Handle query error
    echo "Error: " . $pool->error;
  } else {
    // Process the result
    $fields = $result->fetch_fields();

    foreach ($fields as $field) {
      $mappedFields[] = [
        'name' => $field->name,
        'dataTypeID' => mysqlDataTypeIdToPostgresType($field->type)
      ];
    }

    $processRows = [];
    if ($result) {
      while ($row = $result->fetch_assoc()) {
        $processRows[] = $row;
      }
    }
    return [
      'fields' => $mappedFields,
      'rows' => $processRows
    ];
  }
}

function getSchemasMysql($connection)
{
  $sql = `SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA
    WHERE schema_name != 'information_schema' 
    AND schema_name != 'performance_schema'
    and schema_name != 'sys';`;
  $results = runQueryMysql($sql, $connection);
  $schemaNames = array_map(function ($row) {
    return $row['SCHEMA_NAME'];
  }, $results);
  return $schemaNames;
}

function disconnectFromMysql($pool)
{
  $pool->close();
}


function connectToDatabase(
  $databaseType,
  $config
) {
  $databaseType = strtolower($databaseType);
  switch ($databaseType) {
    case "mysql":
      return connectToMysql($config);
    default:
      return connectToMysql($config);
  }
}

function formatMysqlConfig($connectionString)
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

function getTablesBySchemaByDatabase(
  $databaseType,
  $connection,
  $schemaName
) {
  $databaseType = strtolower($databaseType);
  switch ($databaseType) {
    case "mysql":
      return getTablesBySchemaMysql(
        $connection,
        $schemaName
      );
    default:
      return null;
  }
}
function getTablesBySchemaMysql(
  $connection,
  $schemaNames
) {
  $allColumns = [];

  foreach ($schemaNames as $schema) {
    $sql = "SELECT TABLE_NAME, TABLE_SCHEMA FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '$schema'";
    $result = mysqli_query($connection, $sql);

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

function getColumnInfoBySchemaByDatabase(
  $databaseType,
  $connection,
  $schemaName,
  $tables
) {
  $databaseType = strtolower($databaseType);
  switch ($databaseType) {
    case "mysql":
      return getSchemaColumnInfoMysql(
        $connection,
        $schemaName,
        $tables,
      );
    default:
      return null;
  }
}

function getSchemaColumnInfoMysql(
  $connection,
  $schemaName,
  $tableNames
) {

  $allColumns = [];
  foreach ($tableNames as $tableName) {
    $query = "
              SELECT COLUMN_NAME AS columnName, DATA_TYPE AS dataType 
              FROM INFORMATION_SCHEMA.COLUMNS 
              WHERE TABLE_SCHEMA = '{$tableName['schemaName']}' 
              AND TABLE_NAME = '{$tableName['tableName']}'
          ";

    $result = mysqli_query($connection, $query);

    if ($result) {
      $columns = [];
      while ($row = mysqli_fetch_assoc($result)) {
        $columns[] = [
          'columnName' => $row['columnName'],
          'displayName' => $row['columnName'],
          'dataTypeID' => mysqlTextDataTypeToPostgresOID($row['dataType']),
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

function getSchemasByDatabase(
  $databaseType,
  $connection
) {
  $databaseType = strtolower($databaseType);
  switch ($databaseType) {
    case "mysql":
      return getSchemasMysql($connection);
    default:
      return null;
  }
}

function runQueryByDatabase(
  $databaseType,
  $connection,
  $sql
) {
  $databaseType = strtolower($databaseType);
  switch ($databaseType) {
    case "mysql":
      return runQueryMysql($sql, $connection);
    default:
      return null;
  }
}

function disconnectFromDatabase(
  $databaseType,
  $database
) {
  $databaseType = strtolower($databaseType);
  switch ($databaseType) {
    case "mysql":
      return disconnectFromMysql($database);
    default:
      return null;
  }
}

function getDatabaseCredentials($databaseType, $connectionString)
{
  $databaseType = strtolower($databaseType);

  switch ($databaseType) {
    case 'mysql':
      return formatMysqlConfig($connectionString);
    default:
      return null;
  }
}

class CachedConnection
{
  public $databaseType;
  public $pool;
  public $orgId;

  public function __construct(
    $databaseType,
    $config,
    $cacheConfig = null
  ) {
    $this->databaseType = $databaseType;
    $this->pool = connectToDatabase($databaseType, $config);
  }

  public function query($text)
  {
    try {
      $newResult = runQueryByDatabase(
        $this->databaseType,
        $this->pool,
        $text
      );

      return $newResult;
    } catch (Exception $e) {
      error_log('Query error: ' . $e->getMessage());
      return ['error' => 'An error occurred while executing the query.'];
    }
  }

  public function close()
  {
    if ($this->pool instanceof mysqli) {
      $this->pool->close();
    }
  }

  public function __destruct()
  {
    $this->close();
  }
}

function removeFields(array $queryResults, array $fieldsToRemove): array
{
  $fields = array_filter($queryResults['fields'], function ($field) use ($fieldsToRemove) {
    return !in_array($field['name'], $fieldsToRemove);
  });

  $rows = array_map(function ($row) use ($fieldsToRemove) {
    foreach ($fieldsToRemove as $field) {
      unset($row[$field]);
    }
    return $row;
  }, $queryResults['rows']);

  return [
    'fields' => array_values($fields), // Reset array keys
    'rows' => $rows,
  ];
}

function convertTypeToPostgres($data_type_id)
{
  // Path to your JSON file
  $jsonFilePath = __DIR__ . '/pgtypes.json';
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

  // $type = array_filter($PG_TYPES, function ($item) use ($data_type_id) {
  //   return $item['oid'] === $data_type_id;
  // });

  // Ensure $data_type_id is an integer
  $data_type_id = (int)$data_type_id;

  // Filter the PG_TYPES array
  $type = array_filter($PG_TYPES, function ($item) use ($data_type_id) {
    return (int)$item["oid"] === $data_type_id;
  });

  $type = array_values($type);

  $type = count($type) > 0 ? $type[0]['typname'] : null;
  if (!$type) {
    return "varchar";
  }
  return $type;
}

function mapQueries(
  $queries,
  $targetConnection
) {
  $mappedArray = [];
  for ($i = 0; $i < count($queries); $i++) {
    $queryResult = $targetConnection->query($queries[$i]);
    array_push($mappedArray, $queryResult['rows']);
  }
  return $mappedArray;
}

class Quill
{
  public $targetConnection;
  public $baseUrl;
  // string instead of object
  public $config;
  public function __construct($privateKey, $databaseType, $databaseConnectionString = null, $databaseConfig = null, $metadataServerURL = null)
  {
    $this->baseUrl = $metadataServerURL ? $metadataServerURL : HOST;
    $this->config = "Authorization: Bearer {$privateKey}";

    $credentials = $databaseConfig;

    if ($databaseConnectionString) {
      $credentials = getDatabaseCredentials(
        $databaseType,
        $databaseConnectionString
      );
    }

    $this->targetConnection = new CachedConnection(
      $databaseType,
      $credentials,
      $cache ?? []
    );
  }

  public function query($params)
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
          'data' => ['queryResults' => $preQueryResults],
          'status' => 'success'
        ];
      }

      if (!isset($metadata['task'])) {
        return ['error' => 'Missing task.', 'status' => 'error', 'data' => new stdClass()];
      }
      $response = $this->postQuill($metadata['task'], array_merge(
        $metadata,
        $preQueryResults,
        ['orgId' => $orgId, 'viewQuery' => $metadata['preQueries'][0] ?? null]
      ));

      if (isset($response['error'])) {
        return ['status' => 'error', 'error' => $response['error'], 'data' => new stdClass()];
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
        'data' => $responseMetadata ? $responseMetadata : new stdClass(),
        'queries' => $results,
        'status' => 'success'
      ];
    } catch (Exception $e) {
      return [
        'status' => 'error',
        'error' => $e->getMessage(),
        'data' => $responseMetadata
      ];
    }
  }



  private function runQueries(
    $queries,
    $pkDatabaseType,
    $databaseType = null,
    $runQueryConfig = null
  ) {
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
      $mappedArray = mapQueries($queries, $this->targetConnection);
      return array_merge($results, ['queryResults' => [], 'mappedArray' => $mappedArray]);
    } elseif (isset($runQueryConfig['getColumns']) && $runQueryConfig['getColumns']) {
      $queryResult = $this->targetConnection->query(
        preg_replace('/;/', '', $queries[0]) . ' limit 1'
      );
      $columns = isset($queryResult['fields']) && is_array($queryResult['fields']) ? array_map(function ($field) {
        return [
          'fieldType' => convertTypeToPostgres($field['dataTypeID']),
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
              'fieldType' => convertTypeToPostgres($field['dataTypeID']),
              'name' => $field['name'],
              'displayName' => $field['name'],
              'isVisible' => true,
              'field' => $field['name'],
            ];
          }, $queryResult['fields']);
          return array_merge($table, ['columns' => $columns, 'rows' => $queryResult['rows']]);
        } catch (Exception $e) {
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
      $queryResult = getTablesBySchemaByDatabase(
        $this->targetConnection->databaseType,
        $this->targetConnection->pool,
        $runQueryConfig['schemaNames'] ?? $runQueryConfig['schema']
      );
      $schemaInfo = getColumnInfoBySchemaByDatabase(
        $this->targetConnection->databaseType,
        $this->targetConnection->pool,
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

          return removeFields($queryResult, $runQueryConfig['fieldsToRemove']);
        }, $queryResults);
      }
      if (isset($runQueryConfig['convertDatatypes']) && $runQueryConfig['convertDatatypes']) {
        $results = array_map(function ($result) {
          return [
            'fields' => array_map(function ($field) {
              return [
                'dataTypeID' => $field['dataTypeID'],
                'fieldType' => convertTypeToPostgres($field['dataTypeID']),
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


  public function postQuill($path, $payload)
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
      throw new Exception("cURL Error: $error");
    }

    // Optionally, get additional info about the HTTP request
    $info = curl_getinfo($curl);
    curl_close($curl);

    // Assuming the response is JSON
    $responseData = json_decode($response, true);

    return $responseData;
  }
}
