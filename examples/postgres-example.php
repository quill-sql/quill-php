<?php

require_once '../src/Quill.php';
require '../vendor/autoload.php';

// Example usage with PostgreSQL
$privateKey = 'your-private-key-here';
$databaseType = 'postgresql'; // or 'postgres'
$databaseConnectionString = 'postgresql://username:password@localhost:5432/database_name';
$metadataServerURL = 'https://your-metadata-server.com';

try {
    // Create an instance of the Quill class with PostgreSQL
    $quill = new Quill($privateKey, $databaseType, $databaseConnectionString, null, $metadataServerURL);
    
    // Example query parameters
    $params = [
        'metadata' => [
            'task' => 'report',
            'reportId' => 'your-report-id',
            'databaseType' => 'postgresql'
        ],
        'tenants' => ['QUILL_SINGLE_TENANT']
    ];
    
    // Execute the query
    $response = $quill->query($params);
    
    // Output the results
    echo "Query executed successfully!\n";
    echo "Response: " . json_encode($response, JSON_PRETTY_PRINT) . "\n";
    
} catch (Exception $e) {
    echo "Error: " . $e->getMessage() . "\n";
}

// Alternative: Using array configuration instead of connection string
$databaseConfig = [
    'host' => 'localhost',
    'port' => '5432',
    'database' => 'your_database',
    'user' => 'your_username',
    'password' => 'your_password'
];

try {
    $quillWithConfig = new Quill($privateKey, 'postgresql', null, $databaseConfig, $metadataServerURL);
    echo "PostgreSQL connection established with array config!\n";
    
} catch (Exception $e) {
    echo "Error with array config: " . $e->getMessage() . "\n";
}
