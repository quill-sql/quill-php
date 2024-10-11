<?php

namespace Quill;
// Include the Quill class
require_once '../src/Quill.php';
require '../vendor/autoload.php';

$dotenv = \Dotenv\Dotenv::createImmutable(dirname(__DIR__));
$dotenv->load();
// Set up the necessary configuration
$privateKey = $_ENV['PRIVATE_KEY'];
$databaseType = $_ENV['DB_TYPE'];
$databaseConnectionString = $_ENV['DB_CONNECTION'];
$metadataServerURL = $_ENV['SERVER_URL'];

// Create an instance of the Quill class
$quill = new Quill($privateKey, $databaseType, $databaseConnectionString, null, $metadataServerURL);
// Handle the request
$requestMethod = $_SERVER['REQUEST_METHOD'];
$fullUri = $_SERVER['REQUEST_URI'];

// Extract the path and query parameters
$parsedUrl = parse_url($fullUri);
$endpoint = $parsedUrl['path']; // Extracts the path part of the URI
$queryParams = isset($parsedUrl['query']) ? $parsedUrl['query'] : ''; // Extracts the query string part

// Set up CORS headers
header("Access-Control-Allow-Origin: *"); // Allow requests from any origin, replace * with specific domain if needed
header("Access-Control-Allow-Methods: GET, POST, OPTIONS"); // Allow specific HTTP methods
header("Access-Control-Allow-Headers: Content-Type, Authorization"); // Allow specific headers

// Handle OPTIONS requests
if ($requestMethod === 'OPTIONS') {
    // No need to send a body
    http_response_code(204); // No Content
    exit;
}

if ($requestMethod == 'GET' && $endpoint == '/') {
    echo 'Hello, World!';
} else if ($requestMethod === 'POST' && $endpoint === '/quill') {
    // Get the POST data
    $input = file_get_contents('php://input');
    $data = json_decode($input, true);
    $orgId = isset($data['orgId']) ? $data['orgId'] : (isset($data['metadata']['orgId']) ? $data['metadata']['orgId'] : null);
    // Validate data
    if (isset($data['metadata'])) {
        $params = [
            'metadata' => $data['metadata'],
            'orgId' => $orgId
        ];

        // Call the query method
        $response = $quill->query($params);
        // Return JSON response
        header('Content-Type: application/json');
        $body = json_encode($response, JSON_PRETTY_PRINT);
        echo $body;
        exit;
    } else {
        // Invalid data
        http_response_code(400);
        echo json_encode(['error' => 'Invalid request payload.'], JSON_PRETTY_PRINT);
        exit;
    }
} else {
    // Handle other methods or endpoints
    http_response_code(404);
    error_log('Endpoint not found: ' . $requestMethod . ' ' . $endpoint);
    echo json_encode(['error' => 'Endpoint not found.'], JSON_PRETTY_PRINT);
    exit;
}
