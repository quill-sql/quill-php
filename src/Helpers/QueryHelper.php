<?php

namespace Quill\Helpers;

class QueryHelper
{
    public static function removeFields(array $queryResults, array $fieldsToRemove): array
    {
        $result = [];

        // Handle 'fields' key if it exists
        if (isset($queryResults['fields'])) {
            $result['fields'] = array_values(array_filter($queryResults['fields'], function ($field) use ($fieldsToRemove) {
                return !in_array($field['name'], $fieldsToRemove);
            }));
        }

        // Handle 'rows' key if it exists
        if (isset($queryResults['rows'])) {
            $result['rows'] = array_map(function ($row) use ($fieldsToRemove) {
                foreach ($fieldsToRemove as $field) {
                    unset($row[$field]);
                }
                return $row;
            }, $queryResults['rows']);
        }

        return $result;
    }

    public static function mapQueries(array $queries, $targetConnection): array
    {
        $mappedArray = [];
        for ($i = 0; $i < count($queries); $i++) {
            $queryResult = $targetConnection->query($queries[$i]);
            array_push($mappedArray, $queryResult['rows']);
        }
        return $mappedArray;
    }
}
