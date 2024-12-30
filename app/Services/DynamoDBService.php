<?php

namespace App\Services;

use Aws\DynamoDb\Marshaler;

use Aws\DynamoDb\DynamoDbClient;
use Aws\DynamoDb\Exception\DynamoDbException;

use Aws\DynamoDb\ScanCommand; // Importa la clase ScanCommand
use Aws\CommandInterface; // Si usas comandos adicionales

class DynamoDBService
{
    private $dynamoDb;
    /**
     * Create a new class instance.
     */
    public function __construct()
    {
        //
        $this->dynamoDb = new DynamoDbClient([
            'region' => env('AWS_REGION'),
            'version' => 'latest',
            'credentials' => [
                'key' => env('AWS_ACCESS_KEY_ID'),
                'secret' => env('AWS_SECRET_ACCESS_KEY'),
            ],
        ]);
    }

    public function listItems($limit = 10, $lastKey = null)
    {
        $params = [
            'TableName' => env('AWS_DYNAMODB_TABLE'),
            'Limit' => $limit,
        ];

        if ($lastKey) {
            $params['ExclusiveStartKey'] = ['DOCUMENT_ID' => ['S' => $lastKey]];
        }

        try {
            $result = $this->dynamoDb->scan($params);
            return [
                'items' => $result['Items'] ?? [],
                'lastKey' => $result['LastEvaluatedKey'] ?? null,
            ];
        } catch (DynamoDbException $e) {
            throw new \Exception("Error fetching data from DynamoDB: " . $e->getMessage());
        }
    }

    public function fetchDocuments($limit)
    {
        $params = [
            'TableName' => 'KendraStack-datalakeclsdevdocumentmetadatadyanmotable2A161681-2GF751K1QGEF',
            'Limit' => $limit,
        ];

        try {
            $result = $this->dynamoDb->scan($params); // Cambiado a scan()
            // Opcionalmente puedes mapear los resultados si lo necesitas
            return array_map(function ($item) {
                return [
                    'DOCUMENT_ID' => $item['DOCUMENT_ID'] ?? null,
                    'FACTURA_ID' => $item['FACTURA_ID'] ?? null,
                    'FECHA' => $item['FECHA'] ?? null,
                    'DIRECCION' => $item['DIRECCION'] ?? null,
                    'TOTAL' => $item['TOTAL'] ?? null,
                ];
            }, $result['Items']);
        } catch (DynamoDbException $e) {
            throw new \Exception('Could not fetch documents: ' . $e->getMessage());
        }
    }

}
