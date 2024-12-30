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

    public function fetchDocuments($limit, $lastEvaluatedKey = null)
    {
        $params = [
            'TableName' => env('AWS_DYNAMODB_TABLE'),
            'Limit' => $limit,
        ];

        // Si tenemos una última clave evaluada (para paginación), la añadimos
        if ($lastEvaluatedKey) {
            $params['ExclusiveStartKey'] = $lastEvaluatedKey;
        }

        try {
            // Realizamos la consulta a DynamoDB
            $result = $this->dynamoDb->scan($params);

            // Mapeamos los resultados al formato deseado
            $items = array_map(function ($item) {
                return [
                    'DOCUMENT_ID' => $item['DOCUMENT_ID']['S'] ?? null,
                    'AWS_LAMBDA_LOG_GROUP_NAME' => $item['AWS_LAMBDA_LOG_GROUP_NAME']['S'] ?? null,
                    'AWS_LAMBDA_LOG_STREAM_NAME' => $item['AWS_LAMBDA_LOG_STREAM_NAME']['S'] ?? null,
                    'CREATED_DATE' => $item['CREATED_DATE']['S'] ?? null,
                    'CREATED_USER' => $item['CREATED_USER']['S'] ?? null,
                    'DOCUMENT_1' => [
                        'DETALLE_FACTURA' => isset($item['DOCUMENT_1']['M']['DETALLE_FACTURA']['L'])
                            ? array_map(function ($detalle) {
                                return [
                                    'CODIGO' => $detalle['M']['CODIGO']['S'] ?? null,
                                    'DESCRIPCION' => $detalle['M']['DESCRIPCION']['S'] ?? null,
                                    'TOTAL_ITEM' => $detalle['M']['TOTAL_ITEM']['S'] ?? null,
                                ];
                            }, $item['DOCUMENT_1']['M']['DETALLE_FACTURA']['L'])
                            : [],
                        'DIRECCION_CLIENTE' => $item['DOCUMENT_1']['M']['DIRECCION_CLIENTE']['S'] ?? null,
                        'ESTADO_DOCUMENTO' => $item['DOCUMENT_1']['M']['ESTADO_DOCUMENTO']['S'] ?? null,
                        'FECHA_EMISION' => $item['DOCUMENT_1']['M']['FECHA_EMISION']['S'] ?? null,
                        'LOCAL' => $item['DOCUMENT_1']['M']['LOCAL']['S'] ?? null,
                        'MONTO_EN_LETRAS' => $item['DOCUMENT_1']['M']['MONTO_EN_LETRAS']['S'] ?? null,
                        'NATURALEZA_DOCUMENTO' => $item['DOCUMENT_1']['M']['NATURALEZA_DOCUMENTO']['S'] ?? null,
                        'NUMERO_FACTURA' => $item['DOCUMENT_1']['M']['NUMERO_FACTURA']['S'] ?? null,
                        'OBSERVACION' => $item['DOCUMENT_1']['M']['OBSERVACION']['S'] ?? null,
                        'RUC_CLIENTE' => $item['DOCUMENT_1']['M']['RUC_CLIENTE']['S'] ?? null,
                        'RUC_EMPRESA' => $item['DOCUMENT_1']['M']['RUC_EMPRESA']['S'] ?? null,
                        'RUM' => $item['DOCUMENT_1']['M']['RUM']['S'] ?? null,
                        'TOTAL_FACTURA' => $item['DOCUMENT_1']['M']['TOTAL_FACTURA']['S'] ?? null,
                        'USUARIO' => $item['DOCUMENT_1']['M']['USUARIO']['S'] ?? null,
                    ],
                    'DOCUMENT_STRUCTURE_ID' => $item['DOCUMENT_STRUCTURE_ID']['S'] ?? null,
                    'LLM_USED' => $item['LLM_USED']['S'] ?? null,
                    'MODIFY_DATE' => $item['MODIFY_DATE']['S'] ?? null,
                    'MODIFY_USER' => $item['MODIFY_USER']['S'] ?? null,
                    'STATUS' => $item['STATUS']['S'] ?? null,
                ];
            }, $result['Items']);

            $hasNextPage = isset($result['LastEvaluatedKey']);

            // Retornar los elementos y la clave de la siguiente página (si existe)
            return [
                'items' => $items,
                'hasNextPage' => $hasNextPage,
                'lastEvaluatedKey' => $result['LastEvaluatedKey'] ?? null
            ];

        } catch (DynamoDbException $e) {
            throw new \Exception('Could not fetch documents: ' . $e->getMessage());
        }
    }

    public function fetchAllDocuments()
{
    $params = [
        'TableName' => env('AWS_DYNAMODB_TABLE'),
    ];

    $allItems = [];
    try {
        do {
            $result = $this->dynamoDb->scan($params);

            // Agregar los elementos de la página actual a la lista completa
            $allItems = array_merge($allItems, $result['Items']);

            // Configurar el token para obtener la siguiente página
            $params['ExclusiveStartKey'] = $result['LastEvaluatedKey'] ?? null;

        } while (!empty($params['ExclusiveStartKey']));

        // Opcionalmente, puedes mapear los resultados si lo necesitas
        return array_map(function ($item) {
            return [
                'DOCUMENT_ID' => $item['DOCUMENT_ID']['S'] ?? null,
                'FACTURA_ID' => $item['FACTURA_ID']['S'] ?? null,
                'FECHA' => $item['FECHA']['S'] ?? null,
                'DIRECCION' => $item['DIRECCION']['S'] ?? null,
                'TOTAL' => $item['TOTAL']['S'] ?? null,
                // Agrega más campos si es necesario
            ];
        }, $allItems);

    } catch (DynamoDbException $e) {
        throw new \Exception('Could not fetch documents: ' . $e->getMessage());
    }
}
}
