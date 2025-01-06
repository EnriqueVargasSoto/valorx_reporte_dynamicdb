<?php

namespace App\Services;

use Aws\DynamoDb\Marshaler;

use Aws\DynamoDb\DynamoDbClient;
use Aws\DynamoDb\Exception\DynamoDbException;

use Aws\DynamoDb\ScanCommand; // Importa la clase ScanCommand
use Aws\CommandInterface; // Si usas comandos adicionales
use Illuminate\Support\Facades\Storage;
use Aws\S3\S3Client;
use Aws\Exception\AwsException;
use Illuminate\Support\Facades\Response;

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

    public function fetchDocuments($limit, $lastEvaluatedKey = null, $previousEvaluatedKey = null,  $searchTerm = null)
    {
        $params = [
            'TableName' => env('AWS_DYNAMODB_TABLE'),
            'Limit' => $limit,
        ];

        // Si hay una clave para la siguiente página, la agregamos
        if ($lastEvaluatedKey) {
            $params['ExclusiveStartKey'] = $lastEvaluatedKey;
        }

        // Si hay una clave para la página anterior, la usamos para obtener los resultados hacia atrás
        if ($previousEvaluatedKey) {
            $params['ExclusiveStartKey'] = $previousEvaluatedKey;
        }

        // Si se pasó un término de búsqueda, filtramos por el campo RUC_CLIENTE
        if ($searchTerm) {
            // Aquí asumimos que el campo 'DOCUMENT_1' tiene una subclave 'RUC_CLIENTE'
            $params['FilterExpression'] = 'contains(DOCUMENT_1.RUC_CLIENTE, :searchTerm) OR contains(DOCUMENT_1.NUMERO_FACTURA, :searchTerm)';//$params['FilterExpression'] = 'contains(DOCUMENT_1.RUC_CLIENTE, :searchTerm)';
            $params['ExpressionAttributeValues'] = [
                ':searchTerm' => ['S' => $searchTerm], // 'S' es el tipo de atributo para strings en DynamoDB
            ];
        }

        try {
            // Realizamos la consulta a DynamoDB

            $result = $this->dynamoDb->scan($params);

            // Obtener el total de los elementos
            //$totalItems = $this->getTotalItems(); // Aquí debes contar el total de elementos de la tabla


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
                    /* 'IMAGEN' => $this->generateImage($item['DOCUMENT_ID']['S']) */
                ];
            }, $result['Items']);

            $hasNextPage = isset($result['LastEvaluatedKey']);
            $hasPreviousPage = $previousEvaluatedKey ? true : false;

            // Si se pasa un término de búsqueda, buscar en toda la tabla
            if ($searchTerm) {
                $foundItem = $this->searchFullTable($searchTerm);

                if ($foundItem) {
                    $response['search_result'] = $foundItem; // Resultado de la búsqueda
                } else {
                    $response['search_result'] = 'No encontrado';
                }

                $items = $response['search_result'];
                //array_push($items,$response['search_result']);
            }


            // Retornar los elementos y la clave de la siguiente página (si existe)
            return [
                'items' => $items,
                'hasNextPage' => $hasNextPage,
                'lastEvaluatedKey' => $hasNextPage ? $result['LastEvaluatedKey'] : null,
                'hasPreviousPage' => $hasPreviousPage,
                'previousEvaluatedKey' => $previousEvaluatedKey, // Retorna la clave para la página anterior
            ];

        } catch (DynamoDbException $e) {
            throw new \Exception('Could not fetch documents: ' . $e->getMessage());
        }
    }

    private function searchFullTable($searchTerm)
    {
        $params = [
            'TableName' => env('AWS_DYNAMODB_TABLE'),
            'FilterExpression' => 'contains(DOCUMENT_1.RUC_CLIENTE, :searchTerm) OR contains(DOCUMENT_1.NUMERO_FACTURA, :searchTerm)',
            'ExpressionAttributeValues' => [
                ':searchTerm' => ['S' => $searchTerm],
            ],
        ];

        $results = []; // Almacenar todos los resultados

        do {
            $result = $this->dynamoDb->scan($params);

            foreach ($result['Items'] as $item) {
                // Verificar si el RUC o número de factura coinciden
                if (
                    (isset($item['DOCUMENT_1']['M']['RUC_CLIENTE']['S']) &&
                        strpos($item['DOCUMENT_1']['M']['RUC_CLIENTE']['S'], $searchTerm) !== false) ||
                    (isset($item['DOCUMENT_1']['M']['NUMERO_FACTURA']['S']) &&
                        strpos($item['DOCUMENT_1']['M']['NUMERO_FACTURA']['S'], $searchTerm) !== false)
                ) {
                    $results[] = [
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
                }
            }

            $params['ExclusiveStartKey'] = $result['LastEvaluatedKey'] ?? null;
        } while (isset($result['LastEvaluatedKey']));

        return $results; // No se encontró el término de búsqueda
    }

    private function getTotalItems()
    {
        // Ejecutar un conteo total sobre la tabla de DynamoDB
        $params = [
            'TableName' =>  env('AWS_DYNAMODB_TABLE'),
        ];

        $result = $this->dynamoDb->scan($params);

        return count($result['Items']); // Retorna la cantidad total de registros
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

    public function generateImage($key){

        $s3Client = new S3Client([
            'version' => 'latest',
            'region'  => env('AWS_DEFAULT_REGION'),
            'credentials' => [
                'key'    => env('AWS_ACCESS_KEY_ID'),
                'secret' => env('AWS_SECRET_ACCESS_KEY'),
            ],
        ]);

        try {
            $command = $s3Client->getCommand('GetObject', [
                'Bucket' => env('AWS_BUCKET'),
                'Key'    => $key,
                'ResponseCacheControl' => 'no-cache',
                'ResponseContentDisposition' => 'inline', // Asegúrate de que el archivo se muestre en línea
                'ResponseContentType' => 'image/png', // Tipo de contenido correcto para imágenes PNG
            ]);

            // Generar la URL firmada con expiración de 1 hora
            $signedUrl = $s3Client->createPresignedRequest($command, '+60 minutes')->getUri();
            //return redirect()->to((string)$signedUrl);//return (string)$signedUrl;//response()->json(['url' => (string)$signedUrl]);

            // Obtener el archivo desde la URL firmada
            $fileContent = file_get_contents((string)$signedUrl);

            // Retornar la imagen con el tipo MIME correcto
            return Response::make($fileContent, 200, [
                'Content-Type' => 'image/png', // Cambiar según el tipo de archivo si es necesario
                'Content-Disposition' => 'inline; filename="' . basename($key) . '"'
            ]);
        } catch (AwsException $e) {
            return response()->json(['error' => 'Could not generate signed URL: ' . $e->getMessage()], 500);
        }
    }
}
