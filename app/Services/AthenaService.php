<?php

namespace App\Services;

use Aws\Athena\AthenaClient;
use Exception;

class AthenaService
{
    protected $client;
    protected $database;

    public function __construct()
    {
        $this->client = new AthenaClient([
            'version' => 'latest',
            'region' => env('AWS_REGION'),
            'credentials' => [
                'key'    => env('AWS_ACCESS_KEY_ID'),
                'secret' => env('AWS_SECRET_ACCESS_KEY'),
            ],
        ]);
        $this->database = env('AWS_ATHENA_DATABASE');
    }

    public function fetchPaginatedData($page, $limit, $filter_column = null, $filter_value = null, $estado = null)
    {
        $start = (($page - 1) * $limit) + 1;

        $escaped_filter_value = str_replace("'", "''", $filter_value);

        // Creamos la consulta base
        $query = "
            WITH numbered_data AS (
                SELECT ROW_NUMBER() OVER (ORDER BY document_issue_date DESC) AS row_num, *
                FROM ".env('ATHENA_TABLE')."
                WHERE status = '".$estado."'
        ";

        // Si tenemos un filtro, agregamos la condición a la consulta
        if ($filter_column && $filter_value) {
            // Validamos si la columna es válida
            $valid_columns = ['client_ruc', 'document_number', 'document_location', 'client_name'];

            if (!in_array($filter_column, $valid_columns)) {
                throw new \Exception("Columna no válida para el filtro.");
            }

            // Concatenamos la condición del filtro
            $query .= " AND $filter_column LIKE '%$escaped_filter_value%' ";
        }

        $query .= ")
            SELECT *
            FROM numbered_data
            WHERE row_num BETWEEN {$start} AND {$start} + {$limit} - 1
        ";

        // Ejecutar la consulta
        $executionResponse = $this->client->startQueryExecution([
            'QueryString' => $query,
            'QueryExecutionContext' => [
                'Database' => $this->database,
            ],
            'ResultConfiguration' => [
                'OutputLocation' => env('AWS_ATHENA_OUTPUT'),
            ],
        ]);

        $executionId = $executionResponse['QueryExecutionId'];

        // Esperar que la consulta se complete
        do {
            $queryStatus = $this->client->getQueryExecution([
                'QueryExecutionId' => $executionId,
            ]);

            $status = $queryStatus['QueryExecution']['Status']['State'];

            if ($status === 'FAILED') {
                throw new \Exception('Query failed: ' . $queryStatus['QueryExecution']['Status']['StateChangeReason']);
            }

            if ($status === 'CANCELLED') {
                throw new \Exception('Query was cancelled.');
            }

            // Esperar antes de verificar nuevamente
            sleep(1);
        } while ($status !== 'SUCCEEDED');

        // Obtener los resultados
        $results = $this->client->getQueryResults([
            'QueryExecutionId' => $executionId,
        ]);

        // Obtener el total de registros
        $totalCount = $this->getTotalCount($filter_column, $filter_value, $estado);

        // Calcular información de paginación
        $totalPages = (int) ceil($totalCount / $limit);
        $currentPage = (int) $page;
        $from = $start;
        $to = min($start + $limit - 1, $totalCount);

        $previousPage = $currentPage > 1 ? $currentPage - 1 : null;
        $nextPage = $currentPage < $totalPages ? $currentPage + 1 : null;

        return [
            'data' => $results['ResultSet']['Rows'], // Datos de la consulta
            'pagination' => [
                'current_page' => $currentPage,
                'per_page' => (int) $limit,
                'total' => (int) $totalCount,
                'total_pages' => $totalPages,
                'from' => $from,
                'to' => $to,
                'previous_page' => $previousPage,
                'next_page' => $nextPage,
            ]
        ];
    }

    public function getTotalCount($filter_column = null, $filter_value = null, $status = null)
    {
        $escaped_filter_value = str_replace("'", "''", $filter_value);

        // Consulta para contar el total de registros
        $query = "SELECT COUNT(*) AS total FROM " . env('ATHENA_TABLE') ." WHERE status='".$status."'";

        // Si tenemos un filtro, agregamos la condición a la consulta
        if ($filter_column && $filter_value) {
            // Validamos si la columna es válida
            $valid_columns = ['client_ruc', 'document_number', 'document_location', 'client_name'];

            if (!in_array($filter_column, $valid_columns)) {
                throw new \Exception("Columna no válida para el filtro.");
            }

            // Concatenamos la condición del filtro
            $query .= " AND $filter_column LIKE '%$escaped_filter_value%' ";
        }
        
        // Ejecutar la consulta para contar el total de registros
        $executionResponse = $this->client->startQueryExecution([
            'QueryString' => $query,
            'QueryExecutionContext' => [
                'Database' => $this->database,
            ],
            'ResultConfiguration' => [
                'OutputLocation' => env('AWS_ATHENA_OUTPUT'),
            ],
        ]);

        $executionId = $executionResponse['QueryExecutionId'];

        // Esperar que la consulta se complete
        do {
            $queryStatus = $this->client->getQueryExecution([
                'QueryExecutionId' => $executionId,
            ]);

            $status = $queryStatus['QueryExecution']['Status']['State'];

            if ($status === 'FAILED') {
                throw new \Exception('Query failed: ' . $queryStatus['QueryExecution']['Status']['StateChangeReason']);
            }

            if ($status === 'CANCELLED') {
                throw new \Exception('Query was cancelled.');
            }

            // Esperar antes de verificar nuevamente
            sleep(1);
        } while ($status !== 'SUCCEEDED');

        // Obtener el resultado
        $results = $this->client->getQueryResults([
            'QueryExecutionId' => $executionId,
        ]);

        return $results['ResultSet']['Rows'][1]['Data'][0]['VarCharValue']; // Total de registros
    }

    // Función para obtener los primeros 5 registros de una columna dada
    public function getColumnMatches($column, $value)
    {
        // Crear la consulta dinámica usando la interpolación de valores en lugar de parámetros
        $sql = "
            SELECT DISTINCT {$column}
            FROM ".env('ATHENA_TABLE');

        // Ejecutar la consulta y devolver los resultados
        try {
            $rucs = $this->executeAthenaQuery($sql);
            // Mapear los resultados para convertirlos al formato { client_ruc: 'valor' }
            $formattedRucs = array_map(function ($ruc)  use ($column) {
                return [ $column  => $ruc];
            }, $rucs);
            return $formattedRucs;
        } catch (Exception $e) {
            throw new Exception('Error al ejecutar la consulta: ' . $e->getMessage());
        }
    }

    // Función para ejecutar la consulta Athena
    public function executeAthenaQuery($sql)
    {
        // Step 1: Start Query Execution
        $executionResponse = $this->client->startQueryExecution([
            'QueryString' => $sql,
            'QueryExecutionContext' => [
                'Database' => $this->database,
            ],
            'ResultConfiguration' => [
                'OutputLocation' => env('AWS_ATHENA_OUTPUT'),
            ],
        ]);

        $executionId = $executionResponse['QueryExecutionId'];

        // Step 2: Wait for Query to Complete
        do {
            $queryStatus = $this->client->getQueryExecution([
                'QueryExecutionId' => $executionId,
            ]);

            $status = $queryStatus['QueryExecution']['Status']['State'];

            if ($status === 'FAILED') {
                $errorMessage = $queryStatus['QueryExecution']['Status']['StateChangeReason'] ?? 'Unknown error';
                throw new \Exception('Query failed: ' . $errorMessage);
            }

            if ($status === 'CANCELLED') {
                throw new \Exception('Query was cancelled.');
            }

            // Wait for a second before checking again
            sleep(1);
        } while ($status !== 'SUCCEEDED');

        // Step 3: Fetch Results with Pagination
        $clientRucValues = [];
        $nextToken = null;

        do {
            // Construir la solicitud
            $queryParams = [
                'QueryExecutionId' => $executionId,
            ];

            if (!empty($nextToken)) {
                $queryParams['NextToken'] = $nextToken; // Agregar solo si hay un valor válido
            }

            $results = $this->client->getQueryResults($queryParams);

            foreach ($results['ResultSet']['Rows'] as $index => $row) {
                // Saltar la fila del encabezado solo en la primera página
                if ($nextToken === null && $index === 0) {
                    continue;
                }

                // Verificar si la fila contiene la clave esperada
                if (!isset($row['Data'][0]['VarCharValue'])) {
                    continue; // Saltar filas que no tienen datos válidos
                }

                // Asumir que 'client_ruc' es la primera columna
                $clientRucValues[] = $row['Data'][0]['VarCharValue'];
            }

            // Obtener el token para la siguiente página
            $nextToken = $results['NextToken'] ?? null;
        } while (!empty($nextToken));

        return $clientRucValues;
    }





    public function checkQueryStatus($executionId)
    {
        $response = $this->client->getQueryExecution([
            'QueryExecutionId' => $executionId,
        ]);

        return $response['QueryExecution']['Status']['State'];
    }

    public function getQueryResults($executionId)
    {
        $response = $this->client->getQueryResults([
            'QueryExecutionId' => $executionId,
        ]);

        return $response['ResultSet'];
    }

    public function queryAthena($sql)
    {
        // Start the query
        $response = $this->client->startQueryExecution([
            'QueryString' => $sql,
            'QueryExecutionContext' => [
                'Database' => $this->database,
            ],
            'ResultConfiguration' => [
                'OutputLocation' => env('AWS_ATHENA_OUTPUT'), // Replace with your bucket
            ],
        ]);

        // Wait for the query to complete
        $queryExecutionId = $response['QueryExecutionId'];
        $status = null;

        do {
            $result = $this->client->getQueryExecution(['QueryExecutionId' => $queryExecutionId]);
            $status = $result['QueryExecution']['Status']['State'];
            sleep(1);
        } while ($status === 'RUNNING');

        if ($status !== 'SUCCEEDED') {
            throw new \Exception('Athena query failed: ' . $status);
        }

        // Fetch the results
        $results = $this->client->getQueryResults([
            'QueryExecutionId' => $queryExecutionId,
        ]);

        return $results['ResultSet']['Rows'];
    }
}
