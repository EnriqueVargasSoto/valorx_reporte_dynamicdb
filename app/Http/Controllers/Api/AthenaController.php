<?php

namespace App\Http\Controllers\Api;

use App\Http\Controllers\Controller;
use Aws\Athena\AthenaClient;
use Aws\Exception\AwsException;
use Illuminate\Http\Request;
use App\Services\AthenaService;
use Exception;

class AthenaController extends Controller
{
    //
    protected $athenaClient;

    // public function __construct()
    // {
    //     $this->athenaClient = new AthenaClient([
    //         'version' => 'latest',
    //         'region'  => env('AWS_DEFAULT_REGION'),
    //         'credentials' => [
    //             'key'    => env('AWS_ACCESS_KEY_ID'),
    //             'secret' => env('AWS_SECRET_ACCESS_KEY'),
    //         ],
    //     ]);
    // }

    protected $athenaService;

    public function __construct(AthenaService $athenaService)
    {
        $this->athenaService = $athenaService;
    }

    // General endpoint to execute a query
    public function executeQuery(Request $request)
    {
        $query = $request->input('query');

        if (!$query) {
            return response()->json(['error' => 'Query is required'], 400);
        }

        try {
            $result = $this->runAthenaQuery($query);
            return response()->json($result);
        } catch (\Exception $e) {
            return response()->json(['error' => $e->getMessage()], 500);
        }
    }

    private function runAthenaQuery($query)
    {
        $this->athenaClient = new AthenaClient([
                     'version' => 'latest',
                     'region'  => env('AWS_DEFAULT_REGION'),
                     'credentials' => [
                         'key'    => env('AWS_ACCESS_KEY_ID'),
                         'secret' => env('AWS_SECRET_ACCESS_KEY'),
                     ],
                 ]);
        // Start query execution
        $result = $this->athenaClient->startQueryExecution([
            'QueryString' => $query,
            'QueryExecutionContext' => [
                'Database' => env('AWS_ATHENA_DATABASE'), // Replace with your database
            ],
            'ResultConfiguration' => [
                'OutputLocation' => env('AWS_ATHENA_OUTPUT'), // Replace with your S3 output path
            ],
        ]);

        $queryExecutionId = $result['QueryExecutionId'];

        // Wait for query to finish
        $this->waitForQueryToFinish($queryExecutionId);

        // Get query results
        $queryResult = $this->athenaClient->getQueryResults([
            'QueryExecutionId' => $queryExecutionId,
        ]);

        // Parse the results
        return $this->parseQueryResults($queryResult);
    }

    private function waitForQueryToFinish($queryExecutionId)
    {
        while (true) {
            $execution = $this->athenaClient->getQueryExecution([
                'QueryExecutionId' => $queryExecutionId,
            ]);

            $status = $execution['QueryExecution']['Status']['State'];
            $reason = $execution['QueryExecution']['Status']['StateChangeReason'] ?? 'No reason provided';

            if ($status == 'SUCCEEDED') {
                break;
            }

            if ($status == 'FAILED' || $status == 'CANCELLED') {
                throw new \Exception("Query failed to run with status: $status. Reason: $reason");
            }

            sleep(5);
        }
    }

    private function parseQueryResults($queryResult)
    {
        $rows = $queryResult['ResultSet']['Rows'];
        $result = [];

        // Skip the first row (headers)
        for ($i = 1; $i < count($rows); $i++) {
            $data = array_map(function ($item) {
                return $item['VarCharValue'] ?? null;
            }, $rows[$i]['Data']);

            $result[] = $data;
        }

        return $result;
    }

    public function getPaginatedData(Request $request)
    {
        $page = (int) $request->input('page', 1);
        $limit = (int) $request->input('limit', 10);
        //$client_ruc = $request->input('client_ruc', null);  // Nuevo filtro opcional
        // Se obtiene el filtro, puede ser cualquiera de las columnas: client_ruc, document_number, document_location, client_name
        $filter_column = $request->input('filter_column', null);  // La columna por la cual filtrar
        $filter_value = $request->input('filter_value', null);    // El valor para filtrar

        try {
            $paginationResults = $this->athenaService->fetchPaginatedData($page, $limit, $filter_column, $filter_value);

            // Procesar los datos para que estén en un formato útil
            $data = [];
            foreach ($paginationResults['data'] as $index => $row) {
                if ($index === 0) {
                    continue; // Omitir la primera fila de encabezado
                }

                $data[] = array_combine(
                    array_map(fn ($col) => $col['VarCharValue'] ?? '', $paginationResults['data'][0]['Data']),
                    array_map(fn ($col) => $col['VarCharValue'] ?? '', $row['Data'])
                );
            }

            return response()->json([
                'success' => true,
                'data' => $data,
                'pagination' => $paginationResults['pagination'],
            ]);
        } catch (\Exception $e) {
            return response()->json([
                'success' => false,
                'message' => $e->getMessage(),
            ], 500);
        }
    }

    // Nueva función para obtener los primeros 5 registros de una columna dada
    public function getColumnMatches(Request $request)
    {
        $column = $request->query('column'); // Nombre de la columna
        $value = $request->query('value'); // Valor a buscar por aproximación

        // Verificar que la columna y el valor sean proporcionados
        if (empty($column) ) {
            return response()->json([
                'success' => false,
                'message' => 'Se debe proporcionar el nombre de la columna y el valor de búsqueda.',
            ], 400);
        }

        // Intentar obtener los resultados
        try {
            $results = $this->athenaService->getColumnMatches($column, $value);
            return response()->json(['data' => $results]);
        } catch (Exception $e) {
            return response()->json([
                'success' => false,
                'message' => 'Error al obtener los resultados: ' . $e->getMessage(),
            ], 500);
        }
    }
}
