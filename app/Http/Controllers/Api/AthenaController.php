<?php

namespace App\Http\Controllers\Api;

use App\Http\Controllers\Controller;
use Aws\Athena\AthenaClient;
use Aws\Exception\AwsException;
use Illuminate\Http\Request;
use App\Services\AthenaService;
use Exception;
use Illuminate\Support\Facades\Storage;

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

    public function saveDataAsJson($column_json)
    {
        $column = $column_json; // Nombre de la columna

        if (empty($column) ) {
            return response()->json([
                'success' => false,
                'message' => 'Se debe proporcionar el nombre de la columna y el valor de búsqueda.',
            ], 400);
        }

        // Verificar que la columna y el valor sean proporcionados
        if (empty($column) ) {
            return response()->json([
                'success' => false,
                'message' => 'Se debe proporcionar el nombre de la columna y el valor de búsqueda.',
            ], 400);
        }
        // 1. Obtener los datos desde Athena
        $data = $this->athenaService->getColumnMatches($column, null);

        // 2. Convertir los datos a JSON
        $jsonData = json_encode($data, JSON_PRETTY_PRINT);

        // 3. Definir el nombre del archivo
        $fileName = 'data/athena_data_'.$column_json.'.json'; // Puedes personalizar la ruta y el nombre

        // 4. Guardar el archivo en el sistema de archivos de Laravel
        Storage::disk('local')->put($fileName, $jsonData);

        return response()->json(['message' => 'Datos guardados en JSON correctamente', 'file' => $fileName]);
    }

    public function sendJsonData()
    {
        // 1. Leer el contenido del archivo JSON
        $fileName = 'data/athena_data_client_name.json'; // Asegúrate de usar el nombre y ruta correctos

        // Verificar si el archivo existe
        if (!Storage::disk('local')->exists($fileName)) {
            return response()->json(['error' => 'Archivo no encontrado'], 404);
        }

        // Obtener el contenido del archivo
        $jsonContent = Storage::disk('local')->get($fileName);

        // 2. Decodificar el JSON si es necesario (opcional)
        $data = json_decode($jsonContent, true); // Devuelve un array asociativo

        // 3. Enviar la respuesta JSON
        return response()->json($data);
    }

    public function sendJsonDataSearch(Request $request)
    {
        // 1. Leer el contenido del archivo JSON
        $fileName = 'data/athena_data_'.$request->input('column_json').'.json'; // Asegúrate de usar el nombre y ruta correctos

        // Verificar si el archivo existe
        if (!Storage::disk('local')->exists($fileName)) {
            return response()->json(['error' => 'Archivo no encontrado'], 404);
        }

        // Obtener el contenido del archivo
        $jsonContent = Storage::disk('local')->get($fileName);

        // 2. Decodificar el JSON
        $data = json_decode($jsonContent, true); // Devuelve un array asociativo

        // 3. Obtener el parámetro de búsqueda desde la solicitud
        $searchString = $request->input('searchString'); // Obtén el string de búsqueda desde el cuerpo de la solicitud

        // Si no se proporciona el string de búsqueda, devolver todos los datos
        if (empty($searchString)) {
            return response()->json($data);
        }

        // 4. Filtrar los datos que coincidan con el parámetro de búsqueda
        $filteredData = array_filter($data, function($item) use ($searchString, $request) {
            // Ajusta esto según la clave por la cual deseas buscar. Por ejemplo, 'name' o 'description'.
            return strpos(strtolower($item[$request->input('column_json')]), strtolower($searchString)) !== false;
        });

        // 5. Si no se encuentran coincidencias
        if (empty($filteredData)) {
            return response()->json(['message' => 'No se encontraron coincidencias'], 200);
        }

        // 6. Enviar los resultados filtrados como respuesta
        return response()->json(array_values($filteredData)); // `array_values()` para reindexar el array
    }

}
