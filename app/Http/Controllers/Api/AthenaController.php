<?php

namespace App\Http\Controllers\Api;

use App\Http\Controllers\Controller;
use Aws\Athena\AthenaClient;
use Aws\Exception\AwsException;
use Illuminate\Http\Request;

class AthenaController extends Controller
{
    //
    protected $athenaClient;

    public function __construct()
    {
        $this->athenaClient = new AthenaClient([
            'version' => 'latest',
            'region'  => env('AWS_DEFAULT_REGION'),
            'credentials' => [
                'key'    => env('AWS_ACCESS_KEY_ID'),
                'secret' => env('AWS_SECRET_ACCESS_KEY'),
            ],
        ]);
    }

    public function executeQuery(Request $request)
    {
        $query = $request->input('query');  // Obtén la consulta SQL desde la solicitud

        if (empty($query)) {
            return response()->json(['error' => 'Query is required'], 400);
        }

        try {
            $result = $this->executeAthenaQuery($query);
            return response()->json($result);
        } catch (AwsException $e) {
            return response()->json(['error' => $e->getMessage()], 500);
        }
    }

    private function executeAthenaQuery($query)
    {
        // Initiate the query execution
        $result = $this->athenaClient->startQueryExecution([
            'QueryString' => $query,
            'QueryExecutionContext' => [
                'Database' => 'document_apdayc_stage' // Reemplaza con tu base de datos de Athena
            ],
            'ResultConfiguration' => [
                'OutputLocation' => 's3://datalake-cls-418295718835-artifactory-s3/athena_sql/ ', // Reemplaza con la ruta de salida en tu bucket S3
            ],
        ]);

        $queryExecutionId = $result['QueryExecutionId'];
        dd($queryExecutionId);
        // Espera hasta que se complete la consulta
        $this->waitForQueryToFinish($queryExecutionId);

        // Obtener los resultados de la consulta
        $queryResult = $this->athenaClient->getQueryResults([
            'QueryExecutionId' => $queryExecutionId
        ]);

        // Procesar los resultados
        $rows = $queryResult['ResultSet']['Rows'];
        $result = [];
        foreach ($rows as $row) {
            $result[] = $this->processRow($row);
        }

        return $result;
    }

    private function processRow($row)
    {
        // Convertir los datos de Athena a un formato legible
        $data = [];
        foreach ($row['Data'] as $index => $cell) {
            // Asumimos que los valores en las celdas son cadenas
            $data[] = $cell['VarCharValue'] ?? null;
        }
        return $data;
    }

    private function waitForQueryToFinish($queryExecutionId)
    {
        while (true) {
            $status = $this->athenaClient->getQueryExecution([
                'QueryExecutionId' => $queryExecutionId,
            ])['QueryExecution']['Status']['State'];

            if ($status == 'SUCCEEDED') {
                break;
            }

            if ($status == 'FAILED' || $status == 'CANCELLED') {
                throw new \Exception("Query failed to run with status: " . $status);
            }

            // Espera 5 segundos antes de comprobar de nuevo
            sleep(5);
        }
    }
}
