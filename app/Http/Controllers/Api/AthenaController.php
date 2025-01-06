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
            $status = $this->athenaClient->getQueryExecution([
                'QueryExecutionId' => $queryExecutionId,
            ])['QueryExecution']['Status']['State'];

            if ($status == 'SUCCEEDED') {
                break;
            }

            if ($status == 'FAILED' || $status == 'CANCELLED') {
                throw new \Exception("Query failed to run with status: " . $status);
            }

            // Wait 5 seconds before checking again
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
}
