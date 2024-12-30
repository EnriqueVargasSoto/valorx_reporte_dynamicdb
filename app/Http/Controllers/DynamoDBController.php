<?php

namespace App\Http\Controllers;

use Illuminate\Http\Request;

use App\Services\DynamoDBService;
use Illuminate\Http\JsonResponse;

class DynamoDBController extends Controller
{
    //
    private $dynamoDBService;

    public function __construct(DynamoDBService $dynamoDBService)
    {
        $this->dynamoDBService = $dynamoDBService;
    }

    public function index(Request $request)
    {
        // $data = $this->dynamoDBService->fetchDocuments(10); // Obtén los datos (puedes ajustar el límite)
        // return view('welcome', ['data' => $data]);

        $limit = 10; // Número de documentos por página
        $lastEvaluatedKey = $request->input('lastEvaluatedKey'); // Obtener la clave de la siguiente página (si existe)

        // Llamar al servicio de DynamoDB para obtener los documentos paginados
        $result = $this->dynamoDBService->fetchDocuments($limit, $lastEvaluatedKey);

        // Obtenemos el número total de páginas basándonos en el límite
       // $documents = $result['documents'];
        $hasNextPage = $result['hasNextPage'];
        $lastEvaluatedKey = $result['lastEvaluatedKey'];

        // Si deseas contar el número total de páginas, puedes calcularlo como:
       // $totalItems = count($documents); // Aquí debes modificar este valor si es necesario contar todos los items
       // $totalPages = ceil($totalItems / $limit);

        return view('welcome', [
            'items' => $result['items'],
            'lastEvaluatedKey' => $result['lastEvaluatedKey']
        ]);
    }

    public function listDocuments(): JsonResponse
    {
        try {
            $result = $this->dynamoDBService->fetchDocuments(10); // Limita a 10 resultados

            return response()->json([
                'success' => true,
                'data' => $result['items'],
                'lastKey' => $result['lastKey'],
            ]);
        } catch (\Exception $e) {
            return response()->json([
                'success' => false,
                'message' => $e->getMessage(),
            ], 500);
        }
    }
}
