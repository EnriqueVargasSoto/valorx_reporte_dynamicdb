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

    public function index()
    {
        $data = $this->dynamoDBService->fetchDocuments(10); // Obtén los datos (puedes ajustar el límite)
        return view('welcome', ['data' => $data]);
    }

    public function listDocuments(): JsonResponse
    {
        try {
            $result = $this->dynamoDBService->listItems(10); // Limita a 10 resultados

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
