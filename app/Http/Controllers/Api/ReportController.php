<?php

namespace App\Http\Controllers\Api;

use App\Http\Controllers\Controller;
use Illuminate\Http\Request;

use App\Services\DynamoDBService;

class ReportController extends Controller
{
    private $dynamoDBService;

    public function __construct(DynamoDBService $dynamoDBService)
    {
        $this->dynamoDBService = $dynamoDBService;
    }
    /**
     * Display a listing of the resource.
     */
    public function index(Request $request)
    {
        $limit = 10; // Número de documentos por página
        $lastEvaluatedKey = $request->input('lastEvaluatedKey'); // Obtener la clave de la siguiente página (si existe)
        $previousEvaluatedKey = $request->input('previousEvaluatedKey'); // Obtener la clave de la página anterior (si existe)
        $searchTerm = $request->input('searchTerm'); // Obtener el término de búsqueda

        // Llamar al servicio de DynamoDB para obtener los documentos paginados
        $result = $this->dynamoDBService->fetchDocuments($limit, $lastEvaluatedKey, $previousEvaluatedKey, $searchTerm);

        // Verificar si hay más páginas
        $hasNextPage = $result['hasNextPage'];
        $nextEvaluatedKey = $result['lastEvaluatedKey'];
        $hasPreviousPage = $result['hasPreviousPage'];

        // Construir la respuesta de paginación
        return response()->json([
            'data' => [
                'items' => $result['items'],
                'hasNextPage' => $hasNextPage,
                'nextEvaluatedKey' => $nextEvaluatedKey, // Este es el valor que enviarás para la siguiente solicitud
                'hasPreviousPage' => $hasPreviousPage, // Indicar si hay una página anterior
                'previousEvaluatedKey' => $result['previousEvaluatedKey'], // Este es el valor que enviarás para la página anterior
            ],
        ]);
    }

    /**
     * Show the form for creating a new resource.
     */
    public function create()
    {
        //
    }

    /**
     * Store a newly created resource in storage.
     */
    public function store(Request $request)
    {
        //
    }

    /**
     * Display the specified resource.
     */
    public function show(string $id)
    {
        //
    }

    /**
     * Show the form for editing the specified resource.
     */
    public function edit(string $id)
    {
        //
    }

    /**
     * Update the specified resource in storage.
     */
    public function update(Request $request, string $id)
    {
        //
    }

    /**
     * Remove the specified resource from storage.
     */
    public function destroy(string $id)
    {
        //
    }
}
