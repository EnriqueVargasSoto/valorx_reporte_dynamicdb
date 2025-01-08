<?php

use App\Http\Controllers\Api\AthenaController;
use Illuminate\Http\Request;
use App\Http\Controllers\Api\ReportController;
use App\Http\Controllers\DynamoDBController;
use Illuminate\Support\Facades\Route;

// Route::get('/', function () {
//     return view('welcome');
// });

//Route::get('document/{key}', [DynamoDBController::class, 'documento'])->name('document');

Route::middleware('auth:sanctum')->get('/user', function (Request $request) {
    return $request->user();
});

Route::resource('reports', ReportController::class);

Route::post('document', [DynamoDBController::class, 'documento']);

Route::post('consulta-athena', [AthenaController::class, 'executeQuery']);

Route::get('/athena/data', [AthenaController::class, 'getPaginatedData']);

Route::get('/athena/column', [AthenaController::class, 'getColumnMatches']);  // Nueva ruta para buscar por columna

Route::get('guarda-nombre-clientes/{column_json}', [AthenaController::class, 'saveDataAsJson']);

Route::get('enviar-data-json', [AthenaController::class, 'sendJsonData']);

Route::post('json-client-search', [AthenaController::class, 'sendJsonDataSearch']);
