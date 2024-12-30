<?php

use App\Http\Controllers\DynamoDBController;
use Illuminate\Support\Facades\Route;

// Route::get('/', function () {
//     return view('welcome');
// });

Route::get('/', [DynamoDBController::class,'index'])->name('/');

Route::get('list', [DynamoDBController::class,'listDocuments']);

//Route::get('document/{key}', [DynamoDBController::class, 'documento'])->name('document');

Route::post('document', [DynamoDBController::class, 'documento'])->name('document');
