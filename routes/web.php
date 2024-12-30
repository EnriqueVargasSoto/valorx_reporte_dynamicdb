<?php

use App\Http\Controllers\DynamoDBController;
use Illuminate\Support\Facades\Route;

// Route::get('/', function () {
//     return view('welcome');
// });

Route::get('/', [DynamoDBController::class,'index']);

Route::get('list', [DynamoDBController::class,'listDocuments']);
