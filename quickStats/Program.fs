﻿
// Learn more about F# at http://fsharp.org
// See the 'F# Tutorial' project for more help.

open System
open quickStats.SqlConn
open quickStats.Files
open quickStats.CSVBuilder
open quickStats.ParseCommandLineArgs

let getTime() =  ( System.DateTime.Now.ToString("dd-MM-yyyy HH:mm:ss")) + " > " 

let writeToConsole message = 
    printfn "%s%s" (getTime()) message

let runScriptAgainstSingleClient (clientName:String) (connectionString:string) outputPath sqlScript= 
    async{
        writeToConsole (sprintf "running script against client %s" clientName)
        try
            match executeScript clientName connectionString sqlScript with
            | Some a -> 
                generateCSVFilesForClient a outputPath clientName
                writeToConsole (sprintf "output files for client %s generated in \"%s\"" clientName outputPath)
            | _ -> writeToConsole (sprintf "The query was ran successfully against client %s but no results were returned to process" clientName)
        with
            e -> 
                 writeToConsole (sprintf "Running script against client %s failed with exception:" clientName)
                 Console.ForegroundColor <- ConsoleColor.DarkRed
                 writeToConsole (sprintf "%s" (e.ToString()) )
                 Console.ResetColor()
                 writeToConsole (sprintf "skipping client %s" clientName)  
    }
    
     
let runScriptAgainstAllClients configPath outputPath scriptPath = 
    let config = getConfig configPath
    let sqlScript = SQLScript(loadSQLScript scriptPath)
    config.ConnectionStrings 
    |> Map.toSeq
    |> Seq.map (fun (name, connectionString) -> runScriptAgainstSingleClient (name) (connectionString) outputPath sqlScript)
    |> Async.Parallel  
    |> Async.RunSynchronously
    

[<EntryPoint>]
let main argv = 
    let arguments = parseCommandLineArguments <| Array.toList argv
    
    match arguments.configPath, arguments.outputPath, arguments.pathToSQLScript with
    | Some configPath, Some outputPath, Some scriptPath -> 
        runScriptAgainstAllClients configPath outputPath scriptPath |> ignore
        printfn "Merging all CSV files producing a single CSV file per query"
        combineAllCSVFiles outputPath

    | _ -> 
        "expected usage:
--config <path to config.json file>
--outputPath <existing path where all CSV files will be generated>
--pathToSQLScript <path to SQL file>"
        |> printfn "%s"
                
    0 // return an integer exit code
