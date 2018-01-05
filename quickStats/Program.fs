
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
    
     
let runScriptAgainstAllClients path script= 
    let conns = getConnections
    let sqlScript = SQLScript(loadSQLScript script)
    conns.Value 
    |> Seq.map (fun c -> runScriptAgainstSingleClient (c.Name) (c.ConnectionString) path sqlScript)
    |> Async.Parallel  
    |> Async.RunSynchronously
    


[<EntryPoint>]
let main argv = 
    let arguments = parseCommandLineArguments <| Array.toList argv
    
    match arguments.outputPath, arguments.pathToSQLScript with
    | Some path, Some script -> 
        
        runScriptAgainstAllClients path script |> ignore
        printfn "Merging all CSV files"
        combineAllCSVFiles path

    | _, _ -> 
        "expected usage:" +
        "\n--outputPath <existing path where all CSV files will be generated>" + 
        "\n--pathToSQLScript <path to SQL file>"
        |> printfn "%s"
                
    0 // return an integer exit code
