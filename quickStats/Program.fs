
// Learn more about F# at http://fsharp.org
// See the 'F# Tutorial' project for more help.

open System
open quickStats.SqlConn
open quickStats.Files
open quickStats.CSVBuilder

let getTime() =  ( System.DateTime.Now.ToString("dd-MM-yyyy HH:mm:ss")) + " > " 

let writeToConsole message = 
    printfn "%s%s" (getTime()) message

let runScriptAgainstSingleClient (clientName:String) (connectionString:string) outputPath= 
    async{
        writeToConsole (sprintf "running script against client %s" clientName)
        try
            match executeScript clientName connectionString loadSQLScript with
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
    
     
let runScriptAgainstAllClients path= 
    let conns = getConnections

    conns.Value 
    |> Seq.map (fun c -> runScriptAgainstSingleClient (c.Name) (c.ConnectionString) path )
    |> Async.Parallel  
    |> Async.RunSynchronously

    //|false-> printfn "no connection strings provided in the config file"

[<EntryPoint>]
let main argv = 
    runScriptAgainstAllClients "c:\svn" |> ignore
    0 // return an integer exit code
