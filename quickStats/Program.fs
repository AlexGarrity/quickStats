
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
    writeToConsole (sprintf "running script against client %s" clientName)
    try
        match executeScript clientName connectionString loadSQLScript with
        | Some a -> 
            generateCSVFilesForClient a outputPath clientName
            writeToConsole (sprintf "output files generated in \"%s\"" outputPath)
        | _ -> writeToConsole "no results returned to process"
    with
        e -> 
             writeToConsole "failed with exception:"
             Console.ForegroundColor <- ConsoleColor.DarkRed
             writeToConsole (sprintf "%s" (e.ToString()) )
             Console.ResetColor()
             writeToConsole (sprintf "skipping client %s" clientName)
             
    
    


let runScriptAgainstAllClients path= 
    let conns = getConnections

    conns.Value 
    |> Seq.iter (fun c -> runScriptAgainstSingleClient (c.Name) (c.ConnectionString) path)

    //|false-> printfn "no connection strings provided in the config file"

[<EntryPoint>]
let main argv = 
    runScriptAgainstAllClients "c:\svn"
    0 // return an integer exit code
