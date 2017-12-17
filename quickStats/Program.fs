
// Learn more about F# at http://fsharp.org
// See the 'F# Tutorial' project for more help.

open System
open quickStats.SqlConn
open quickStats.Files
open quickStats.CSVBuilder

let getTime() =  ( System.DateTime.Now.ToString("dd-MM-yyyy HH:mm:ss")) + " > " 

let runScriptAgainstSingleClient (clientName:String) (connectionString:string) outputPath= 
    printfn "%srunning script against client %s" (getTime()) clientName
    match executeScript clientName connectionString loadSQLScript with
    | Some a -> 
        generateCSVFilesForClient a outputPath clientName
    | _ -> printfn "no results returned to process"


let runScriptAgainstAllClients path= 
    let conns = getConnections

    conns.Value 
    |> Seq.iter (fun c -> runScriptAgainstSingleClient (c.Name) (c.ConnectionString) path)

    //|false-> printfn "no connection strings provided in the config file"

[<EntryPoint>]
let main argv = 
    runScriptAgainstAllClients "c:\svn"

    0 // return an integer exit code
