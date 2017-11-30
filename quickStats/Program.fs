
// Learn more about F# at http://fsharp.org
// See the 'F# Tutorial' project for more help.

open System
open quickStats.SqlConn
open quickStats.Files



[<EntryPoint>]
let main argv = 
    //printConnections
    match executeScript "Server=EMILIYAN;Database=test;Integrated Security=true" loadSQLScript with//loadSQLScript with
    | Some a -> a |> printQueries
    | _ -> printfn "no results returned"


    0 // return an integer exit code
