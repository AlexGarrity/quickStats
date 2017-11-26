
// Learn more about F# at http://fsharp.org
// See the 'F# Tutorial' project for more help.

open System
open quickStats.conn



[<EntryPoint>]
let main argv = 
    printConnections
    let d = getConnections()
    printfn "%d" (Seq.length d)
    testConnections
    0 // return an integer exit code
