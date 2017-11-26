namespace unitTesting

open quickStats
open System
open Xunit

type testQuickStats() =
    
    [<Fact>]
    let ``when generating query without params should generate empty query``() =
        let conns = quickStats.conn.getConnections()
        let length  =  Seq.length (conns)
        Assert.Equal(2, length)