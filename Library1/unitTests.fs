namespace unitTests

open quickStats
open System
open Xunit
open quickStats.SqlConn
open quickStats.Files
open quickStats.CSVBuilder

type testQuickStats() =
    let result (queries:'T list option) = 
        match queries with 
            |Some q -> q.Length
            | None -> -1
    [<Fact>]
    let ``when generating query without params should generate empty query``() =
        let conns = quickStats.SqlConn.getConnections.Value
        let length  =  Seq.length (conns)
        Assert.Equal(3, length)

    [<Fact>]
    let ``check if sql script was loaded correctly``() =
        let expectedString = """Use test  Declare @color varchar(50) = 'blue';  with cte_yesNo as( select * from ( Select 1 as id, 'yes' as'answer' """+
                             """union all Select 2 as id, 'no' as 'answer' ) as d ) Select a1.id, count(*) as [countOfRows] from apples a1 """+
                             """inner join apples a2 on a1.id = a2.id left join cte_yesNo yn on yn.id = a2.id where a1.color = @color group by a1.id having count(*) = 1 """+
                             """select * from apples"""
        Assert.Equal(expectedString, Files.loadSQLScript)
    
    [<Fact>]
    let ``test executeScript with single select statement``() =
        let queries =  executeScript "Server=EMILIYAN;Database=test;Integrated Security=true" """Use Test Select * from apples"""
        Assert.Equal(1,(result queries))

    [<Fact>]
    let ``test executeScript with two select statements``() =
        let queries =  executeScript "Server=EMILIYAN;Database=test;Integrated Security=true" loadSQLScript
        Assert.Equal(2,(result queries))

    [<Fact>]
    let ``test executeScript with two select statements, one of which returns not results``() =
        let queries =  executeScript "Server=EMILIYAN;Database=test;Integrated Security=true" """Use Test Select * from apples Select * from Empty"""


        Assert.Equal(2,(result queries))

    [<Fact>]
    let ``test executeScript with no select statements``() =
        let queries =  executeScript "Server=EMILIYAN;Database=test;Integrated Security=true" """Use Test"""
        Assert.Equal(-1,(result queries))
    
    [<Fact>]
    let ``test building CSV row with no special characters``() =
        
        let a = Cell("a")
        let b = Cell("b")
        let c = Cell("c")
        let row = ResultRow([a; b; c])
        let result = buildCSVrow row 
        Assert.Equal("a,b,c",result)
    
    [<Fact>]
    let ``test building CSV row with comma and double quotes``() =
        
        let a = Cell("a\"")
        let b = Cell("b,b")
        let c = Cell("c")
        let row = ResultRow([a; b; c])
        let result = buildCSVrow row 
        Assert.Equal("\"a\"\"\",\"b,b\",c",result)

    [<Fact>]
    let ``test building CSV row with all special characters``() =
        let a = Cell("a\"")
        let b = Cell("b,b")
        let c = Cell("c\r")
        let d = Cell("d\n\",")
        let row = ResultRow([a; b; c; d])
        let result = buildCSVrow row 
        Assert.Equal("\"a\"\"\",\"b,b\",\"c\r\",\"d\n\"\",\"",result)

    [<Fact>]
    let ``test building CSV row with empty cells``() =
        let a = Cell("")
        let b = Cell("b,b")
        let c = Cell("c")
        let row = ResultRow([a; b; c])
        let headers = QueryHeaders([a; a; a])
        let result = buildCSVrow row 
        let headersResult = buildCSVrowHeaders headers
        Assert.Equal(",,",headersResult)
        