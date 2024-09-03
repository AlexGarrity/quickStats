namespace unitTests

open quickStats
open quickStats.SqlConn
open quickStats.Files
open quickStats.CSVBuilder
open System
open Xunit


type testQuickStats() =
    let result (queries:'T list option) = 
        match queries with 
            |Some q -> q.Length
            | None -> -1
    [<Fact>]
    let ``when generating query without params should generate empty query``() =
        let config = quickStats.SqlConn.getConfig ()
        let conns = Map.toSeq config.ConnectionStrings
        let length  =  Seq.length (conns)
        Assert.Equal(3, length)

    [<Fact>]
    let ``check if sql script was loaded correctly``() =
        let expectedString = """Use test  Declare @color varchar(50) = 'blue';  with cte_yesNo as( select * from ( Select 1 as id, 'yes' as'answer' """+
                             """union all Select 2 as id, 'no' as 'answer' ) as d ) Select a1.id, count(*) as [countOfRows] from apples a1 """+
                             """inner join apples a2 on a1.id = a2.id left join cte_yesNo yn on yn.id = a2.id where a1.color = @color group by a1.id having count(*) = 1 """+
                             """select * from apples"""
        Assert.Equal(expectedString, Files.loadSQLScript "C:\\svn\\test.sql")
    
    [<Fact>]
    let ``test executeScript with single select statement``() =
        let queries =  executeScript "Emiliyan" "Server=EMILIYAN;Database=test;Integrated Security=true" (SQLScript("""Use Test Select * from apples"""))
        Assert.Equal(1,(result queries))

    [<Fact>]
    let ``test executeScript with two select statements``() =
        let queries =  executeScript "Emiliyan" "Server=EMILIYAN;Database=test;Integrated Security=true" (SQLScript(loadSQLScript "C:\\svn\\test.sql"))
        Assert.Equal(2,(result queries))

    [<Fact>]
    let ``test executeScript with two select statements, one of which returns not results``() =
        let queries =  executeScript "Emiliyan" "Server=EMILIYAN;Database=test;Integrated Security=true" (SQLScript("""Use Test Select * from apples Select * from Empty"""))


        Assert.Equal(2,(result queries))

    [<Fact>]
    let ``test executeScript with no select statements``() =
        let queries =  executeScript "Emiliyan" "Server=EMILIYAN;Database=test;Integrated Security=true" (SQLScript("""Use Test"""))
        Assert.Equal(-1,(result queries))
    
    [<Fact>]
    let ``test building CSV row with no special characters``() =
        
        let a = Cell("a")
        let b = Cell("b")
        let c = Cell("c")
        let row = ResultRow([c; b; a])
        let result = buildCSVrow (Cell("Emiliyan")) row 
        Assert.Equal("Emiliyan,a,b,c",result)
    
    [<Fact>]
    let ``test building CSV row with comma and double quotes``() =
        
        let a = Cell("a\"")
        let b = Cell("b,b")
        let c = Cell("c")
        let row = ResultRow([c; b; a])
        let result = buildCSVrow (Cell("Emiliyan")) row 
        Assert.Equal("Emiliyan,\"a\"\"\",\"b,b\",c",result)

    [<Fact>]
    let ``test building CSV row with all special characters``() =
        let a = Cell("a\"")
        let b = Cell("b,b")
        let c = Cell("c\r")
        let d = Cell("d\n\",")
        let row = ResultRow([d; c; b; a])
        let result = buildCSVrow (Cell("Emiliyan")) row 
        Assert.Equal("Emiliyan,\"a\"\"\",\"b,b\",\"c\r\",\"d\n\"\",\"",result)

    [<Fact>]
    let ``test building CSV row with empty cells``() =
        let a = Cell("")
        let b = Cell("b,b")
        let c = Cell("c")
        let row = ResultRow([a; b; c])
        let headers = QueryHeaders([a; a; a])
        let result = buildCSVrow (Cell("Emiliyan")) row 
        let headersResult = buildCSVrowHeaders headers
        Assert.Equal("client name,,,",headersResult)
    
    let rec compareLists (expected:(int * string list) list) (actual:(int * string list) list) = 
        match expected with
        | [] -> 
            match actual with
            | [] -> Assert.True(true)
            | head::tail -> Assert.True(false, (sprintf "the actual result contains additional elements: %A" actual))
        | expectedHead::expectedTail -> 
            match actual with
            | [] -> Assert.True(false, "the actual result does not contain enough elements")
            | actualHead::actualTail -> 
                Assert.Equal(expectedHead, actualHead)
                compareLists expectedTail actualTail


    [<Fact>]
    let ``test grouping files based on name``() = 
        let files = [|"client1_query_1.csv"
                      "client1_query_2.csv"
                      "client1_query_3.csv"
                      "client2_query_1.csv"
                      "client2_query_2.csv"
                    |]
        
        let expectedResult =   [(1, ["client1_query_1.csv"; "client2_query_1.csv"]);
                               (2, ["client1_query_2.csv"; "client2_query_2.csv"]);
                               (3, ["client1_query_3.csv"])]

        let groups = groupCSVFileNames files

        if expectedResult.Length <> groups.Length then Assert.True(false, "the length should be the same")
        else compareLists expectedResult groups