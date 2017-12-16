namespace quickStats


open System.Configuration
open FSharp.Configuration
    module SqlConn =
        
        open System.Data
        open System.Data.SqlClient
        open System.Collections.Generic
        
        

        type Cell = 
            |Cell of string
            member this.getData = 
                match this with
                | Cell s -> s

        type ResultRow = 
            |ResultRow of Cell list
            member this.getRow = 
                match this with
                | ResultRow r -> r
                
        type QueryHeaders = 
            |QueryHeaders of Cell list
            member this.getQueryHeaders =
                match this with
                | QueryHeaders q -> q
        
        type queryResults = {headers:QueryHeaders; rows:ResultRow list}
        
        
        ///<summary>load all connections from app.config</summary>
        ///<returns>Sequence of all connections listed in the app.config</returns>
        let getConnections = 
            lazy
                seq { for conn in ConfigurationManager.ConnectionStrings -> 
                        match conn.ConnectionString with
                        | "" -> None
                        | x -> Some conn}
                |> Seq.choose id
            
        //let ListToString (l:string list) = 
        //    let mutable s = ""
        //    l |> Seq.iter (fun f -> s <- s + " " + f )
        //    s
        

        let rec printQueryRows rows = 
            match rows with
            | [] -> ()
            | row::tail -> 
                    printfn "%A" row
                    printQueryRows tail

        let printQuery q = 
            printfn "%A" q.headers
            printQueryRows q.rows
            
        
        let rec printQueries q =
            match q with
            | [] -> ()
            | a::tail -> printQuery a
                         printQueries tail 
                   

        let rec getQueryHeaders headers (reader:SqlDataReader) i = 
            match i>=0 && reader.FieldCount > i with 
            | false -> headers
            | true -> 
                //printfn "count %i and index %i " (reader.FieldCount) i
                getQueryHeaders ( Cell( reader.GetName(i) )::headers) reader (i+1)

        let rec getResultRow columns (reader:SqlDataReader) i = 
            match i>=0 && reader.FieldCount > i with 
            | false -> columns
            | true -> getResultRow (Cell(reader.GetProviderSpecificValue(i).ToString())::columns) reader (i+1)

        let rec getAllResultRowsForQuery rows (reader:SqlDataReader)=
            match reader.Read() with
            | false -> rows
            | true -> getAllResultRowsForQuery (ResultRow(getResultRow [] reader 0)::rows) reader

        let rec processAllQueries queries (reader:SqlDataReader) hasMore = 
            
            match hasMore with
            | false -> 
                 match queries with 
                    | [] -> None
                    | q -> Some q
            | true -> 
                let qr = { headers =QueryHeaders( getQueryHeaders [] reader 0 )
                           rows = (getAllResultRowsForQuery [] reader) }
                    
                processAllQueries (match qr.headers with
                                    | QueryHeaders([]) -> queries
                                    | a -> qr::queries) reader (reader.NextResult())
            
      
            

        ///<summary>Execute a sql script and return results as list of linked lists</summary>
        ///<param name="connectionString">The connection string which will be used</param>
        ///<param name="str">the sql script which will be executed using the specified connection string</param>
        ///<returns>Sequence of linked lists</returns>
        let executeScript connectionString str = 
            let sqlConnection = new SqlConnection(connectionString);
            
            let cmd = new SqlCommand();
            cmd.CommandText <- str         
            cmd.CommandType <- CommandType.Text
            cmd.Connection <- sqlConnection
            cmd.CommandTimeout <- 300
            sqlConnection.Open()
            let reader = cmd.ExecuteReader()

            let queries = processAllQueries [] reader true
            

            reader.Dispose()
            reader.Close()
            sqlConnection.Close()
            queries

        let printConnections = 
            lazy
                let conn = getConnections.Value
                conn |> Seq.iter (fun s-> printfn "%s" (s.ToString()) )   
            

    module Files = 
        open System.IO

        let loadSQLScript = 
            File.ReadAllLines(AppSettings<"app.config">.SqlFilePathAndName)
            |> (fun s -> 
                    String.concat " " s)
        
    module CSVBuilder =
        open SqlConn
        let internal escapeSpecialCharacters (newCell:string) (strb:System.Text.StringBuilder) isFirstCell =
            let shouldBeQuoted = newCell.Contains(",") 
                                 || newCell.Contains("\r")
                                 || newCell.Contains("\n")
                                 || newCell.Contains("\"")

            let processedCell = 
                match newCell.Contains("\"") with
                | true -> newCell.Replace("\"","\"\"")
                | false -> newCell
            
            match isFirstCell with
                | true -> ()
                | false -> 
                    strb.Append(",") |> ignore
               
            match shouldBeQuoted with
                |true -> strb.Append("\"") |> ignore
                         strb.Append(processedCell) |> ignore
                         strb.Append("\"") |> ignore
                |false -> strb.Append(processedCell) |> ignore
            strb
        
        let rec internal buildCSVRowRecursively (list:Cell list) (strb:System.Text.StringBuilder) isFirstCell = 
            match list with
            | [] -> strb
            | head::tail -> 
                buildCSVRowRecursively tail 
                            (escapeSpecialCharacters (head.getData) strb isFirstCell) 
                            false
        
        let buildCSVrow (sqlRow:ResultRow) = 
            let strb = System.Text.StringBuilder()
            buildCSVRowRecursively sqlRow.getRow strb true |> ignore
            strb.ToString()
        
        let buildCSVrowHeaders (headers:QueryHeaders) = 
            let strb = System.Text.StringBuilder()
            buildCSVRowRecursively headers.getQueryHeaders strb true |> ignore
            strb.ToString()

        let rec internal writeCSVRowsRecursively (wr:System.IO.StreamWriter) (rows:ResultRow list)=
            match rows with
            |[] -> ()
            | head::tail -> 
                wr.Write (buildCSVrow head)
                wr.Write("\r\n")
                writeCSVRowsRecursively wr tail

        let generateCSVFile (queryResults:queryResults) = 
            let wr = new System.IO.StreamWriter("C:\svn\Csv.csv")
            wr.Write (buildCSVrowHeaders (queryResults.headers))
            wr.Write("\r\n")
            writeCSVRowsRecursively wr (List.rev (queryResults.rows))
            wr.Close()
            
            
