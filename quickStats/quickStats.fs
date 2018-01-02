namespace quickStats


open System.Configuration
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
        
        type queryResults = {clientName:string; headers:QueryHeaders; rows:ResultRow list}
        
        
        ///<summary>load all connections from app.config</summary>
        ///<returns>Sequence of all connections listed in the app.config</returns>
        let getConnections = 
            lazy
                seq { for conn in ConfigurationManager.ConnectionStrings -> 
                        match conn.ConnectionString with
                        | "" -> None
                        | x -> Some conn}
                |> Seq.choose id
            
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

        let rec internal processAllQueries clientName queries (reader:SqlDataReader) hasMore = 
            
            match hasMore with
            | false -> 
                 match queries with 
                    | [] -> None
                    | q -> Some q
            | true -> 
                let qr = { clientName=clientName
                           headers =QueryHeaders( getQueryHeaders [] reader 0 )
                           rows = (getAllResultRowsForQuery [] reader) }
                    
                processAllQueries clientName (match qr.headers with
                                             | QueryHeaders([]) -> queries
                                             | a -> qr::queries) reader (reader.NextResult())
            
        
            

        ///<summary>Execute a sql script and return results as list of linked lists</summary>
        ///<param name="connectionString">The connection string which will be used</param>
        ///<param name="str">the sql script which will be executed using the specified connection string</param>
        ///<returns>Sequence of linked lists</returns>
        let executeScript clientName connectionString str = 
            let sqlConnection = new SqlConnection(connectionString);
            
            let cmd = new SqlCommand();
            cmd.CommandText <- str         
            cmd.CommandType <- CommandType.Text
            cmd.Connection <- sqlConnection
            cmd.CommandTimeout <- 300
            sqlConnection.Open()
            let reader = cmd.ExecuteReader()

            let queries = processAllQueries clientName [] reader true
            

            reader.Dispose()
            reader.Close()
            sqlConnection.Close()
            queries

        let printConnections = 
            lazy
                let conn = getConnections.Value
                conn |> Seq.iter (fun s-> printfn "%s" (s.ToString()) )   
            

    module CSVBuilder =
        open SqlConn
        
        [<Literal>] 
        let fileNamePattern = "_query_"

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
        
        let buildCSVrow (clientName:Cell) (sqlRow:ResultRow) = 
            let strb = System.Text.StringBuilder()
            buildCSVRowRecursively (clientName::(List.rev sqlRow.getRow)) strb true |> ignore
            strb.ToString()
        
        let buildCSVrowHeaders (headers:QueryHeaders) = 
            let strb = System.Text.StringBuilder()
            buildCSVRowRecursively (Cell("client name")::(List.rev headers.getQueryHeaders)) strb true |> ignore
            strb.ToString()

        let rec internal writeCSVRowsRecursively clientName (wr:System.IO.StreamWriter) (rows:ResultRow list)=
            match rows with
            |[] -> ()
            | head::tail -> 
                wr.Write (buildCSVrow clientName head)
                wr.Write("\r\n")
                writeCSVRowsRecursively clientName wr tail
        
        ///Output query results to a CSV file
        let generateCSVFile (queryResults:queryResults) (clientName:Cell) path fileName = 
            let wr = new System.IO.StreamWriter(path + "\\" + fileName)
            wr.Write (buildCSVrowHeaders (queryResults.headers))
            wr.Write("\r\n")
            writeCSVRowsRecursively clientName wr (List.rev (queryResults.rows))
            wr.Close()
        
        let rec internal generateCSVFilesRecursively (queryResults:queryResults list) path (clientName:Cell) queryCount = 

            match queryResults with
            |[] -> ()
            |head::tail-> 
                let fileName = (clientName.getData) + fileNamePattern + queryCount.ToString() + ".csv"
                generateCSVFile head clientName path fileName
                generateCSVFilesRecursively tail path clientName (queryCount + 1)
        
        let generateCSVFilesForClient queryResults (path:string) (clientName:string) = 
            generateCSVFilesRecursively (List.rev queryResults) path (Cell(clientName)) 1

    module Files = 
        open System.IO

        let loadSQLScript = 
            File.ReadAllLines(ConfigurationManager.AppSettings.Get("sql file path and name"))
            |> (fun s -> 
                    String.concat "\n" s)
        
        let getAllCSVFiles path = 
            Directory.GetFiles(path, "*" + CSVBuilder.fileNamePattern + "*.csv")
        
        let groupCSVFileNames (files:string []) = 
            
            let filesToList = files |> Array.toList
            
            match filesToList with
            | [] -> []
            | fls ->
                fls
                |> List.groupBy (
                    fun f -> 
                        let startIndex = f.LastIndexOf("_query_") 
                                         + "_query_".Length
                        let endIndex = f.LastIndexOf(".csv")
                        f.Substring(startIndex, (endIndex - startIndex) ) |> int
                    )
        
        
        let internal combineCSVFiles path (fileGroups:(int * string list) list) = 
            
            fileGroups
            |> List.iter (
                fun f -> 
                    let combinedFileName = path + "\\resultsForQuery_" + ((fst f).ToString()) + ".csv"
                    snd f
                    |> List.iter ( 
                        fun filePathAndName -> 
                            let fileLines = File.ReadAllLines(filePathAndName)
                            match File.Exists(combinedFileName) with
                            | false -> File.WriteAllLines(combinedFileName, fileLines)
                            | true -> File.AppendAllLines(combinedFileName, fileLines.[1..])
                         
                       )
                )
                    
        let combineAllCSVFiles path =           
           getAllCSVFiles path
           |> groupCSVFileNames
           |> combineCSVFiles path

