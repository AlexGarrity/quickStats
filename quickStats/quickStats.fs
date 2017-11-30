namespace quickStats


open System.Configuration
open FSharp.Configuration
    module SqlConn =
        
        open System.Data
        open System.Data.SqlClient
        open System.Collections.Generic

        ///<summary>load all connections from app.config</summary>
        ///<returns>Sequence of all connections listed in the app.config</returns>
        let getConnections() = 
            seq { for conn in ConfigurationManager.ConnectionStrings -> 
                    match conn.ConnectionString with
                    | "" -> None
                    | x -> Some conn}
            |> Seq.choose id
        let ListToString (l:string list) = 
            let mutable s = ""
            l |> Seq.iter (fun f -> s <- s + " " + f )
            s
        type queryResult = {headers:string list; rows:string list list}

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
                getQueryHeaders ((reader.GetName(i))::headers) reader (i+1)

        let rec getResultRow columns (reader:SqlDataReader) i = 
            match i>=0 && reader.FieldCount > i with 
            | false -> columns
            | true -> getResultRow ((reader.GetProviderSpecificValue(i).ToString())::columns) reader (i+1)

        let rec getAllResultRowsForQuery rows (reader:SqlDataReader)=
            match reader.Read() with
            | false -> rows
            | true -> getAllResultRowsForQuery ((getResultRow [] reader 0)::rows) reader

        let rec processAllQueries (reader:SqlDataReader) = 
            let mutable hasMore = true
            let mutable queries = []
            while hasMore do
            
                    let qr = { headers = getQueryHeaders [] reader 0;
                               rows = (getAllResultRowsForQuery [] reader) }
                    
                    match qr.headers with
                        | [] -> ()
                        | a -> queries <- qr::queries
                    hasMore <- reader.NextResult()
      
            match queries with 
            | [] -> None
            | q -> Some q

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

            sqlConnection.Open()
            let reader = cmd.ExecuteReader()
            let queries = processAllQueries reader

            reader.Dispose()
            reader.Close()
            sqlConnection.Close()
            queries

        let printConnections = 
            getConnections() 
            |> Seq.iter (fun s-> printfn "%s" (s.ToString()) )   
    

    module Files = 
        open System.IO

        let loadSQLScript = 
            File.ReadAllLines(AppSettings<"app.config">.SqlFilePathAndName)
            |> (fun s -> 
                    printfn "%A" s
                    String.concat " " s)

