namespace quickStats



    module conn =
        open System.Configuration
        open System.Data
        open System.Data.SqlClient
        open System.Collections.Generic
        
        let getConnections() = 
            seq { for conn in ConfigurationManager.ConnectionStrings -> 
                    match conn.ConnectionString with
                    | "" -> None
                    | x -> Some conn}
            |> Seq.choose id
        let ListToString (l:List<string>) = 
            let mutable s = ""
            l |> Seq.iter (fun f -> s <- s + " " + f )
            s
        let testConnections = 
            let sqlConnection1 = new SqlConnection("Server=EMILIYAN;Database=test;Integrated Security=true");
            let cmd = new SqlCommand();
            

            cmd.CommandText <- "Use test

                                Declare @color varchar(50) = 'blue';

                                with cte_yesNo as(
	                                select * from (
		                                Select 1 as id, 'yes' as 'answer'
		                                union all
		                                Select 2 as id, 'no' as 'answer'
	                                ) as d
                                )
                                Select a1.id, count(*) as [countOfRows] from apples a1
                                inner join apples a2 on a1.id = a2.id
                                left join cte_yesNo yn on yn.id = a2.id
                                where a1.color = @color
                                group by a1.id having count(*) = 1
                                select * from apples
                                "
            cmd.CommandType <- CommandType.Text
            cmd.Connection <- sqlConnection1

            sqlConnection1.Open();

            let reader = cmd.ExecuteReader();
            // Data is accessible through the DataReader object here.
            let mutable moreResults = true
            while moreResults do
                while (reader.Read()) do
                    let mutable row = new List<string>()
                    
                    for i in 0..reader.FieldCount-1 do
                        //printfn "string: %d " i
                        row.Add(reader.GetProviderSpecificValue(i).ToString())
                        
                    printfn "string: %s " (ListToString row)
                moreResults <- reader.NextResult()
                

            reader.Dispose()
            reader.Close()

            sqlConnection1.Close();

        let printConnections = 
            getConnections() 
            |> Seq.iter (fun s-> printfn "%s" (s.ToString()) )   

