namespace quickStats

module Regex = 
    open System.Text.RegularExpressions
    open System
   
    let (|OutputPath|_|) arg = 
        if String.Compare("--outputPath", arg, StringComparison.OrdinalIgnoreCase) = 0
        then Some() else None

    let (|ValidOutputPath|_|) arg = 
        match IO.Directory.Exists arg with
        | true -> Some arg
        | _ -> None

    let (|ValidPathToSQLFile|_|) arg =
        match IO.File.Exists arg with
        | true -> 
            match IO.Path.GetExtension arg with
            | ".sql" -> Some arg
            | _ -> None
        | _ -> None

    let (|ValidConfigPath|_|) arg =
        match IO.File.Exists arg with
        | true ->
            match IO.Path.GetExtension arg with
            | ".json" -> Some arg
            | _ -> None
        | _ -> None
    
    let (|PathToSQLScript|_|) arg = 
        if String.Compare("--pathToSQLScript", arg, StringComparison.OrdinalIgnoreCase) = 0
        then Some() else None

    let (|PathToConfig|_|) arg =
        if String.Compare("--config", arg, StringComparison.OrdinalIgnoreCase) = 0 then
            Some()
        else
            None
    
    
module ParseCommandLineArgs = 
    open Regex
  
    type CommandLineOptions = {configPath: string option; pathToSQLScript: string option; outputPath: string option}

    let defaultOptions = {configPath = None; pathToSQLScript = None; outputPath = None}
    
    let rec internal parseCommandLine optionsSoFar args  = 
        match args with 
        // empty list means we're done.
        | [] -> optionsSoFar  

        // Match --config
        | PathToConfig::xs ->
            match xs with
            | ValidConfigPath x :: xss ->
                let newOptionsSoFar = { optionsSoFar with configPath = Some x }
                parseCommandLine newOptionsSoFar xss
            | _ ->
                eprintfn "--config must be followed by a valid path to a config file"
                parseCommandLine optionsSoFar xs

        // match --pathToSQLScript flag
        | PathToSQLScript::xs -> 
            match xs with 
            | ValidPathToSQLFile x ::xss -> 
                let newOptionsSoFar = { optionsSoFar with pathToSQLScript=Some x}
                parseCommandLine newOptionsSoFar xss  
            | _ ->
                eprintfn "--pathToSQLScript must be followed by a valid path to a file containing a SQL script" 
                parseCommandLine optionsSoFar xs  

        // match --outputPath flag
        | OutputPath::xs -> 
            match xs with
            | ValidOutputPath x ::xss ->
                let newOptionsSoFar = { optionsSoFar with outputPath=Some x}
                parseCommandLine newOptionsSoFar xss  
            | _ ->
                eprintfn "--outputPath must be followed by a valid path to a directory"
                parseCommandLine optionsSoFar xs  
        
        // handle unrecognized option and keep looping
        | x::xs -> 
            failwithf "Option '%s' is not recognized" x
            parseCommandLine optionsSoFar xs  
    
    let parseCommandLineArguments args =
        parseCommandLine defaultOptions args