namespace quickStats

module Regex = 
    open System.Text.RegularExpressions
    open System

    [<Literal>] 
    let pathToSQLScript = "^(?:[\\w]\\:|\\\\)(\\\\[a-zA-Z_\\-\\s0-9\\.\\$]+)+\\.(sql)$"
    [<Literal>] 
    let outputPath = "^(?:[\\w]\\:|\\\\)(\\\\[a-zA-Z_\\-\\s0-9\\.\\$]+)+$"

    let (|ParseRegex|_|) regex str =
        let m = Regex(regex).Match(str)
        if m.Success then Some str
        else None
    
    let (|OutputPath|_|) arg = 
        if String.Compare("--outputPath", arg, StringComparison.OrdinalIgnoreCase) = 0
        then Some() else None

    let (|ValidOutputPath|_|) arg = 
        match arg with
        | ParseRegex outputPath s -> Some s
        | _ -> None

    let (|ValidConfigPath|_|) arg =
        match System.IO.File.Exists arg with
        | true -> Some arg
        | _ -> None
    
    let (|PathToSQLScript|_|) arg = 
        if String.Compare("--pathToSQLScript", arg, StringComparison.OrdinalIgnoreCase) = 0
        then Some() else None

    let (|PathToConfig|_|) arg =
        if String.Compare("--config", arg, StringComparison.OrdinalIgnoreCase) = 0 then
            Some()
        else
            None

    let (|ValidPathToSQLFile|_|) arg = 
        match arg with
        | ParseRegex pathToSQLScript s -> Some s
        | _ -> None
    
    
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
                eprintfn "The config parameter must be followed by a valid filepath"
                parseCommandLine optionsSoFar xs

        // match --pathToSQLScript flag
        | PathToSQLScript::xs -> 
            match xs with 
            | ValidPathToSQLFile x ::xss -> 
                let newOptionsSoFar = { optionsSoFar with pathToSQLScript=Some x}
                parseCommandLine newOptionsSoFar xss  
            | _ ->
                eprintfn "--pathToSQLScript needs a second argument or the provided argument is not valid" 
                parseCommandLine optionsSoFar xs  

        // match --outputPath flag
        | OutputPath::xs -> 
            match xs with
            | ValidOutputPath x ::xss ->
                let newOptionsSoFar = { optionsSoFar with outputPath=Some x}
                parseCommandLine newOptionsSoFar xss  
            | _ ->
                eprintfn "--outputPath needs a second argument  or the provided argument is not valid"
                parseCommandLine optionsSoFar xs  
        
        // handle unrecognized option and keep looping
        | x::xs -> 
            failwithf "Option '%s' is not recognized" x
            parseCommandLine optionsSoFar xs  
    
    let parseCommandLineArguments args =
        parseCommandLine defaultOptions args