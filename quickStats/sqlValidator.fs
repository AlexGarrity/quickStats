namespace quickStats


module sqlValidator = 
    open System
    open System.Text


    let ModifyingKeywords = [
        "alter"
        "delete"
        "drop"
        "dump"
        "exec"
        "insert"
        "insert"
        "truncate"
        "update" 
        ]


    let rec private seqSearch keywords str = 
        match keywords with
        | [] -> false
        | h::t ->
            match String.Compare(str,h, true) with 
            | 0 -> true
            | p when p > 0 -> seqSearch t str
            | _ -> false
    

    let private isModifyingKeyword str = 
        seqSearch ModifyingKeywords str
    
    let stringToCharList (s:string) = [for c in s -> c]
    

    let rec private findBlockCommentEnd (sqlString:char list) =
        
        match sqlString with
        | [] -> []
        | '*'::tail -> 
            match tail with 
            | '/'::xs -> xs
            | _ -> findBlockCommentEnd tail
        | _::tail -> findBlockCommentEnd tail


    let rec findLineCommentEnd (sqlString:char list) =
        
        match sqlString with
        | [] -> []
        | '\n'::tail -> tail
        | _::tail -> findLineCommentEnd tail

    let rec containsModifyingKeywords (sqlString:char list) (word:StringBuilder) = 
        match sqlString with 
        | [] -> false
        | h::tail -> 
            match h with 
            | c when Char.IsLetter c -> containsModifyingKeywords tail (word.Append h)
            | '\r'|'\n'|'\t'|'('|' ' -> 
                match isModifyingKeyword (word.ToString()) with
                | true -> true
                | false -> 
                    
                    containsModifyingKeywords tail (word.Clear())
            | '-' -> 
                    
                    match tail with 
                    | [] -> false
                    | '-'::xs -> 
                    containsModifyingKeywords (findLineCommentEnd xs) (word.Clear()) 
                    | _ -> containsModifyingKeywords tail (word.Clear())
            | '/' ->
                    
                    match tail with 
                    | [] -> false
                    | x::xs when x = '*' -> 
                    containsModifyingKeywords (findBlockCommentEnd xs) (word.Clear()) 
                    | _ -> containsModifyingKeywords tail (word.Clear())

            | _ -> containsModifyingKeywords tail word
                    




