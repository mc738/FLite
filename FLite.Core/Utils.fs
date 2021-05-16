namespace FLite.Core.Utils

open System

[<AutoOpen>]
module Extensions =
    
    type String with
    
        member str.ToSnakeCase() =            
            str
            |> List.ofSeq
            |> List.fold (fun (acc, i) c ->
                let newAcc =
                    match Char.IsUpper c, i = 0 with
                    | false, _ -> acc @ [ c ]
                    | true, true -> acc @ [ Char.ToLower(c) ]
                    | true, false -> acc @ [ '_'; Char.ToLower(c) ]
                (newAcc, i + 1)) ([], 0)
            |> (fun (chars, _) -> String(chars |> Array.ofList))
