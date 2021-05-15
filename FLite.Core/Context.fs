module FLite.Core.Context

module Common =
   
    type ResponseType<'Request, 'Response> =
    | PostBack of 'Request * AsyncReplyChannel<'Response>
    | None of 'Request
    | Ping of AsyncReplyChannel<Result<unit,unit>>
    | Fetch of 'Request * AsyncReplyChannel<obj>

    type Agent<'Request, 'Response>(requestHandler : 'Request -> 'Response) =
        
        let agent = MailboxProcessor<ResponseType<'Request,'Response>>.Start(fun inbox ->
            let rec loop() =
                async {
                    let! responseType = inbox.Receive()
                    match responseType with
                    | PostBack (msg, rc) ->
                        rc.Reply(requestHandler msg)
                        ()
                    | None msg -> requestHandler msg |> ignore
                    | Ping rc -> ()
                    | Fetch (msg, rc) -> ()
                    
                    return! loop ()
                }
            
            loop ())

        member a.Post(message : 'Request) = agent.Post(None message)
        
        member a.PostAndReply(message : 'Request) = agent.PostAndReply(fun rc -> PostBack(message, rc))

        member a.Ping() = agent.PostAndReply(fun rc -> Ping(rc))

        member a.Fetch<'Type>(message : 'Request) =
            let o = agent.PostAndReply(fun rc -> Fetch(message, rc))
            o :?> 'Type
            
module Sqlite =
    
    open Common
    
    type QueryType =
        | NonQuery
    
    module private Internal =
        let i = ()
    
    type DbRequest =
        | NoOp
    
    type DbResult =
        | Success
    
    type DbContext() =
        
        let agent = Agent<DbRequest, DbResult>(fun r -> DbResult.Success)
        
        member context.Test() = ()