﻿open System
open System.IO
open System.Reactive.Linq
open System.Threading
open System.Threading.Tasks

open Discord
open Discord.WebSocket
open LibGit2Sharp
open LibGit2Sharp.Handlers
open OpenAPI.Net
open OpenAPI.Net.Helpers
open ParquetSharp

let repoPath = "../TickData"

let addFile (f : string) =
  printfn $"add file: {f}"
  use repo = new Repository (repoPath)
  let f = f[repoPath.Length+1..]
  printfn $"{f}"
  repo.Index.Add f
  repo.Index.Write ()

let addDir (d : string) =
  printfn $"add dir: {d}"
  use repo = new Repository (repoPath)
  Directory.GetFiles d
  |> Array.iter (fun f ->
    let f = f[repoPath.Length+1..]
    printfn $"{f}"
    repo.Index.Add f)
  repo.Index.Write ()

let commit (dir : string) =
  printfn "commit"
  use repo = new Repository (repoPath)
  let signature = Signature ("upload bot", "sheganinans@gmail.com", DateTimeOffset.Now)
  repo.Commit ($"added: {dir[repoPath.Length+1..]}", signature, signature) |> ignore

let creds =
  let c = UsernamePasswordCredentials ()
  c.Username <- "sheganinans@gmail.com"
  c.Password <- Environment.GetEnvironmentVariable "HUGGING_FACE_PW"
  c

let push () =
  printfn "push"
  use repo = new Repository (repoPath)
  let options = PushOptions ()
  options.CredentialsProvider <- CredentialsHandler (fun _ _ _ -> creds)
  repo.Network.Push (repo.Network.Remotes["origin"], "refs/heads/main", options)

let delDir (d : string) =
  Directory.GetFiles d |> Array.iter (fun f -> (FileInfo f).Delete ())
  (DirectoryInfo d).Delete ()

let sendAlert =
  printfn "logging into discord."
  let discord = new DiscordSocketClient ()
  let token =  Environment.GetEnvironmentVariable "DISCORD_TOKEN"
  let guild = uint64 <| Environment.GetEnvironmentVariable "DISCORD_GUILD"
  let channel = uint64 <| Environment.GetEnvironmentVariable "DISCORD_CHANNEL"
  discord.LoginAsync (TokenType.Bot, token) |> Async.AwaitTask |> Async.RunSynchronously
  discord.StartAsync () |> Async.AwaitTask |> Async.RunSynchronously
  Thread.Sleep 3_000

  fun () ->
    async {
      do! Async.Sleep 60_000
      do!
        discord.GetGuild(guild).GetTextChannel(channel).SendMessageAsync "@everyone Did not receive response in over a minute."
        |> Async.AwaitTask
        |> Async.Ignore
    }
    
let client_id = Environment.GetEnvironmentVariable "CTRADER_API_CLIENT_ID"
let client_secret = Environment.GetEnvironmentVariable "CTRADER_API_CLIENT_SECRET"
let access_token = Environment.GetEnvironmentVariable "CTRADER_API_CLIENT_ACCESS_TOKEN"
let acc_id = int64 <| Environment.GetEnvironmentVariable "CTRADER_API_ACCOUNT_ID"

let inline private sub<'t> (client : OpenClient) (f : 't -> unit) =
  (unbox<IObservable<_>> client).OfType<'t>().Subscribe f

let client =
  task {
    let client = new OpenClient (ApiInfo.DemoHost, ApiInfo.Port, TimeSpan.FromSeconds 10, useWebSocket = false)
    do! client.Connect ()
    printfn "connected."

    let applicationAuthReq = ProtoOAApplicationAuthReq ()
    applicationAuthReq.ClientId <- client_id
    applicationAuthReq.ClientSecret <- client_secret
    do! client.SendMessage applicationAuthReq
    do! Task.Delay 100

    let accountAuthReq = ProtoOAAccountAuthReq ()
    accountAuthReq.CtidTraderAccountId <- acc_id
    accountAuthReq.AccessToken <- access_token
    do! client.SendMessage accountAuthReq
    printfn "auth completed."
    return client
  } |> Async.AwaitTask |> Async.RunSynchronously

Thread.Sleep 100

if not (File.Exists $"{repoPath}/finished.txt")
then
  let f = File.Create $"{repoPath}/finished.txt"
  f.Flush ()
  f.Close ()

let symbols =
  task {
    let mutable symbols = [||]

    sub client (fun (rsp : ProtoOASymbolsListRes) ->
      printfn $"# of symbols: {rsp.Symbol.Count}"
      symbols <- rsp.Symbol |> Seq.toArray) |> ignore

    let req = ProtoOASymbolsListReq ()
    req.CtidTraderAccountId <- acc_id
    do! client.SendMessage req
    while symbols.Length = 0 do do! Task.Delay 1_000
    let skipSymbols = File.ReadLines $"{repoPath}/finished.txt" |> Seq.filter (fun s -> s <> "") |> Seq.map int64 |> Set.ofSeq
    return symbols |> Array.filter (fun s -> not <| skipSymbols.Contains s.SymbolId)
  } |> Async.AwaitTask |> Async.RunSynchronously

Thread.Sleep 100

let symbolInfo =
  task {
    let mutable info = [||]

    sub client (fun (rsp : ProtoOASymbolByIdRes) ->
      info <- rsp.Symbol |> Seq.toArray) |> ignore

    let req = ProtoOASymbolByIdReq ()
    req.CtidTraderAccountId <- acc_id
    symbols |> Array.iter (fun s -> req.SymbolId.Add s.SymbolId)
    do! client.SendMessage req
    while info.Length = 0 do do! Task.Delay 1_000
    return info |> Array.map (fun s -> s.SymbolId, s) |> Map.ofArray
  } |> Async.AwaitTask |> Async.RunSynchronously

let firstDay = DateTime (2018, 1, 1)
let lastDay = DateTime (2023, 8, 1)
let mutable date = firstDay
let mutable symbol = ""
let mutable symbolId = 0L
let mutable side = ProtoOAQuoteType.Bid

let tokenSource = new CancellationTokenSource ()

type GrabReq =
  | NewDay
  | OffsetBy of int64

let grabber =
  MailboxProcessor.Start (fun inbox ->
    let rec loop () =
      async {
        let mutable endFilter = 0L
        match! inbox.Receive () with
        | NewDay -> endFilter <- int64 (date.AddDays(1).Subtract(DateTime(1970, 1, 1)).TotalMilliseconds)
        | OffsetBy t -> endFilter <- t

        let req = ProtoOAGetTickDataReq ()
        req.CtidTraderAccountId <- acc_id
        req.SymbolId <- symbolId
        req.FromTimestamp <- int64 (date.Subtract(DateTime(1970, 1, 1)).TotalMilliseconds)
        req.ToTimestamp <- endFilter
        req.Type <- side
        printfn $"""grab: {symbolId} {symbol} {side} {date}
          {DateTimeOffset.FromUnixTimeMilliseconds req.FromTimestamp}
          {DateTimeOffset.FromUnixTimeMilliseconds req.ToTimestamp}"""

        // backpressure: api limit is 5 req/sec.
        do! Async.Sleep 50
        client.SendMessage req |> Async.AwaitTask |> Async.RunSynchronously
        Async.Start (sendAlert (), tokenSource.Token)
        return! loop ()
      }
    loop ())

type SymbolMsg =
  | Init of ProtoOALightSymbol []
  | Next

let symbolMgr =
  MailboxProcessor.Start (fun inbox ->
    let mutable i = 0
    let mutable symbols = [||]
    let rec loop () =
      async {
        match! inbox.Receive () with
        | Init syms ->
          symbols <- syms
        | Next ->
          if i = symbols.Length
          then printfn "done!"
          else
            symbol <- symbols[i].SymbolName
            symbolId <- symbols[i].SymbolId
            i <- i + 1
            grabber.Post NewDay
        return! loop ()
      }
    loop ())

let mutable data : {| Tick : float; Timestamp:int64 |} [] = [||]

let saver =
  MailboxProcessor.Start (fun inbox ->
    let rec loop () =
      async {
        do! inbox.Receive ()
        if not (Directory.Exists $"{repoPath}/{symbolId}") then Directory.CreateDirectory $"{repoPath}/{symbolId}" |> ignore
        let cols : Column [] = [| Column<int64> "timestamp"; Column<float> "tick" |]
        let prettySide = match side with | ProtoOAQuoteType.Ask -> "ask" | _ -> "bid"
        let filePath = @$"{repoPath}/{symbolId}/{date.Year}-%02i{date.Month}-%02i{date.Day}.{prettySide}.parquet"
        use file = new ParquetFileWriter (filePath, cols)
        use rowGroup = file.AppendRowGroup ()
        let dataDedup =
          data
          |> Array.mapi (fun i x -> i, x)
          |> Array.filter (fun  (i, x) -> if (i + 1) < data.Length then x <> data[i+1] else true)
          |> Array.map snd
        use w = rowGroup.NextColumn().LogicalWriter<int64>() in w.WriteBatch (dataDedup |> Array.map (fun r -> r.Timestamp))
        use w = rowGroup.NextColumn().LogicalWriter<float>() in w.WriteBatch (dataDedup |> Array.map (fun r -> r.Tick))
        printfn $"~~~\n{filePath}\n~~~\nsaved\n~~~"
        data <- [||]
        if date = lastDay
        then
          let _ = (use w = File.AppendText $"{repoPath}/finished.txt" in w.WriteLine $"{symbolId}"; w.Flush (); w.Close ())
          addFile $"{repoPath}/finished.txt"
          let d = $"{repoPath}/{symbolId}"
          addDir d
          commit d
          push ()
          delDir d
          date <- firstDay
          symbolMgr.Post Next
        else
          match side with
          | ProtoOAQuoteType.Bid -> side <- ProtoOAQuoteType.Ask
          | ProtoOAQuoteType.Ask -> side <- ProtoOAQuoteType.Bid; date <- date.AddDays 1
          | _ -> raise (Exception "this should never happen.")
          grabber.Post NewDay
        return! loop ()
      }
    loop ())

let onTickData (rsp : ProtoOAGetTickDataRes) =
  printfn $"rec'd ticks: {rsp.TickData.Count}, {rsp.HasMore}"
  tokenSource.Cancel ()
  if rsp.TickData.Count = 0
  then saver.Post ()
  else
    let roundBy = symbolInfo[symbolId].Digits
    let ticks = rsp.TickData |> Seq.toArray
    let firstTick = {| Tick = Math.Round (float ticks[0].Tick / 100_000., roundBy); Timestamp = ticks[0].Timestamp |}
    let ticks =
      ticks[1..] |> Array.scan
        (fun acc x -> {|
          Tick      = Math.Round (acc.Tick + (float x.Tick / 100_000.), roundBy)
          Timestamp = acc.Timestamp + x.Timestamp
        |})
        firstTick
    printfn $"0: {DateTimeOffset.FromUnixTimeMilliseconds firstTick.Timestamp} {firstTick.Tick}"
    printfn $"l: {DateTimeOffset.FromUnixTimeMilliseconds (ticks |> Array.last).Timestamp} {(ticks |> Array.last).Tick}"
    data <- Array.append (Array.append [|firstTick|] ticks |> Array.rev) data
    if rsp.HasMore
    then grabber.Post (OffsetBy data[0].Timestamp)
    else saver.Post ()

sub client onTickData |> ignore

symbolMgr.Post (Init symbols)
symbolMgr.Post Next

while true do Thread.Sleep 60_000