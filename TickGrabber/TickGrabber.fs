open System
open System.IO
open System.Reactive.Linq
open System.Threading

open Amazon.Runtime.CredentialManagement
open Amazon.S3
open Amazon.S3.Transfer
open Discord
open Discord.WebSocket
open OpenAPI.Net
open OpenAPI.Net.Helpers
open ParquetSharp

let sendAlert =
  printfn "logging into discord."
  let discord = new DiscordSocketClient ()
  let token =  Environment.GetEnvironmentVariable "DISCORD_TOKEN"
  let guild = uint64 <| Environment.GetEnvironmentVariable "DISCORD_GUILD"
  let channel = uint64 <| Environment.GetEnvironmentVariable "DISCORD_CHANNEL"
  discord.LoginAsync (TokenType.Bot, token) |> Async.AwaitTask |> Async.RunSynchronously
  discord.StartAsync () |> Async.AwaitTask |> Async.RunSynchronously
  Thread.Sleep 3_000

  fun (m : string) ->
    async {
      do! Async.Sleep 60_000
      do!
        discord.GetGuild(guild).GetTextChannel(channel).SendMessageAsync $"@everyone\n```{m}\n```"
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
  async {
    let client = new OpenClient (ApiInfo.DemoHost, ApiInfo.Port, TimeSpan.FromSeconds 10, useWebSocket = false)
    do! client.Connect () |> Async.AwaitTask
    printfn "connected."

    let applicationAuthReq = ProtoOAApplicationAuthReq ()
    applicationAuthReq.ClientId <- client_id
    applicationAuthReq.ClientSecret <- client_secret
    do! client.SendMessage applicationAuthReq |> Async.AwaitTask
    do! Async.Sleep 100

    let accountAuthReq = ProtoOAAccountAuthReq ()
    accountAuthReq.CtidTraderAccountId <- acc_id
    accountAuthReq.AccessToken <- access_token
    do! client.SendMessage accountAuthReq  |> Async.AwaitTask
    printfn "auth completed."
    return client
  } |> Async.RunSynchronously

Thread.Sleep 100

if not (File.Exists $"./finished.txt")
then
  use f = File.Create $"./finished.txt"
  f.Flush ()
  f.Close ()

if not (File.Exists $"./date.txt")
then
  use f = File.Create $"./date.txt"
  f.Flush ()
  f.Close ()

let symbols =
  async {
    let mutable symbols = [||]

    sub client (fun (rsp : ProtoOASymbolsListRes) ->
      printfn $"# of symbols: {rsp.Symbol.Count}"
      symbols <- rsp.Symbol |> Seq.toArray) |> ignore

    let req = ProtoOASymbolsListReq ()
    req.CtidTraderAccountId <- acc_id
    do! client.SendMessage req |> Async.AwaitTask
    while symbols.Length = 0 do do! Async.Sleep 1_000
    let skipSymbols = File.ReadLines $"./finished.txt" |> Seq.filter (fun s -> s <> "") |> Seq.map int64 |> Set.ofSeq
    return symbols |> Array.filter (fun s -> not <| skipSymbols.Contains s.SymbolId)
  } |> Async.RunSynchronously

Thread.Sleep 100

let writeFile () =
  use sw = new StreamWriter ("./tickers.tsv")
  sw.WriteLine "id\tnane\tdescrip"
  symbols |> Array.iter (fun i -> sw.WriteLine $"{i.SymbolId}\t{i.SymbolName}\t{i.Description}")

//writeFile ()

let symbolInfo =
  async {
    let mutable info = [||]

    sub client (fun (rsp : ProtoOASymbolByIdRes) -> info <- rsp.Symbol |> Seq.toArray) |> ignore

    let req = ProtoOASymbolByIdReq ()
    req.CtidTraderAccountId <- acc_id
    symbols |> Array.iter (fun s -> req.SymbolId.Add s.SymbolId)
    do! client.SendMessage req  |> Async.AwaitTask
    while info.Length = 0 do do! Async.Sleep 1_000
    return info |> Array.map (fun s -> s.SymbolId, s) |> Map.ofArray
  } |> Async.RunSynchronously

let firstDay = DateTime (2018, 1, 1)
let lastDay = DateTime (2023, 8, 1)
let mutable date = firstDay
let mutable symbol = ""
let mutable symbolId = 0L
let mutable side = ProtoOAQuoteType.Bid

type GrabReq =
  | NewDay
  | OffsetBy of int64

let grabber =
  let req = ProtoOAGetTickDataReq ()
  let mutable endFilter = 0L
  MailboxProcessor.Start (fun inbox ->
    async {
      try
        while true do
          match! inbox.Receive () with
          | NewDay -> endFilter <- int64 (date.AddDays(1).Subtract(DateTime(1970, 1, 1)).TotalMilliseconds)
          | OffsetBy t -> endFilter <- t

          req.CtidTraderAccountId <- acc_id
          req.SymbolId <- symbolId
          req.FromTimestamp <- int64 (date.Subtract(DateTime(1970, 1, 1)).TotalMilliseconds)
          req.ToTimestamp <- endFilter
          req.Type <- side
          printfn $"""grab: {symbolId} {symbol} {side} {date}
            {DateTimeOffset.FromUnixTimeMilliseconds req.FromTimestamp}
            {DateTimeOffset.FromUnixTimeMilliseconds req.ToTimestamp}"""

          // backpressure: api limit is 5 req/sec.
          do! Async.Sleep 210
          if not <| (client.SendMessage req).Wait 60_000 then do! sendAlert "Did not receive response in over a minute."
      with err -> do! sendAlert $"{err}"
    })

type SymbolMsg =
  | Init of ProtoOALightSymbol []
  | Next

let symbolMgr =
  let mutable checkedSkip = false
  let fastForward () =
    if not checkedSkip
    then
      checkedSkip <- true
      File.ReadLines $"./date.txt"
      |> Seq.tryHead
      |> Option.iter (fun s ->
        (try Some (DateTime.Parse s) with _ -> None)
        |> Option.iter (fun dt -> date <- dt))
  MailboxProcessor.Start (fun inbox ->
    let mutable i = 0
    let mutable symbols = [||]
    async {
      try
        while true do
          match! inbox.Receive () with
          | Init syms ->
            symbols <- syms
          | Next ->
            if i = symbols.Length
            then do! sendAlert "done!"
            else
              symbol <- symbols[i].SymbolName
              symbolId <- symbols[i].SymbolId
              fastForward ()
              i <- i + 1
              grabber.Post NewDay
      with err -> do! sendAlert $"{err}"
    })

let uploadFile (file : string) (bucket : string) (key : string) =
  let chain = CredentialProfileStoreChain ()
  let res, creds = chain.TryGetAWSCredentials "default"
  if res
  then
    use s3 = new AmazonS3Client (creds, Amazon.RegionEndpoint.USEast1)
    use u = new TransferUtility (s3)
    u.Upload (file, bucket, key)
  else sendAlert "failed to login to upload file." |> Async.Start

let mutable data : {| Tick : float; Timestamp : int64 |} [] = [||]

let saver =
  let uploadFile = uploadFile "tmp.parquet" "tick-data-sheganinans"
  MailboxProcessor.Start (fun inbox ->
    async {
      try
        while true do
          do! inbox.Receive ()
          printfn "saving"
          let cols : Column [] = [| Column<int64> "timestamp"; Column<float> "tick" |]
          let prettySide = match side with | ProtoOAQuoteType.Ask -> "ask" | _ -> "bid"
          use file = new ParquetFileWriter ("./tmp.parquet", cols)
          use rowGroup = file.AppendRowGroup ()
          let dataDedup =
            data
            |> Array.mapi (fun i x -> i, x)
            |> Array.filter (fun  (i, x) -> if (i + 1) < data.Length then x <> data[i+1] else true)
            |> Array.map snd
          use w = rowGroup.NextColumn().LogicalWriter<int64>() in w.WriteBatch (dataDedup |> Array.map (fun r -> r.Timestamp))
          use w = rowGroup.NextColumn().LogicalWriter<float>() in w.WriteBatch (dataDedup |> Array.map (fun r -> r.Tick))
          file.Close ()
          file.Dispose ()
          let bucketKey = @$"{symbolId}/{date.Year}-%02i{date.Month}-%02i{date.Day}.{prettySide}.parquet"
          uploadFile bucketKey
          printfn $"~~~\n{bucketKey}\n~~~\nsaved\n~~~"
          (FileInfo "./tmp.parquet").Delete ()
          data <- [||]
          if date = lastDay
          then
            let _ = (use w = File.AppendText $"./finished.txt" in w.WriteLine $"{symbolId}"; w.Flush (); w.Close ())
            date <- firstDay
            symbolMgr.Post Next
          else
            match side with
            | ProtoOAQuoteType.Bid -> side <- ProtoOAQuoteType.Ask
            | ProtoOAQuoteType.Ask ->
              side <- ProtoOAQuoteType.Bid
              File.WriteAllText ($"./date.txt", date.ToString ())
              date <- date.AddDays 1
            | _ -> raise (Exception "this should never happen.")
            grabber.Post NewDay
      with err -> do! sendAlert $"{err}"
    })

let onTickData (rsp : ProtoOAGetTickDataRes) =
  printfn $"rec'd ticks: {rsp.TickData.Count}, {rsp.HasMore}"
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

Thread.Sleep -1
