open System
open System.IO
open System.Net.Http
open System.Net.Http.Json
open System.Threading

open Amazon.Runtime.CredentialManagement
open Amazon.S3
open Amazon.S3.Transfer
open Newtonsoft.Json
open ParquetSharp

open Shared

if not <| File.Exists "./date.txt" then (File.Create "./date.txt").Close ()

let firstDay = DateTime (2018, 1, 1)
let lastDay = DateTime (2023, 8, 1)
let mutable date = firstDay

File.ReadLines "./date.txt"
|> Seq.tryHead
|> Option.iter (fun s ->
  (try Some (DateTime.Parse s) with _ -> None)
  |> Option.iter (fun dt -> date <- dt))

let mutable symbol = ""
let mutable symbolId = 0L
let mutable roundBy = 4
let mutable side = ProtoOAQuoteType.Bid

let mutable finished = false

let sync_node = Environment.GetEnvironmentVariable "SYNC_NODE_ADDR"

task {
  use client = new HttpClient ()
  let! response = client.GetAsync $"{sync_node}/get-symbol-id/{CTrader.acc_id}"
  let! c = response.Content.ReadAsStringAsync ()
  match JsonConvert.DeserializeObject<Msg> c with
  | SymbolInfo i ->
    symbol <- i.Symbol
    symbolId <- i.Id
    roundBy <- i.Digits
  | Finished -> finished <- true
  | Err err ->
    finished <- true
    Discord.sendAlert err |> Async.Start
}
|> Async.AwaitTask
|> Async.RunSynchronously

type GrabReq =
  | NewDay
  | OffsetBy of int64

let client = CTrader.getClient ()

let grabber =
  let req = ProtoOAGetTickDataReq ()
  let mutable endFilter = 0L
  MailboxProcessor.Start (fun inbox ->
    async {
      try
        while true do
          match! inbox.Receive () with
          | NewDay -> endFilter <- int64 <| date.AddDays(1).Subtract(DateTime(1970, 1, 1)).TotalMilliseconds
          | OffsetBy t -> endFilter <- t

          if date = firstDay then Discord.sendAlert $"starting: {symbol}" |> Async.Start

          req.CtidTraderAccountId <- CTrader.acc_id
          req.SymbolId <- symbolId
          req.FromTimestamp <- int64 <| date.Subtract(DateTime(1970, 1, 1)).TotalMilliseconds
          req.ToTimestamp <- endFilter
          req.Type <- side
          printfn $"""grab: {symbolId} {symbol} {side} {date}
            {DateTimeOffset.FromUnixTimeMilliseconds req.FromTimestamp}
            {DateTimeOffset.FromUnixTimeMilliseconds req.ToTimestamp}"""

          // backpressure: api limit is 5 req/sec.
          do! Async.Sleep 170
          if not <| (client.SendMessage req).Wait 60_000 then do! Discord.sendAlert "Did not receive response in over a minute."
      with err -> do! Discord.sendAlert $"{err}"
    })

type SymbolMsg =
  | Init of ProtoOALightSymbol []
  | Next

let uploadFile (file : string) (bucket : string) (key : string) =
  let chain = CredentialProfileStoreChain ()
  let res, creds = chain.TryGetAWSCredentials "default"
  if res
  then
    use s3 = new AmazonS3Client (creds, Amazon.RegionEndpoint.USEast1)
    use u = new TransferUtility (s3)
    u.Upload (file, bucket, key)
  else Discord.sendAlert "failed to login to upload file." |> Async.Start

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
          let bucketKey = $"{symbolId}/{date.Year}-%02i{date.Month}-%02i{date.Day}.{prettySide}.parquet"
          uploadFile bucketKey
          printfn $"~~~\n{bucketKey}\n~~~\nsaved\n~~~"
          (FileInfo "./tmp.parquet").Delete ()
          data <- [||]
          if date = lastDay
          then
            task {
              use client = new HttpClient ()
              let! response =  client.GetAsync $"{sync_node}/finished-job/{CTrader.acc_id}/{symbolId}"
              let! c = response.Content.ReadAsStringAsync ()
              match JsonConvert.DeserializeObject<Msg> c with
              | SymbolInfo i ->
                symbol <- i.Symbol
                symbolId <- i.Id
                roundBy <- i.Digits
                Discord.sendAlert $"starting: {symbol}" |> Async.Start
              | Finished -> finished <- true
              | Err err ->
                finished <- true
                Discord.sendAlert err |> Async.Start
            }
            |> Async.AwaitTask
            |> Async.RunSynchronously
            date <- firstDay
            grabber.Post NewDay
          else
            match side with
            | ProtoOAQuoteType.Bid -> side <- ProtoOAQuoteType.Ask
            | ProtoOAQuoteType.Ask ->
              side <- ProtoOAQuoteType.Bid
              File.WriteAllText ("./date.txt", date.ToString ())
              date <- date.AddDays 1
            | _ -> raise (Exception "this should never happen.")
            grabber.Post NewDay
      with err -> do! Discord.sendAlert $"{err}"
    })

let onTickData (rsp : ProtoOAGetTickDataRes) =
  printfn $"rec'd ticks: {rsp.TickData.Count}, {rsp.HasMore}"
  if rsp.TickData.Count = 0
  then saver.Post ()
  else
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

CTrader.sub client onTickData |> ignore

grabber.Post NewDay

Thread.Sleep -1
