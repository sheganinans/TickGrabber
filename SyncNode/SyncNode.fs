open System
open System.IO
open System.Threading

open Saturn
open Giraffe
open Shared

if not <| File.Exists "./finished.txt" then (File.Create "./finished.txt").Close ()
if not <| File.Exists "./node_states.txt" then (File.Create "./node_states.txt").Close ()

//let _ = (use w = File.AppendText "./finished.txt" in w.WriteLine $"{symbolId}"; w.Flush (); w.Close ())

type     CTId = int64
type SymbolId = int64

let mutable currState : Map<CTId, SymbolId> =
  (File.ReadAllText "./node_states.txt").Split '\n'
  |> Array.filter (fun s -> s <> "")
  |> Array.map (fun s ->
    let s = s.Split ','
    int64 s[0], int64 s[1])
  |> Map.ofArray

let saveCurrState () =
  use sw = new StreamWriter "./curr_state.txt"
  currState |> Map.iter (fun k v -> sw.WriteLine $"{k},{v}")
  sw.Flush ()

let mutable finished : SymbolId Set =
  (File.ReadAllText "./finished.txt").Split '\n'
  |> Array.filter (fun s -> s <> "")
  |> Array.map int64
  |> Set.ofArray

let client = CTrader.getClient ()

let symbols =
  async {
    let mutable symbols = [||]

    CTrader.sub client (fun (rsp : ProtoOASymbolsListRes) ->
      printfn $"# of symbols: {rsp.Symbol.Count}"
      symbols <- rsp.Symbol |> Seq.toArray) |> ignore

    let req = ProtoOASymbolsListReq ()
    req.CtidTraderAccountId <- CTrader.acc_id
    do! client.SendMessage req |> Async.AwaitTask
    while symbols.Length = 0 do do! Async.Sleep 100
    return symbols |> Array.filter (fun s -> not <| finished.Contains s.SymbolId)
  } |> Async.RunSynchronously

Thread.Sleep 100

let symbolInfo : Map<SymbolId, ProtoOASymbol> =
  async {
    let mutable info = [||]

    CTrader.sub client (fun (rsp : ProtoOASymbolByIdRes) -> info <- rsp.Symbol |> Seq.toArray) |> ignore

    let req = ProtoOASymbolByIdReq ()
    req.CtidTraderAccountId <- CTrader.acc_id
    symbols |> Array.iter (fun s -> req.SymbolId.Add s.SymbolId)
    do! client.SendMessage req  |> Async.AwaitTask
    while info.Length = 0 do do! Async.Sleep 100
    return info |> Array.map (fun s -> s.SymbolId, s) |> Map.ofArray
  } |> Async.RunSynchronously

let reqLock = Object () // replace with mailbox

let mutable i = 0

let currSymbol () =
  if i >= symbols.Length
  then json Finished
  else
    let s = symbols[i]
    match symbolInfo |> Map.tryFind s.SymbolId with
    | None -> json (Err $"unexpected symbol id: {s.SymbolId}")
    | Some i -> json (SymbolInfo {| Id = s.SymbolId; Symbol = s.SymbolName; Digits = i.Digits |})

let routes = router {
  getf "/get-symbol-id/%i" (fun ctid ->
    lock reqLock (fun () ->
      let ctid = int64 ctid
      match currState |> Map.tryFind ctid with
      | None ->
        currState <- currState.Add (ctid, symbols[i].SymbolId)
        saveCurrState ()
        let rsp = currSymbol ()
        i <- i + 1
        rsp
      | Some sid ->
        match symbolInfo |> Map.tryFind sid with
        | None -> json (Err $"did not find symbol id: {sid}")
        | Some i -> json (SymbolInfo {| Id = i.SymbolId; Symbol = (symbols |> Array.find (fun s -> s.SymbolId = sid)).SymbolName; Digits = i.Digits |})))

  getf "/finished-job/%i/%i" (fun (ctid, symbolId) ->
    lock reqLock (fun () ->
      let ctid, symbolId = int64 ctid, int64 symbolId
      match currState |> Map.tryFind ctid with
      | None -> json (Err $"did not find ctid: {ctid}")
      | Some currId ->
        if currId <> symbolId
        then json (Err $"unexpected symbol: {symbolId}")
        else
          finished <- finished.Add symbolId
          use sw = new StreamWriter "./finished.txt"
          finished |> Set.iter sw.WriteLine
          sw.Close ()
          i <- i + 1
          currState <- currState.Add (ctid, symbols[i].SymbolId)
          saveCurrState ()
          currSymbol ()))
}

run <| application {
  use_router routes
}
