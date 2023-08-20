namespace Shared

type Msg =
  | SymbolInfo of {| Id : int64; Symbol: string; Digits : int |}
  | Finished
  | Err of string

module CTrader =
  open System
  open System.Reactive.Linq

  open OpenAPI.Net
  open OpenAPI.Net.Helpers

  let client_id = Environment.GetEnvironmentVariable "CTRADER_API_CLIENT_ID"
  let client_secret = Environment.GetEnvironmentVariable "CTRADER_API_CLIENT_SECRET"
  let access_token = Environment.GetEnvironmentVariable "CTRADER_API_CLIENT_ACCESS_TOKEN"
  let acc_id = int64 <| Environment.GetEnvironmentVariable "CTRADER_API_ACCOUNT_ID"

  let inline sub<'t> (client : OpenClient) (f : 't -> unit) = (unbox<IObservable<_>> client).OfType<'t>().Subscribe f

  let getClient () =
    async {
      let client = new OpenClient (ApiInfo.DemoHost, ApiInfo.Port, TimeSpan.FromSeconds 10, useWebSocket = false)
      do! client.Connect () |> Async.AwaitTask
      printfn "connected."
      do! Async.Sleep 100

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


module Discord =
  open System
  open System.Threading

  open Discord
  open Discord.WebSocket

  let sendAlert =
    printfn "logging into discord."
    let discord = new DiscordSocketClient ()
    let user_id = Environment.GetEnvironmentVariable "DISCORD_USER_ID"
    let token =  Environment.GetEnvironmentVariable "DISCORD_TOKEN"
    let guild = uint64 <| Environment.GetEnvironmentVariable "DISCORD_GUILD"
    let channel = uint64 <| Environment.GetEnvironmentVariable "DISCORD_CHANNEL"
    discord.LoginAsync (TokenType.Bot, token) |> Async.AwaitTask |> Async.RunSynchronously
    discord.StartAsync () |> Async.AwaitTask |> Async.RunSynchronously
    Thread.Sleep 3_000

    fun (m : string) ->
      async {
        do!
          discord.GetGuild(guild).GetTextChannel(channel).SendMessageAsync $"<@{user_id}>\n```{m}\n```"
          |> Async.AwaitTask
          |> Async.Ignore
      }