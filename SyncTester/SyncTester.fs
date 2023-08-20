open System.Net.Http
open System.Net.Http.Json
open System.IO
open System.Text

open Newtonsoft.Json
open Newtonsoft.Json.Linq

task {
    use client = new HttpClient ()
    let! response =  client.PostAsJsonAsync ("http://localhost:5000/finished-job/1/1", {| Foo = "bar"  |})
    let! c = response.Content.ReadAsStringAsync ()
    let r = JsonConvert.DeserializeObject<Shared.Msg> c
    printfn $"{r}"
    //do! File.WriteAllTextAsync("./response.html", response)
    return ()
}
|> Async.AwaitTask
|> Async.RunSynchronously
