#r "nuget: Akka.FSharp"
#r "nuget: Akka.TestKit"

open System
open Akka.FSharp
open Akka.Actor
open System.Threading

let system =
    System.create "system" (Configuration.defaultConfig ())

type command =
    | Construct of string * int
    | Routing of string * string * int
    | Join of string * int
    | ChangeRT of string []
    | Start of int * int

let mutable networkMap: Map<String, IActorRef> = Map.empty
let mutable hopsMap: Map<String, double []> = Map.empty
let mutable numNodes = 0
let mutable numRequests = 0
let mutable numDigits = 0
let mutable stop = true
let mutable stop1 = false

let NetworkPeer (mailbox: Actor<_>) =
    let mutable id = ""
    let mutable routingRows = 0
    let mutable routingColumns = 16
    let mutable routingTable: string [,] = Array2D.zeroCreate 0 0
    let mutable leafSet = Set.empty
    let mutable comLen = 0
    let mutable currRow = 0

    let rec loop () =
        actor {
            let! msg = mailbox.Receive()

            match msg with
            | Join (key, currentIndex) ->
                //printfn "In JOIN ID: %s" id
                let mutable i = 0
                let mutable j = 0
                let mutable k = currentIndex
                while key.[i] = id.[i] do
                    i <- i + 1
                comLen <- i
                let mutable routingRow = Array.zeroCreate 0
                while k <= comLen do
                    routingRow <- routingTable.[k, *]

                    let index1 =
                        Int32.Parse(id.[comLen].ToString(), Globalization.NumberStyles.HexNumber)

                    routingRow.[index1] <- id
                    networkMap.[key] <! ChangeRT(routingRow)
                    k <- k + 1
                let mutable rtrow = comLen

                let mutable rtcol =
                    Int32.Parse(key.[comLen].ToString(), Globalization.NumberStyles.HexNumber)

                if routingTable.[rtrow, rtcol] = null then
                    routingTable.[rtrow, rtcol] <- key
                else
                    networkMap.[routingTable.[rtrow, rtcol]]
                    <! Join(key, k)
            | Routing (key, source, hops) ->
                if key = id then
                    if hopsMap.ContainsKey(source) then
                        let mutable tempActorHops = hopsMap.[source]
                        let totalHops = double tempActorHops.[1]
                        let averageHops = double tempActorHops.[0]
                        tempActorHops.[0] <- ((averageHops * totalHops) + double hops)
                                             / (totalHops + 1.0)
                        tempActorHops.[1] <- totalHops + 1.0
                        hopsMap <- hopsMap.Add(source, tempActorHops)
                    else
                        let tempArray = [| double hops; 1.0 |]
                        hopsMap <- hopsMap.Add(source, tempArray)
                elif leafSet.Contains(key) then
                    networkMap.[key] <! Routing(key, source, hops + 1)
                else
                    let mutable i = 0
                    let mutable j = 0
                    while key.[i] = id.[i] do
                        i <- i + 1
                    comLen <- i
                    let mutable check = 0
                    let mutable rtrow = comLen

                    let mutable rtcol =
                        Int32.Parse(key.[comLen].ToString(), Globalization.NumberStyles.HexNumber)

                    if routingTable.[rtrow, rtcol] = null then rtcol <- 0
                    networkMap.[routingTable.[rtrow, rtcol]]
                    <! Routing(key, source, hops + 1)
            | Construct (nodeid, rows) ->
                id <- nodeid
                routingRows <- rows
                routingTable <- Array2D.zeroCreate routingRows routingColumns
                let mutable itr = 0

                let number =
                    Int32.Parse(nodeid, Globalization.NumberStyles.HexNumber)

                let mutable left = number
                let mutable right = number
                while itr < 8 do
                    if left = 0 then left <- networkMap.Count - 1
                    leafSet <- leafSet.Add(string left)
                    itr <- itr + 1
                    left <- left - 1
                while itr < 16 do
                    if right = networkMap.Count - 1 then right <- 0
                    leafSet <- leafSet.Add(string right)
                    itr <- itr + 1
                    right <- right + 1

            | ChangeRT (row) ->
                routingTable.[currRow, *] <- row
                currRow <- currRow + 1

            | _ -> ()

            return! loop ()
        }

    loop ()

let spawner (mailbox: Actor<_>) =
    let rec loop () =
        actor {
            let! msg = mailbox.Receive()

            match msg with
            | Start (nn, nr) ->
                numNodes <- nn
                numRequests <- nr
                numDigits <- int (ceil (Math.Log(double numNodes) / Math.Log(double 16)))
                printfn "Network Construction Initiated."
                let mutable nodeId = ""
                let mutable hexNum = ""
                let mutable len = 0
                nodeId <- String.replicate numDigits "0"

                let mutable actor =
                    spawn system (sprintf "actor%s" nodeId) NetworkPeer

                networkMap <- networkMap.Add(nodeId, actor)
                //printfn "PLEASE NODEID: %s" nodeId
                actor <! Construct(nodeId, numDigits)
                for i = 2 to numNodes do
                    if i = numNodes / 10
                    then printfn "10 percent of network is constructed."
                    elif i = numNodes / 2
                    then printfn "50 percent of network is constructed."
                    elif i = numNodes * 4 / 5
                    then printfn "80 percent of network is constructed."

                    hexNum <- i.ToString("X")
                    len <- hexNum.Length
                    nodeId <- (String.replicate (numDigits - len) "0") + hexNum
                    //printfn "PLEASE NODEID: %s" nodeId
                    actor <- spawn system (sprintf "actor%s" nodeId) NetworkPeer
                    actor <! Construct(nodeId, numDigits)
                    networkMap <- networkMap.Add(nodeId, actor)
                    networkMap.[String.replicate numDigits "0"]
                    <! Join(nodeId, 0)
                    Thread.Sleep(5)
                let mutable flag1 = false
                Thread.Sleep(1000)
                printfn "100 percent of the network has been built."
                let mutable actorsArray = Array.empty
                for entry in networkMap do
                    let temp = [| entry.Key |]
                    actorsArray <- Array.append actorsArray temp
                printfn "Processing Requests"
                let mutable k = 1
                let mutable destinationId = ""
                let mutable counter = 0
                while k <= numRequests do
                    for sourceId in actorsArray do
                        counter <- counter + 1
                        destinationId <- sourceId
                        while destinationId = sourceId do
                            destinationId <- actorsArray.[Random().Next(0, numNodes)]
                        networkMap.[sourceId]
                        <! Routing(destinationId, sourceId, 0)
                        Thread.Sleep(5)
                    printfn "%i requests perfomed" k
                    k <- k + 1
                Thread.Sleep 1000
                printfn "Requests processed"
                let mutable totalHopSize = double 0
                for entry in hopsMap do
                    totalHopSize <- totalHopSize + double entry.Value.[0]
                printfn "Average Hop Size: %f" (totalHopSize / double hopsMap.Count)
                stop1 <- true

            | _ -> ()

            return! loop ()
        }

    loop ()

let arguments = Environment.GetCommandLineArgs()
let a = arguments.[3] |> int
let b = arguments.[4] |> int
let spawnerId = spawn system "spawner" spawner
spawnerId <! Start(a, b)

while stop do
    if stop1 then stop <- false
