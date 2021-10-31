#r "nuget: Akka.FSharp"
// Load the necessary libraries
open System
open System.Threading
open Akka.Actor
open System.Security.Cryptography
open Akka.Configuration
open System.Text
open Akka.FSharp
open System.Diagnostics
open Akka.Event
open Akka.Util
open System.Collections.Generic

let defaultNodes = 100
let defaultRequests = 10

// //Command line args
let numPeers = fsi.CommandLineArgs.[1]|>int;
let numRequests = fsi.CommandLineArgs.[2]|>int;
let failure = fsi.CommandLineArgs.[3] 
let mutable numFailure = (0.2*(numPeers|>float)) |> int
let mutable actorNameMap = Map.empty
let logN = Math.Log2(numPeers|>float)
//TODO
// let numPeers = defaultNodes
// let requests = defaultRequests
// let failure ="no"

let config = ConfigurationFactory.ParseString("""
      akka {
          loglevel = INFO
      }""")

let mutable counter=0;
let m=5
let environmentSpace = pown 2 25
let path="akka://system/user/"

// USER VARIABLES
let mutable first = -1
let timer = Stopwatch()
let mutable nodeStatus : AtomicBoolean = AtomicBoolean(true)
let mutable nodeToStart=0
let mutable messagesHeard =0                    
let mutable totalHops = 0
let mutable nodeOrderMap = Map.empty
let mutable ordering =1
let mutable total = 0
let mutable rejoined = false
let waitFor(value:int) = 
    System.Threading.Thread.Sleep(value)

////----------------------------------------------------------------------------------------
/// ----------------------------------------------------------------------------------------
/// ----------------------------------------------------------------------------------------
type MessageFormat =
    |Greet of String
    |Init
    |Terminate
    |StartMessage
    |Transmit of int
    |CreateChordNetwork
    |Join of int*int
    |Display
    |PrintResult
    |SetChainSuccessor of int*int
    |Encrypt
    |GetSuccessor of bool
    |SetSuccessor of int
    |SetPredecessor of int
    |CheckSuccessor of int*int
    |CheckPredecessor of int
    |StartMessageTransfer
    |FixFingers
    |Stabalize of int
    |FindFinger of int
    |Notify
    |FindSuccessor of int*int
    |FindChainSuccessor
    |JoinNextActors of int
    |UpdateFingerTable of int

//Initialise the actor system
let system = System.create  "system" config

let findClosestPrecedingNeighbour(nodeToFind:int,startNode:int)=
    
    let router = system.ActorSelection(path+(string startNode))
    router<!FindFinger(nodeToFind)

let encrypt (input:int) =

    let byteArr = Encoding.ASCII.GetBytes(string input)
    let encoder = SHA1.Create()
    
    let ans = encoder.ComputeHash(byteArr)
    let stringVersion = BitConverter.ToString(ans)
    let mutable intVersion = BitConverter.ToInt32(ans,0)

    intVersion <- (abs intVersion) % environmentSpace
    
    abs intVersion

let peerActors(mailbox:Actor<_>)=

    let mutable successor = -1
    let mutable predecessor = -1
    let mutable joined=false
    let mutable fingerTable = Map.empty
    let mutable hops = 0
    let mutable chainSuccessor = -1

    let printInfo = 
        for i=1 to total+1 do
            let viewActor = system.ActorSelection(path+(string i))
            viewActor<!Display

    let initFinger(n:int)=
        for i = 1 to m do
            fingerTable<-fingerTable.Add(i,n)

    let rec loop() = actor{
        let! msg = mailbox.Receive()
        let sender = mailbox.Sender()        

        let self = mailbox.Self
        let name = self.Path.Name|>int
        match msg with
        |Greet s-> printfn "I am  Peer \n"

        
        |FindFinger(nodeID) ->  // Called by the node of which we will check the finger table, or else reroute to next node
            
            if total < numPeers then
                printfn "CHECKING FINGER TABLE OF %d " name
                //check your finger table if available
                let mutable flag=0
                let mutable finalAns =0

                for i=1 to m do
                    
                    if fingerTable.Item(i) = nodeID then
                        flag<-1
                        finalAns <- fingerTable.Item(i)
                        printfn "FOUND FINGER AT ROUTER %d" name


                // not found in current router
                if flag=0 then
                    printfn "Not found at router %d\n Finding next potential router....." name
                
                    let mutable ans = -1
                    let mutable k=m

                    // FIND THE FARTHEST NODE FROM CURRENT NODE WHICH IS LESSER THAN THE DESIRED NODE
                    while k >0 do
                        printfn "CHECKING FARTHEST NODE....."
                        let mutable temp=fingerTable.Item(k)
                        if temp<> -1 then // value exists 
                            if temp > nodeID then
                                k<-k-1   //continue checking
                            else
                                ans <- temp
                                printfn "FARTHEST NODE FOUND"
                                k<-k-1
                        else    // value -1, keep checking rest of table
                            k<-k-1

                    let mutable currentPosition=0
                    let mutable nextPosition = 0
                    
                    // SCENARIO WHERE THE FINGER TABLES OF THE NODES DONT POINT TO EACH OTHER. INITIALLY SPARSE.          
                    // IN THIS DEADLOCK SCENARIO, CHECK FOR THE NEXT AVAILABLE NODE AND FIND IT'S FINGER INDEX
                    // ELSE CHECK FOR THE FOUND NODE'S FINGER INDEX
                    if ans = -1 then
               

                        for p = 1 to nodeOrderMap.Count do
                            if nodeOrderMap.Item(p) = name then
                                currentPosition <- p      
                        printfn "NODE FOUND, REROUTING..."
                        if currentPosition <> nodeOrderMap.Count then
                            nextPosition <- currentPosition + 1
                            let nextActor = nodeOrderMap.Item(nextPosition)
                            let nextActorRef = system.ActorSelection(path+(string nextActor))
                            printfn "NEXT ROUTING %d ---> %d" name nextActor  
                            nextActorRef <! FindFinger(nodeID)
                        
                            
                            // TEMPORARILY HAULT THE THREAD AND WAIT FOR OTHER NODES TO JOIN AND RESTART THE FIND FINGER WITH UPDATED TABLES
                            // self<!FindFinger(nodeID)

                    // Found item in finger table
                    else
                        let nextActorRef = system.ActorSelection(path+(string ans))
                        nextActorRef<!FindFinger(nodeID)
                        printfn "NEXT ROUTING %d ---> %d" name ans  

                    // RECURSE OVER THE CHORD NETWORK TO FIND THE NODE WITH THE FINGER
                     
                else
                    //CLOSEST NEAREST NEIGHBOUR FOUND: UPDATE THE PREDECESSOR OF THE NEW NODE TO THIS NODE 
                    // AND UPDATE THE SUCCESSOR OF THE FOUND NODE TO THE NEW NODE
                    let ref = system.ActorSelection(path+(string finalAns))
                    //UPDATE OLD SUCCESSOR TO STORE THE NEW PREDECESSOR
                    ref<!SetSuccessor(nodeID)
                    
                    //notify the other peers that a new node has arrived
                    printfn "Updating others"
                    system.ActorSelection(path+(string nodeID))<!Notify

            else
                printfn "All neighbours found successfully"

        |Notify ->
            let mutable k=0
            while k<total do
               let actor = system.ActorSelection(path+string(k))
               if predecessor= -1 && name > predecessor then
                    actor<!FixFingers
    
                  
        |FixFingers->
            let mutable next = 1
            while next < m do
                let temp = fingerTable.Item(next)

                if next > m then
                    let value = fingerTable.Item(next)
                    let actorRef = system.ActorSelection(path+(string value))
                    let distanceValue = pown 2 (next-1)
                    let fixFingerDistance = name + distanceValue
                    actorRef <! FindSuccessor(name,fixFingerDistance)
            next <- next+1
        
        |Join (actorID,joiningActor) -> 
            

            // First Node
            if actorID = -1 then
 

                successor<-name
                predecessor<-name
                joined<-true
                total<-total+1
                initFinger(-1)
                // self<!Display
                printfn "First Node Initialized"
                nodeOrderMap<-nodeOrderMap.Add(ordering,actorID)
                printfn "TOTAL =: %d" total
                ordering<-ordering+1
            else    
                
                initFinger(-1)
                nodeToStart<-actorID // for recursive calls, start at root for every new incoming node
                printfn "%d Joining %d\n" joiningActor actorID
                self<!JoinNextActors(actorID)
                printfn "Initialized node %d" joiningActor
                nodeOrderMap<-nodeOrderMap.Add(ordering,actorID)
                ordering<-ordering+1
                nodeStatus.Value <- not nodeStatus.Value
                total<-total+1
                printfn "TOTAL =: %d" total


        |JoinNextActors (seed) ->
            let seeder = system.ActorSelection(path+(string seed))
            seeder<!FindSuccessor(name,seed)

            

        |FindSuccessor(node,seed) ->
            printfn "Finding successor for %d" node
            let startActor = system.ActorSelection(path+(string seed))
            let nodetoAdd = system.ActorSelection(path+(string node))

            // At this point only one node has joined
            if total=1 then
                nodetoAdd<!SetPredecessor(seed)
                nodetoAdd<!SetSuccessor(seed)
                startActor<!SetPredecessor(node)
                startActor<!SetSuccessor(node)  
            else
                
                
                // SCENARIO 1: INCOMING NODE BETWEEN THE ROOT AND SUCCESSOR, SIMPLY INSERT AND UPDATE POINTERS AND FIX FINGERS
             
                if node > seed && node < successor then // check if the node is between the seed and it's successor
                    printfn "found easy node"
                    nodetoAdd<!SetSuccessor(successor)
                    nodetoAdd<!SetPredecessor(seed)
                    let act = system.ActorSelection(path+(string successor))
                    act<!SetPredecessor(node)
                    startActor<!SetSuccessor(node)

                // SCENARIO 2: INCOMING NODE IS IN THE OTHER ARC OF THE CHORD WHICH IS AFTER THE ROOT'S SUCCESSOR AND BEFORE ROOT

                else
                    if total<numPeers then
                        printfn "Finding the closest preceeding neighbour and udpdating pointers"
                   
                        if node > seed && node < environmentSpace then
                            findClosestPrecedingNeighbour(node,nodeToStart)
                    else
                        printfn "Updated nearest neighbours"    
                

        |Stabalize (node) ->
            let x = successor
            let y = system.ActorSelection(path+(string x))
            
            //FIND THE SUCCESSOR OF THE SUCCESSOR
            y<!FindSuccessor
            y<!Stabalize
            
            //IF THIS NODE BELONGS BETWEEN THE CURRENT NODE AND ITS SUCCESSOR, THEN
            // THEN THE NEW NODE WILL UPDATE THE SUCCESSOR TO THIS NODE'S SUCCESSOR
            // AND THE PREVIOUS SUCCESSOR WILL UPDATE THE PREDECESSOR TO THIS NODE
            if name = node then
                if x < name then
                    successor <- name
                // ALSO NOTIFY ALL PEERS TO UPDATE THE FINGER TABLE
                let act = system.ActorSelection(path+(string successor))
                act<!Notify

        |SetPredecessor (newPredecessor) ->
            printfn "setting predecessor of %d to %d"name newPredecessor
            predecessor <- newPredecessor

            
        |SetSuccessor (newSuccessor) ->
            printfn "setting Succecessor of %d to %d" name newSuccessor
            successor <-newSuccessor

            self<!UpdateFingerTable(newSuccessor) //update your finger table store the new successor 
            
        |UpdateFingerTable(newSuccessor)->
            printfn "updating finger table"
            let myNodeValue = name
            for i=1 to m do
                let mutable distance = ( myNodeValue + (pown 2 (i-1)) ) % environmentSpace
                if distance = newSuccessor then
                    fingerTable<-fingerTable.Add(i,newSuccessor)

            printfn "NODE JOINED CHORD"
            joined<-true

            self<!Display
        
        |StartMessage ->
            // SET INITIAL TRANSMITTER COUNTERS TO 1
            hops<-hops+1

            totalHops <- totalHops + hops

            for i =1 to numRequests do
                //GENERATE A UNIQUE MESSAGE FROM EACH PEER TO EVERY OTHER PEER
                // WE CONCATENATE EVERY PEER'S NAME WITH THE MESSAGE NUMBER TO GENERATE A UNIQUE SHA MESSAGE
                if messagesHeard < (numPeers*numRequests) then
                    let messageTextToEncrypt = name + i
                    let encryptedMessage = encrypt(messageTextToEncrypt)
                    printfn "Message %d transmitting for actor %d" messageTextToEncrypt name
                    
                    //AFTER EVERY ONE SECOND REINITIATE THE TRANSFER
                    waitFor(1000)
                    self<!Transmit(encryptedMessage)

        |Transmit(incomingMessage) ->
            // SINCE THE MESSAGE IS HEARD, INCREASE HOPS
            // AND THE TOTAL MESSAGES HEARD
            hops<-hops+1
            totalHops <- totalHops + 1
            messagesHeard<-messagesHeard+1
            printfn "Message Heard from actor %d" name
            // FIND MY SUCCESSOR AND FORWARD THE MESSAGE
            // if ((name = successor) && (messagesHeard <> (numPeers*numRequests))) then
            self<!CheckSuccessor(self.Path.Name|>int,incomingMessage)


        |FindChainSuccessor ->
            //Update the successor of the deleted node to the successor of its parent
            if successor <> -1 then
                self<! SetChainSuccessor(name,successor)
            else
                // FIND NEXT SUPER SUCCESSOR
                self<!FindChainSuccessor

            rejoined<-true
            waitFor(100)

        |SetChainSuccessor (node,newSuccessor) ->
            // WHILE UPDATING A SUCCESSOR'S PREDECESSOR TO POINT TO ITSELF
            // WE MUST CHECK IF THE NODE WHOSE SUCCESSOR WE ARE UPDATING IS 
            // ITSELF ALIVE OR NOT
            if actorNameMap.ContainsKey(node) && actorNameMap.Item(node) then
                chainSuccessor <- newSuccessor
                let ref = system.ActorSelection(path+(string chainSuccessor))
                rejoined<-true
                waitFor(100)
                totalHops <- totalHops+1
                ref<!FindChainSuccessor
            else
                let ref = system.ActorSelection(path+(string node))
                ref<!FindChainSuccessor


        |CheckSuccessor(name,message) ->
            //check the finger table to find the farthest node
            let mutable k=0
            while k < m do
                
                messagesHeard<-messagesHeard+1
                totalHops <- totalHops + 1

                if (fingerTable.ContainsKey(k) && fingerTable.Item(k)=name) then
                    printfn "Not found"
                else
                    printfn "forwarding message to next peer" 
                    totalHops <- totalHops + hops
                    if fingerTable.ContainsKey(k) && 
                       fingerTable.Item(k)<> -1 && 
                       (successor = name) then
                        let nextActor = system.ActorSelection(path+(string k))
                        nextActor<!Transmit(message)
            
                k<-k+1

        |Display -> 

            printfn "----------------------------------------"
            printfn "My name : %d" name 
            printfn "Successor : %d" successor
            printfn "Predecessor : %d" predecessor 
            printfn "Joined Chord : %A" joined 
            printfn "\nFinger Table : of %d %A" name fingerTable
            printfn "----------------------------------------"

              
        | (_) -> printfn "Default pattern for peerActor match";
        return! loop()
    }
    loop()


    
let boss(mailbox:Actor<_>)=
    let mutable timeElapsed =0
    let mutable timeElapsed2 =0
    let mutable averageHops=0.0
    let mutable heardMessages=0
    let mutable messageBuffer2=0
    let rec loop() = actor{
        let! msg = mailbox.Receive()
        let sender = mailbox.Sender()
        match msg with
        |Encrypt -> 
            for i =0 to numPeers-1 do
                printfn "%A" i

        |Greet s -> 
            printfn "\n\t---------------------------------------------------------------------------"
            printfn "\n\tStarting chord protocol with %d peers and %d requests with deletion = %s" numPeers numRequests failure
            printfn "\n\t\t\tTotal environment space : %d peers" environmentSpace
            printfn "\n\t\t\t\tUSING SHA-1 ENCRYPTION"
            printfn "\n\t---------------------------------------------------------------------------"
            System.Threading.Thread.Sleep(3000);      
            mailbox.Self<!Init

        |Init ->    
            timer.Start()
            let mutable actorName=0
            let mutable precentageCompleted=0

            for i = 1 to numPeers do   
                let r = Random()
                
                actorName <- encrypt(i)
                // A map to store all names so that if the SHA encryption results in same 
                // actor name, then the id gets modified
                while (actorNameMap.ContainsKey(actorName)) do
                    // if key already exists, give it a new name by adding a random factor
                    actorName <- encrypt (i+r.Next(1,5000))

                // Add this new actor to the system
                actorNameMap<-actorNameMap.Add(actorName,true)
                

                //implies starting of chord network
                if first = -1 then
                    first <- actorName
                    let node = spawn system (string first) peerActors
                    printfn "\nSpawning First actor: %d \n" first
                    node<!Join(-1,actorName)
                    printfn "back here"
                    System.Threading.Thread.Sleep(300)
                    counter<-counter+1
                else    
                        
                    printfn "\n\t\tSPAWNING NODE NUMBER : %d HAVING HASH ID: %d\n" counter actorName
                    let node = spawn system (string actorName) peerActors
                    counter<-counter+1
                    if i>=10 && (i% (numPeers/10)) =0 then
                        precentageCompleted<-precentageCompleted+10
                        printfn "\t\t\n\nCompleted %d percent " precentageCompleted
                        waitFor(100)
                        node<!Display
                    let currentNodeStatus = nodeStatus.Value
                    node<!Join(first,actorName)           

                    while currentNodeStatus = nodeStatus.Value && total<numPeers do
                        printfn "NODE %d UPDATING PREDECESSOR & SUCCESSOR" actorName
                        System.Threading.Thread.Sleep(10)

            // Wait for all nodes to finish joining
            while total<numPeers do
                let progress = (((total/numPeers)*100) |>int)
                printf "\t\t--------------NODES ARE STILL JOINING COMPLETED %d PERCENT" progress 

                System.Threading.Thread.Sleep(200);

            timer.Stop()
            System.Threading.Thread.Sleep(2000);
            if total = numPeers then
                printfn "\n\t---------------------------------"
                printfn "\t\t     JOIN COMPLETE\t" 
                
            timeElapsed <- timer.ElapsedMilliseconds |>int
            System.Threading.Thread.Sleep(5000);

            printfn "\n\t     SYSTEM BOOTUP TIME = %d ms\t" timeElapsed
            
            if failure="no" then
                numFailure<-0 
                printfn "\n\t     STARTING MESSAGE TRANSFER\t"
                printfn "\t---------------------------------"
                printfn "Total messages to be transferred = %d with 5 percent buffer" (numPeers*numRequests)
                waitFor(5000)
                mailbox.Self<!StartMessageTransfer

            else 
                printfn "Deletion is enabled... starting to terminate certain actors"
                waitFor(3000)
                let randomizer = Random()
                let outerBound = numPeers-numFailure
                let mutable deletedActorCount=0
                let mutable k=0
                while k<numFailure do 
                    // CHOOSE A RANDOM ACTOR BETWEEN THE TOTAL ACTOR MAP SIZE
                    let randomActorPosition = k+randomizer.Next(1,outerBound-1)
                    let randomActorHash = encrypt(randomActorPosition)
                    
                    // Reset the join flag to be broken
                    rejoined <- false
                    //CHECK IF THE ACTOR IS ALREADY DEAD OR NOT
                    if actorNameMap.ContainsKey(randomActorHash) && 
                        actorNameMap.Item(randomActorHash) then
                            printfn "Deleting actor with HASHID = %d" randomActorHash
                            deletedActorCount<-deletedActorCount+1
                            
                            // Terminate the actor status                           
                            actorNameMap<-actorNameMap.Add(randomActorHash,false)
                            
                            // Update pointers and reestabilish joins
                            let actorRef = system.ActorSelection(path+(string randomActorHash))
                            actorRef<!FindChainSuccessor
                            while not rejoined do
                                waitFor(10)
                                printfn "Reestabilishing join...."
                            k<-k+1
                            

                    else
                        printfn "actor %d couldn't be deleted, finding next candidate" randomActorHash

                    printfn "Actors deleted = %d" deletedActorCount   

                if deletedActorCount = numFailure then
                    printfn "Actor deletion complete"
                    printfn "\n\t     STARTING MESSAGE TRANSFER\t"
                    printfn "\t---------------------------------"
                    printfn "Total messages to be transferred = %d with 5 percent buffer and deletion" ((numPeers*numRequests)-numFailure)                  
                    waitFor(5000)
                    mailbox.Self<!StartMessageTransfer



        |StartMessageTransfer ->
            timer.Start()
            for i=1 to numPeers do
                let peer = encrypt(i)
                let peerInChord = system.ActorSelection(path+(string peer))
                peerInChord<!StartMessage


            let totalRequests = ((numPeers*numRequests)-numFailure)
            let messageBuffer = (0.05*(float)totalRequests) |> int 
            messageBuffer2 <- messageBuffer
            
            while messagesHeard<(totalRequests-messageBuffer) do
                printfn "Total messages heard (Including all actors) : %d / %d" messagesHeard ((numRequests*numPeers)-numFailure)
                waitFor(1)
            
            heardMessages <- messagesHeard

            if failure = "yes" then
                messagesHeard <- messagesHeard + numFailure
                
            timer.Stop()
            while messagesHeard < (totalRequests) do
                printfn "WAITING FR ALL THREADS TO GET DONE"
                waitFor(300)
            System.Threading.Thread.Sleep(10000);
            printfn "\nCOMPLETED MESSAGE TRANSFER"           
            printfn "\nCalculating statistics for result (System waiting 10 seconds)"
            System.Threading.Thread.Sleep(10000);
            
            timeElapsed2 <- timer.ElapsedMilliseconds/(2|>int64) |> int

            averageHops <- (totalHops / (totalRequests-messageBuffer)+1) |>float // +1 For box integer function

            mailbox.Self<!PrintResult


        |PrintResult ->
            printfn "\n\t\t---------------------------------"
            printfn "\t\t     PEERS JOINED: %d\t" total
            printfn "\t\t     JOINING TIME: %d ms\t" timeElapsed

            if failure="yes" then
                printfn "\t\t     FAILURE MODE : %s \t" "ON"
                printfn "\t\t     DELETED PEERS (20 percent): %d \t" numFailure

            printfn "\t\t     MESSAGES TRANSMITTED: %d\t" heardMessages
            printfn "\t\t     MESSAGE BUFFER: %d \t" messageBuffer2
            printfn "\t\t     TOTAL TRANSMITTING TIME: %d ms\t" timeElapsed2 
            printfn "\t\t     TOTAL HOPS: %d\t" totalHops
            printfn "\t\t     AVERAGE NUMBER OF HOPS: %.2f\t" averageHops
            printfn "\n\t\t---------------------------------"
            printfn "\n \nExiting..."
            
            
            mailbox.Self<!Terminate
            
        |Terminate -> 
            mailbox.Context.System.Terminate() |> ignore

        |(_) -> printfn "Default pattern for Boss Actor match";
        return! loop()
    }
    loop()

let bossRef = spawn system "boss" boss

bossRef<!Greet("GoBabyGo")

system.WhenTerminated.Wait()