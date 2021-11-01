# CHORD PEER TO PEER PROTOCOL

This project implements the peer to peer architecure using the CHORD protocol. Further, the project incorporates the AKKA actor model in F# to create a distributed actor based communication system. 
To know more about the protocol, please visit: [Chord P2P research paper from MIT.
](https://en.wikipedia.org/wiki/Chord%28peer-to-peer%29) To now abut the AKKA model please visit: 
[Akka Documentation](https://doc.akka.io/docs/akka/current/typed/guide/introduction.html)
# To run the project

 Please download the zip **Chord_P2P.zip** and extract it at your desired location.
 
 The **BONUS** implementation is also integrated in the same code base. Simply set the last command line argument to `"yes"`.
 
 Run  `dotnet fsi <filename> <numPeers> <numRequests> <deletion>` where
  
 1. `filename` : The name of the file to run, here ***chord_p2p.fsx***
 2. `numPeers` : The number of peers to join in the network
 3. `numRequests`: The number of messages to be sent by each peer.
 4. `deletion`: Takes the values `"yes"` or `"no"` where the former enables a 20% node deletion and network restabalization and the latter just simulates the entire network without any failure.

## Sample output snippets

 1. `dotnet fsi chord_p2p.fsx 20000 5 yes` 
 
  ![20000 peers 5 requests deletion=yes](https://github.com/amanagarwal19/Chord_P2P/blob/89d5b88c330dba095559f4f893f02c7288890bd9/sample_screenshots/Screenshot%202021-10-31%20at%203.25.30%20PM.png)
 
 2.  `dotnet fsi chord_p2p.fsx 15000 5 no`

  ![15000 peers 5 requests deletion=no](https://github.com/amanagarwal19/Chord_P2P/blob/89d5b88c330dba095559f4f893f02c7288890bd9/sample_screenshots/Screenshot%202021-10-30%20at%2010.43.59%20PM.png)

#### Thank you :)
