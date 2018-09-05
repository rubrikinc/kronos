# Cluster HTTP handler                                                           
                                                                                 
## Add Node                                                                      
Add a new node to the raft cluster of kronos.

POST: https://address:raft_port/cluster/add

Input: AddNodeRequest with NodeID, Address in JSON format, e.g.,
```
{"node_id":"5085e631629d4560","address":"127.0.0.1:6766"}
```
 Output: None
                                                                                 
## Remove Node                                                                   
                                                                                 
POST: https://address:raft_port/cluster/remove                                   
                                                                                 
Remove a node from the raft cluster of kronos.                                  
                                                                                 
Input: RemoveNodeRequest with NodeID in JSON format, e.g.,                       
```
{"node_id":"5085e631629d4560"}
```                                                                          
Output: None

## Get Nodes

GET: https://address:raft_port/cluster/nodes

Get all the nodes currently part of the cluster according to the server.

Output: A list of nodes. The list can be incomplete when the cluster is 
bootstrapping and multiple nodes are being added simultaneously.

```
[
  {
    "node_id": "1",
     // JSON representation of kronospb.Node.
  },
  {
    "node_id": "2",
    // ...
  }
]
```


