### Introduction
This is the TinySNS system implemented in the distributed system course. Each assignments builds on each other. 

**HW1:** Implemented server-side functionality for **LOGIN, FOLLOW, UNFOLLOW, LIST, TIMELINE**, and persistent message storage. The client communicated with the server through RPC calls using the **gRPC** framework.

**HW2.1:** Built on HW1 by implementing a **coordinator service**. Servers registered themselves with the coordinator, and clients contacted the coordinator to locate an available server. The coordinator returned the server’s port so the client could redirect its connection. Implement Heartbeat Mechanism to ensure the liveness of server. 

**HW2.2:** Extended HW2.1 by adding a **master/slave process model** within each cluster. Used **RabbitMQ** to synchronize information across clusters. When the master process in a cluster failed, the slave process took over as the new master.

### Running the Program
To run mp1 and mp2-1, one can pull the following image from docker hub: 

```
docker pull your_dockerhub_username/csce662_env_snapshot:latest
```

To run mp2-2, the situtaion will be more complicated since it involves rabbitmq setup. One will need to have the container in the previous step first, and then follow the README.md in mp2-2/
