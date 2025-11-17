#include <algorithm>
#include <cstdio>
#include <ctime>

#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>
#include <chrono>
#include <sys/stat.h>
#include <sys/types.h>
#include <utility>
#include <vector>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <mutex>
#include <stdlib.h>
#include <unistd.h>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>
#include <glog/logging.h>

#include "coordinator.grpc.pb.h"
#include "coordinator.pb.h"

using google::protobuf::Timestamp;
using google::protobuf::Duration;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;
using csce438::CoordService;
using csce438::ServerInfo;
using csce438::Confirmation;
using csce438::ID;
using csce438::ServerList;
using csce438::SynchService;

//#define log(severity, msg) LOG(severity) << msg; google::FlushLogFiles(google::severity);


struct BaseNode{
    std::string hostname;
    std::string port;
    std::string type;
    std::time_t last_heartbeat;
    bool missed_heartbeat;
    bool isActive();

};

struct zNode: public BaseNode {
    int serverID;

};

// node for synchronizers
struct syncNode: public BaseNode {
    int syncID;
};



//potentially thread safe 
std::mutex v_mutex;
std::vector<zNode*> cluster1;
std::vector<zNode*> cluster2;
std::vector<zNode*> cluster3;

// creating a vector of vectors containing znodes
std::vector<std::vector<zNode*>> clusters = {cluster1, cluster2, cluster3};




// synchronizer nodes vector
// Do we need the mutex here as well?
std::mutex sync_v_mutex;

std::vector<syncNode*> sync_cluster1;
std::vector<syncNode*> sync_cluster2;
std::vector<syncNode*> sync_cluster3;
std::vector<std::vector<syncNode*>> sync_clusters = {sync_cluster1, sync_cluster2, sync_cluster3};


//func declarations
int findServer(std::vector<zNode*> v, int id); 
std::time_t getTimeNow();
void checkHeartbeat();


bool BaseNode::isActive(){
    bool status = false;
    if(!missed_heartbeat){
        status = true;
    }else if(difftime(getTimeNow(),last_heartbeat) < 10){
        status = true;
    }
    return status;
}


/*

    int32 serverID = 1;
    string hostname = 2;
    string port = 3;
    string type = 4;
    int32 clusterID = 5;
    bool isMaster = 6;

*/

class CoordServiceImpl final : public CoordService::Service {

    Status Heartbeat(ServerContext* context, const ServerInfo* serverinfo, Confirmation* confirmation) override {
        // Your code here
        // The cluster index starts from 0.
        

        int cluster_id = serverinfo -> clusterid() - 1;
        if (cluster_id < 0 || cluster_id >= (int) clusters.size()){
            LOG(ERROR) << "cluster_id not valid";
            return Status::OK;
        }

        bool isSync = (serverinfo -> type() == "synchronizer");
        std::time_t now = getTimeNow();


        std::lock_guard<std::mutex> guard(isSync ? sync_v_mutex : v_mutex);
        //std::cout << "entered mutex" << std::endl;

        bool updated = false;


        // declare a BaseNode pointer for shared fields
        BaseNode* node = nullptr;

        if (isSync) {
            auto* sync_node = new syncNode();
            sync_node->syncID = serverinfo->serverid();
            node = sync_node; // assign to base pointer
        } else {
            auto* z_node = new zNode();
            z_node->serverID = serverinfo->serverid();
            node = z_node;
        }

     
        // shared initialization
        node->hostname = serverinfo->hostname();
        node->port = serverinfo->port();
        node->type = serverinfo->type();
        node->last_heartbeat = now;
        node->missed_heartbeat = false;



        if (isSync) { // synchornization node
            for (auto& s : sync_clusters[cluster_id]) {
                if (s->syncID == static_cast<syncNode*>(node)->syncID) {
                    s->last_heartbeat = now;
                    s->missed_heartbeat = false;
                    updated = true;
                    delete node;
                    break;
                }
            }
            if (!updated) {
                sync_clusters[cluster_id].push_back(static_cast<syncNode*>(node));
                LOG(INFO) << "Added synchronizer " << static_cast<syncNode*>(node)->syncID << " to cluster " << cluster_id;
            }
        } else { // the server nodes
            for (auto& s : clusters[cluster_id]) {
                if (s->serverID == static_cast<zNode*>(node)->serverID) {
                    s->last_heartbeat = now;
                    s->missed_heartbeat = false;
                    updated = true;
                    delete node;
                    break;
                }
            }
            if (!updated) {
                clusters[cluster_id].push_back(static_cast<zNode*>(node));
                LOG(INFO) << "Added server " << static_cast<zNode*>(node)->serverID << " to cluster " << cluster_id;
            }
        }

        return Status::OK;
    }

    

    //function returns the server information for requested client id
    //this function assumes there are always 3 clusters and has math
    //hardcoded to represent this.
    Status GetServer(ServerContext* context, const ID* id, ServerInfo* serverinfo) override {
        // Your code here

        // link back to the serverinfo the send the message
        /*
            int32 serverID = 1;
            string hostname = 2;
            string port = 3;
            string type = 4;
        */
        //modulus operation

        int raw_id = id -> id();
        int PORT_SERVER = 10000;
        int PORT_SYNC= 9000;
        if (raw_id >= PORT_SYNC && raw_id < PORT_SERVER){
             // TODO: The logic is a little different here (from the server nodes)
             int clusterId = (id -> id() / PORT_SYNC) - 1;
             // Does follower synchronizer master needs to know where the slave is?
             // Master may not need to know this information, will change the structure later.
        }
        else if (raw_id >= PORT_SERVER){ // Asking for the existence of slave
            // find the cluster, take out the inforamtion in the second node
            int clusterId = (id -> id() / PORT_SERVER) -1 ;
            // If there is only one node in the cluster, may cause segmentation fault
            if (clusters[clusterId].size() > 1) {
                zNode* destServerInfo = clusters[clusterId][1];
                if (destServerInfo -> isActive()){
                    serverinfo->set_serverid(destServerInfo->serverID);
                    serverinfo->set_hostname(destServerInfo->hostname);
                    serverinfo->set_port(destServerInfo->port);
                    serverinfo->set_type(destServerInfo->type);
                }
            }
            else {
                LOG(ERROR) << "There is no slave in this cluster.";
                return grpc::Status(grpc::StatusCode::UNAVAILABLE, "There is no slave in this cluster.");
            }

        }
        else {

            int clusterId = ((id -> id() - 1) % 3);
            int serverIndex = 0; // There is always one entry in each znode, will be updated later...
            zNode* destServerInfo = clusters[clusterId][serverIndex];
            
            // setting the information for the client to fetch
            // use protobuf setters with arguments
            if (destServerInfo -> isActive()){
                serverinfo->set_serverid(destServerInfo->serverID);
                serverinfo->set_hostname(destServerInfo->hostname);
                serverinfo->set_port(destServerInfo->port);
                serverinfo->set_type(destServerInfo->type);

            }
            else {
                // std::cout << "The server is not active..." << std::endl;
                LOG(ERROR) << "No active server in the cluster";
                return grpc::Status(grpc::StatusCode::UNAVAILABLE, "No active server in the cluster.");
            }



        }


        return Status::OK;
    }


};

void RunServer(std::string port_no){
    //start thread to check heartbeats
    std::thread hb(checkHeartbeat);
    //localhost = 127.0.0.1
    std::string server_address("127.0.0.1:"+port_no);
    CoordServiceImpl service;
    //grpc::EnableDefaultHealthCheckService(true);
    //grpc::reflection::InitProtoReflectionServerBuilderPlugin();
    ServerBuilder builder;
    // Listen on the given address without any authentication mechanism.
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    // Register "service" as the instance through which we'll communicate with
    // clients. In this case it corresponds to an *synchronous* service.
    builder.RegisterService(&service);
    // Finally assemble the server.
    std::unique_ptr<Server> server(builder.BuildAndStart());
    LOG(INFO) << "Server listening on " << server_address << std::endl;
    //std::cout << "Server listening on " << server_address << std::endl;

    // Wait for the server to shutdown. Note that some other thread must be
    // responsible for shutting down the server for this call to ever return.
    server->Wait();
}

int main(int argc, char** argv) {
    
    std::string port = "3010";
    int opt = 0;
    while ((opt = getopt(argc, argv, "p:")) != -1){
        switch(opt) {
            case 'p':
                port = optarg;
                break;
            default:
                std::cerr << "Invalid Command Line Argument\n";
        }
    }
    std::string log_file_name = std::string("coordinator-") + port;

    // /usr/local/include/glog/logging.h
    google::InitGoogleLogging(log_file_name.c_str());

    FLAGS_log_dir = "./logs/coordinator/";
    std::filesystem::create_directories(FLAGS_log_dir);
    //FLAGS_logtostderr = 1;
    FLAGS_alsologtostderr = 1;   // and also to stderr (terminal)
    FLAGS_colorlogtostderr = 1;  // optional: colored terminal logs
    //FLAGS_stderrthreshold = google::GLOG_FATAL;  // only FATAL to stderr
    FLAGS_logbufsecs = 0;  // set once after InitGoogleLogging

    LOG(INFO) << "Coordinator starting...";

    RunServer(port);
    return 0;
}


template<typename NodeT>
void checkHeartbeatGeneric(std::vector<std::vector<NodeT*>>& clusters_vec,
                           std::mutex& mtx,
                           std::function<int(NodeT*)> getId,
                           const std::string& label) {
    while (true) {
        // // We release the lock when the thread is sleeping
        // holding the lock when it sleep will cause lock contention + blocking
        { 
            std::lock_guard<std::mutex> guard(mtx);
            for (auto& cluster : clusters_vec) {
                for (auto& node : cluster) {
                    if (difftime(getTimeNow(), node->last_heartbeat) > 10) {
                        std::cout << "missed heartbeat from " << label << " " << getId(node) << std::endl;
                        if (!node->missed_heartbeat) {
                            node->missed_heartbeat = true;
                            node->last_heartbeat = getTimeNow();
                        }
                    }
                }
            }
        }
        sleep(5);
    }
}

// from generic template to more specific templates
void checkHeartbeat() {
    checkHeartbeatGeneric<zNode>(clusters, v_mutex,
        [](zNode* n){ return n->serverID; }, "server");
}

void checkSyncHeartbeat() {
    checkHeartbeatGeneric<syncNode>(sync_clusters, sync_v_mutex,
        [](syncNode* n){ return n->syncID; }, "synchronizer");
}

std::time_t getTimeNow(){
    return std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
}

