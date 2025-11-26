// NOTE: This starter code contains a primitive implementation using the default RabbitMQ protocol.
// You are recommended to look into how to make the communication more efficient,
// for example, modifying the type of exchange that publishes to one or more queues, or
// throttling how often a process consumes messages from a queue so other consumers are not starved for messages
// All the functions in this implementation are just suggestions and you can make reasonable changes as long as
// you continue to use the communication methods that the assignment requires between different processes

#include <bits/fs_fwd.h>
#include <ctime>
#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>
#include <chrono>
#include <semaphore.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unordered_map>
#include <vector>
#include <unordered_set>
#include <filesystem>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <mutex>
#include <stdlib.h>
#include <stdio.h>
#include <cstdlib>
#include <unistd.h>
#include <algorithm>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>
#include <glog/logging.h>
#include "sns.grpc.pb.h"
#include "sns.pb.h"
#include "coordinator.grpc.pb.h"
#include "coordinator.pb.h"

#include <amqp.h>
#include <amqp_tcp_socket.h>
#include <jsoncpp/json/json.h>

#define log(severity, msg) \
    LOG(severity) << msg;  \
    google::FlushLogFiles(google::severity);

namespace fs = std::filesystem;

using csce438::AllUsers;
using csce438::Confirmation;
using csce438::CoordService;
using csce438::ID;
using csce438::ServerInfo;
using csce438::ServerList;
using csce438::SynchronizerListReply;
using csce438::SynchService;
using google::protobuf::Duration;
using google::protobuf::Timestamp;
using grpc::ClientContext;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;
// tl = timeline, fl = follow list
using csce438::TLFL;
using csce438::SNSService; // connect coordinator -> server
using csce438::Empty;
using csce438::CoordConfirmation;

int synchID = 1;
int clusterID = 1;
bool isMaster = false;
int total_number_of_registered_synchronizers; // update this by asking coordinator
std::string coordAddr;
std::string clusterSubdirectory;
std::vector<std::string> otherHosts;
std::unordered_map<std::string, int> timelineLengths;

std::vector<std::string> get_lines_from_file(std::string);
std::vector<std::string> get_all_users_func(int);
std::vector<std::string> get_tl_or_fl(int, int, bool);
std::vector<std::string> getFollowersOfUser(int);
bool file_contains_user(std::string filename, std::string user);

// sanitizing semaphore components
std::string sanitizeSemComponent(const std::string &input) {
    std::string output;
    for (char c : input) {
        if (c == '/'){
            output += "_";
        }
        else{
            output += c;
        }
    }
    return output;
}


std::string makeSemaphoreName(const std::string &filename) {
    // for instance, filename = "cluster/1/1/all_users.txt"
    // should return "/1_1_all_users.txt"
    std::string base = std::to_string(clusterID) + "_" + clusterSubdirectory + "_" + filename;
    return "/" + sanitizeSemComponent(base);
}

struct SynchronizerRegistry{
    std::mutex mu;
    std::vector<int> server_ids;
    std::vector<std::string> hosts;
    std::vector<std::string> ports;

    void updateFromServerList(const ServerList &sl) {
        std::lock_guard<std::mutex> lk(mu);
        server_ids.assign(sl.serverid().begin(), sl.serverid().end());
        hosts.assign(sl.hostname().begin(), sl.hostname().end());
        ports.assign(sl.port().begin(), sl.port().end());
    }

    void snapshot(std::vector<int> &out_ids,
                  std::vector<std::string> &out_hosts,
                  std::vector<std::string> &out_ports) {
        std::lock_guard<std::mutex> lk(mu);
        out_ids = server_ids;
        out_hosts = hosts;
        out_ports = ports;
    }

};

SynchronizerRegistry synchRegistry;

void Heartbeat(std::string coordinatorIp, std::string coordinatorPort, ServerInfo serverInfo, int syncID);

std::unique_ptr<csce438::CoordService::Stub> coordinator_stub_;
void notifyServersToReloadUsers(std::string clusterID, std::string clusterSubdirectory);  // forward declaration
void notifyServersToReloadFollowers(std::string clusterID, std::string clusterSubdirectory);  // forward declaration
// some port mapping (should be corrected later)
std::unordered_map<std::string, int> myMap = {
    {"1_1", 10000},
    {"1_2", 10001},
    {"2_1", 20000},
    {"2_2", 20001},
    {"3_1", 30000},
    {"3_2", 30001}
};

class SynchronizerRabbitMQ {
private:
    std::string hostname;
    int port;
    int synchID;

    amqp_connection_state_t conn;
    amqp_channel_t publish_channel;   // for all publish/declare/purge
    amqp_channel_t consume_channel;   // for all basic_get / read_message

    std::mutex amqpMutex;             // protect conn/channels across threads

public:
    SynchronizerRabbitMQ(const std::string &host, int p, int id)
        : hostname("rabbitmq"),        // your original choice
          port(p),
          synchID(id),
          conn(nullptr),
          publish_channel(1),
          consume_channel(2)
    {
        setupRabbitMQ();

        // Declare the queues that *this* synchronizer owns
        declareQueue("synch" + std::to_string(synchID) + "_users_queue");
        declareQueue("synch" + std::to_string(synchID) + "_clients_relations_queue");
        declareQueue("synch" + std::to_string(synchID) + "_timeline_queue");

        // If you want to start from a clean state, you can purge your own queues here:
        purgeQueue("synch" + std::to_string(synchID) + "_users_queue");
        purgeQueue("synch" + std::to_string(synchID) + "_clients_relations_queue");
        purgeQueue("synch" + std::to_string(synchID) + "_timeline_queue");
    }

    ~SynchronizerRabbitMQ() {
        std::lock_guard<std::mutex> lock(amqpMutex);
        if (conn) {
            amqp_channel_close(conn, publish_channel, AMQP_REPLY_SUCCESS);
            amqp_channel_close(conn, consume_channel, AMQP_REPLY_SUCCESS);
            amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
            amqp_destroy_connection(conn);
        }
    }

private:
    void setupRabbitMQ() {
        std::lock_guard<std::mutex> lock(amqpMutex);

        conn = amqp_new_connection();
        amqp_socket_t *socket = amqp_tcp_socket_new(conn);
        if (!socket) {
            throw std::runtime_error("Failed to create AMQP socket");
        }

        int status = amqp_socket_open(socket, hostname.c_str(), port);
        if (status) {
            throw std::runtime_error("Failed to open AMQP socket");
        }

        amqp_rpc_reply_t login_reply = amqp_login(
            conn, "/", 0, 131072, 0,
            AMQP_SASL_METHOD_PLAIN, "guest", "guest");

        if (login_reply.reply_type != AMQP_RESPONSE_NORMAL) {
            throw std::runtime_error("AMQP login failed");
        }

        // Open one channel for publishing/declaring
        amqp_channel_open(conn, publish_channel);
        if (amqp_get_rpc_reply(conn).reply_type != AMQP_RESPONSE_NORMAL) {
            throw std::runtime_error("Failed to open publish channel");
        }

        // Open one channel for consuming (basic_get)
        amqp_channel_open(conn, consume_channel);
        if (amqp_get_rpc_reply(conn).reply_type != AMQP_RESPONSE_NORMAL) {
            throw std::runtime_error("Failed to open consume channel");
        }
    }

    void declareQueue(const std::string &queueName) {
        std::lock_guard<std::mutex> lock(amqpMutex);

        amqp_queue_declare(conn,
                           publish_channel,
                           amqp_cstring_bytes(queueName.c_str()),
                           0,   // passive
                           0,   // durable
                           0,   // exclusive
                           0,   // auto-delete
                           amqp_empty_table);
        amqp_rpc_reply_t r = amqp_get_rpc_reply(conn);
        if (r.reply_type != AMQP_RESPONSE_NORMAL) {
            std::cerr << "Failed to declare queue " << queueName << std::endl;
        }
    }

    void purgeQueue(const std::string &queueName) {
        std::lock_guard<std::mutex> lock(amqpMutex);

        amqp_queue_purge(conn, publish_channel,
                         amqp_cstring_bytes(queueName.c_str()));
        amqp_get_rpc_reply(conn); // ignore error for now
    }

public:
    void publishMessage(const std::string &queueName, const std::string &message) {
        std::lock_guard<std::mutex> lock(amqpMutex);

        int status = amqp_basic_publish(
            conn,
            publish_channel,
            amqp_empty_bytes, // default exchange
            amqp_cstring_bytes(queueName.c_str()),
            0,    // mandatory
            0,    // immediate
            NULL, // properties
            amqp_cstring_bytes(message.c_str()));

        if (status < 0) {
            std::cerr << "Failed to publish to " << queueName
                      << ": " << amqp_error_string2(status) << std::endl;
        }
    }

private:
    // Core helper: get one message from exactly this queue using basic_get.
    std::string basicGet(const std::string &queueName) {
        std::lock_guard<std::mutex> lock(amqpMutex);

        amqp_rpc_reply_t reply = amqp_basic_get(
            conn,
            consume_channel,
            amqp_cstring_bytes(queueName.c_str()),
            1  // no_ack = 1: automatic ack
        );

        if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
            if (reply.reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION) {
                std::cerr << "[basic_get] error on queue " << queueName
                          << ": " << amqp_error_string2(reply.library_error)
                          << std::endl;
            }
            return "";
        }

        if (reply.reply.id == AMQP_BASIC_GET_EMPTY_METHOD) {
            // No message currently in this queue
            return "";
        }

        amqp_message_t message;
        amqp_rpc_reply_t msg_reply =
            amqp_read_message(conn, consume_channel, &message, 0);

        if (msg_reply.reply_type != AMQP_RESPONSE_NORMAL) {
            std::cerr << "[read_message] failed on queue " << queueName << std::endl;
            amqp_destroy_message(&message);
            return "";
        }

        std::string body;
        if (message.body.bytes && message.body.len > 0) {
            body.assign(static_cast<char*>(message.body.bytes),
                        static_cast<size_t>(message.body.len));
        }

        amqp_destroy_message(&message);
        amqp_maybe_release_buffers(conn);
        return body;
    }



public:


    void publishUserList()
    {
        std::vector<std::string> users = get_all_users_func(synchID);
        std::cout << "we are publishing user list from synchronizer " << synchID << " with " << users.size() << " users." << std::endl;
        std::sort(users.begin(), users.end());
        Json::Value userList;
        for (const auto &user : users)
        {
            userList["users"].append(user);
        }
        Json::FastWriter writer;
        std::string message = writer.write(userList);
        publishMessage("synch" + std::to_string(synchID) + "_users_queue", message);
    }

    void consumeUserLists()
    {
        std::cerr << "consumeUserLists() entered for synchID=" << synchID << std::endl;
        std::unordered_set<std::string> allUsersSet; // Use set to avoid duplicates

        std::vector<int> server_ids;
        std::vector<std::string> hosts, ports;
        synchRegistry.snapshot(server_ids, hosts, ports);
        
        std::cout << "consumeUserLists: Will consume from " << server_ids.size() << " synchronizer queues" << std::endl;
        for (int id : server_ids) {
            std::cout << "  - synch" << id << "_users_queue" << std::endl;
        }

        for (int id : server_ids)
        {
            // Skip consuming from our own queue - we already have our own users
            if (id == synchID) {
                std::cout << "consumeUserLists: Skipping own queue synch" << id << "_users_queue" << std::endl;
                continue;
            }
            
            std::string queueName = "synch" + std::to_string(id) + "_users_queue";
            std::cout << "consumeUserLists: Attempting to read from " << queueName << std::endl;
            
            // Read only ONE message per queue to give other consumers a chance
            std::string message = basicGet(queueName);
            if (message.empty()) {
                std::cout << "  No messages in " << queueName << std::endl;
            } else {
                std::cout << "Consumer (synch" << synchID << ") read from " << queueName 
                          << ": " << message << std::endl;
                
                Json::Value root;
                Json::Reader reader;
                if (reader.parse(message, root))
                {
                    for (const auto &user : root["users"])
                    {
                        std::string userName = user.asString();
                        allUsersSet.insert(userName);
                        std::cout << "  - Extracted user: " << userName << std::endl;
                    }
                }
            }
        }
        
        // Convert set to vector
        std::vector<std::string> allUsers(allUsersSet.begin(), allUsersSet.end());
        std::cerr << "go to update all users file with " << allUsers.size() << " unique users." << std::endl;
        updateAllUsersFile(allUsers);
    }

    void publishClientRelations()
    {
        Json::Value relations;
        std::vector<std::string> users = get_all_users_func(synchID);

        for (const auto &client : users)
        {
            int clientId = std::stoi(client);
            std::vector<std::string> followers = getFollowersOfUser(clientId);

            Json::Value followerList(Json::arrayValue);
            for (const auto &follower : followers)
            {
                followerList.append(follower);
            }

            if (!followerList.empty())
            {
                relations[client] = followerList;
            }
        }

        Json::FastWriter writer;
        std::string message = writer.write(relations);
        publishMessage("synch" + std::to_string(synchID) + "_clients_relations_queue", message);
    }

    void consumeClientRelations()
    {
        std::vector<std::string> allUsers = get_all_users_func(synchID);

        // YOUR CODE HERE
        std::vector<int> server_ids;
        std::vector<std::string> hosts, ports;
        synchRegistry.snapshot(server_ids, hosts, ports);

        std::cerr << "consuming user lists from " << server_ids.size() << " synchronizers\n";

        bool hasChanges = false; // Track if any changes were made

        // TODO: hardcoding 6 here, but you need to get list of all synchronizers from coordinator as before
        for (int id : server_ids)
        {
            // Skip consuming from our own queue - we already have our own data
            if (id == synchID) {
                std::cout << "consumeClientRelations: Skipping own queue synch" << id << "_clients_relations_queue" << std::endl;
                continue;
            }

            std::string queueName = "synch" + std::to_string(id) + "_clients_relations_queue";
            //std::string message = consumeMessage(queueName, 1000); // 1 second timeout
            std::string message = basicGet(queueName); // <--- key change
            if (!message.empty())
            {
                Json::Value root;
                Json::Reader reader;
                if (reader.parse(message, root))
                {
                    for (const auto &client : allUsers)
                    {
                        std::string followerFile = "./cluster/" + std::to_string(clusterID) + "/" + clusterSubdirectory + "/" + client + "_followers.txt";
                        //std::string semName = "/" + std::to_string(clusterID) + "_" + clusterSubdirectory + "_" + client + "_followers.txt";
                        std::string semName = makeSemaphoreName(client + "_followers.txt");
                        sem_t *fileSem = sem_open(semName.c_str(), O_CREAT, 0644, 1);
                        // lock the file
                        if (fileSem == SEM_FAILED) {
                            // Try to recover by unlinking and recreating
                            perror("consumeClientRelations: sem_open failed, trying to recover");
                            sem_unlink(semName.c_str());
                            fileSem = sem_open(semName.c_str(), O_CREAT, 0644, 1);
                            if (fileSem == SEM_FAILED) {
                                perror("consumeClientRelations: sem_open recovery failed");
                                continue;
                            }
                        } 

                        sem_wait(fileSem);

                        std::ofstream followerStream(followerFile, std::ios::app | std::ios::out | std::ios::in);
                        if (root.isMember(client))
                        {
                            for (const auto &follower : root[client])
                            {
                                if (!file_contains_user(followerFile, follower.asString()))
                                {
                                    followerStream << follower.asString() << std::endl;
                                    hasChanges = true; // Mark that we made a change
                                }
                            }
                        }

                        sem_post(fileSem);
                        sem_close(fileSem);
                    }
                }
            }
        }

        // Only reload if there were actual changes
        if (hasChanges) {
            notifyServersToReloadFollowers(std::to_string(clusterID), clusterSubdirectory);
        }
    }

    // for every client in your cluster, update all their followers' timeline files
    // by publishing your user's timeline file (or just the new updates in them)
    //  periodically to the message queue of the synchronizer responsible for that client
    void publishTimelines()
    {
        std::cout<< "Publishing timelines from synchronizer " << synchID << std::endl;
        std::vector<std::string> users = get_all_users_func(synchID);
        std::cout << "There are " << users.size() << " users in this synchronizer." << std::endl;
        std::vector<int> server_ids;
        std::vector<std::string> hosts, ports;
        synchRegistry.snapshot(server_ids, hosts, ports);

        if (synchID % 2 == 0){
            // we don't let the slave synchornizer publish timelines
            std::cout << "This is a slave synchronizer, skipping timeline publishing." << std::endl;
            return;
        }

        for (const auto &client : users)
        {
            int clientId = std::stoi(client);
            int client_cluster = ((clientId - 1) % 3) + 1;
            // only do this for clients in your own cluster
            if (client_cluster != clusterID){
                continue;
            }

            std::vector<std::string> timeline = get_tl_or_fl(synchID, clientId, true);
            std::cout << "Client " << clientId << " timeline has " << timeline.size() << " lines." << std::endl;
            std::cout << "User " << clientId << " has " << timeline.size() / 3 << " posts in their timeline." << std::endl;
            std::vector<std::string> followers = getFollowersOfUser(clientId);

            int totalPosts = timeline.size() / 3;
            int oldPosts = 0;
            if (timelineLengths.find(client) != timelineLengths.end()){
                oldPosts = timelineLengths[client];
            }
            std::cout << "User " << clientId << " has " << oldPosts << " old posts in their timeline." << std::endl;
            if (oldPosts >= totalPosts){
                continue; // nothing new to send
            }

            Json::Value posts(Json::arrayValue);
            for (int i = oldPosts * 3; i < totalPosts * 3; i += 3){
                
                Json::Value post(Json::arrayValue);
                post.append(timeline[i + 0]); // "T ..."
                post.append(timeline[i + 1]); // "U ..."
                post.append(timeline[i + 2]); // "W ..."
                post.append(""); // "" (blank)
                posts.append(post);
            }

            if (posts.empty()){
                continue;
            }

            for (const auto &follower : followers)
            {
                // send the timeline updates of your current user to all its followers
                int followerId = std::stoi(follower);
                int follower_cluster = ((followerId - 1) % 3) + 1;
                // choose a synchronizer that is responsible for the follower
                for (int id: server_ids){
                    int syncCluster = ((id - 1) % 3) + 1;
                    if (syncCluster != follower_cluster){ 
                        continue;
                    }
                    std::string queueName = "synch" + std::to_string(id) + "_timeline_queue";
                    std::cout <<"User " << clientId << " Publishing to queue " << queueName << " for follower " << followerId << std::endl;

                    // Send all *new* posts for this author to this synchronizer
                    Json::Value timelineEntry;

                
                    timelineEntry["receiver"] = follower;
                    timelineEntry["post"] = posts;

                    static Json::FastWriter writer;  // can reuse
                    std::string message = writer.write(timelineEntry);
                    publishMessage(queueName, message);
                    std::cout << "Published " << posts.size() << " new posts from user " << clientId << " to follower " << followerId << " via synchronizer " << id << std::endl;
                    std::cout << "the follower id is" << followerId << std::endl;
                }
            

            }
            // We have now sent all posts up to totalPosts for this user
            timelineLengths[client] = totalPosts;
            std::cout << "User " << clientId << " timeline updated to " << totalPosts << " posts." << std::endl;
        }
    }

    // For each client in your cluster, consume messages from your timeline queue and modify your client's timeline files based on what the users they follow posted to their timeline
    void consumeTimelines()
    {

        std::string queueName = "synch" + std::to_string(synchID) + "_timeline_queue";
        //std::string message = consumeMessage(queueName, 1000); // 1 second timeout
        std::string message = basicGet(queueName); // <--- key change
        std::cout << "consumeTimelines: raw message: " << message <<  " from " << queueName << std::endl;

        if (!message.empty())
        {
            // consume the message from the queue and update the timeline file of the appropriate client with
            // the new updates to the timeline of the user it follows
            Json::Value root;
            Json::Reader reader;
            if (reader.parse(message, root)){
                std::string receiver = root["receiver"].asString();
                std::cout << "consumeTimelines: updating timeline for receiver " << receiver << std::endl;
                if (receiver.empty()) { // There might be no timeline messages or clients available
                    std::cerr << "consumeTimelines: empty receiver, ignoring message\n";
                    return;
                }

                if (!root.isMember("post") || !root["post"].isArray()) { // it fetch the wrong message
                    std::cerr << "consumeTimelines: missing or invalid 'post', raw message: "
                            << message << std::endl;
                    return;
                }



                std::string timelineFile = "./cluster/" + std::to_string(clusterID) + "/" +
                                        clusterSubdirectory + "/" + receiver + "_following.txt";
                std::string semName = makeSemaphoreName(receiver + "_following.txt");
                std::cout << "Updating following file " << timelineFile << " for client " << receiver << std::endl;

                sem_t *fileSem = sem_open(semName.c_str(), O_CREAT, 0644, 1);
                if (fileSem == SEM_FAILED) {
                    // Try to recover by unlinking and recreating
                    perror("consumeTimelines: sem_open failed, trying to recover");
                    sem_unlink(semName.c_str());
                    fileSem = sem_open(semName.c_str(), O_CREAT, 0644, 1);
                    if (fileSem == SEM_FAILED) {
                        perror("consumeTimelines: sem_open recovery failed");
                        return;
                    }
                }

                // protect writes with semaphore, open file in append mode
                sem_wait(fileSem);
                std::cout << "start to write to " << timelineFile << std::endl;
                std::ofstream timelineStream(timelineFile, std::ios::app);
                if (!timelineStream.is_open()) {
                    std::cerr << "consumeTimelines: could not open " << timelineFile << std::endl;
                    sem_post(fileSem);
                    sem_close(fileSem);
                    return;
                }

                std::cout << "consumeTimelines: updating timeline for receiver " << receiver << std::endl;
                const Json::Value &postsJSON = root["post"]; // array of posts

                for (const auto &postEntry : postsJSON) {
                    for (const auto &lineVal : postEntry) {
                        timelineStream << lineVal.asString() << std::endl;
                    }
                }

                timelineStream.close();

                

                sem_post(fileSem);
                sem_close(fileSem);
            


            }

        }
    }

private:
    void updateAllUsersFile(const std::vector<std::string> &users)
    {
        // Create directory structure if it doesn't exist
        std::string dirPath = "./cluster/" + std::to_string(clusterID) + "/" + clusterSubdirectory;
        try {
            std::filesystem::create_directories(dirPath);
            std::cout << "updateAllUsersFile: ensured directory exists: " << dirPath << std::endl;
        } catch (const std::filesystem::filesystem_error& e) {
            std::cerr << "updateAllUsersFile: failed to create directory " << dirPath 
                      << ": " << e.what() << std::endl;
            return;
        }

        std::string usersFile = "./cluster/" + std::to_string(clusterID) + "/" + clusterSubdirectory + "/all_users.txt";
        
        // Read existing users OUTSIDE the write lock to avoid deadlock
        std::vector<std::string> existingUsers = get_lines_from_file(usersFile);
        std::unordered_set<std::string> existingSet(existingUsers.begin(), existingUsers.end());
        
        std::cout << "updateAllUsersFile: existing users in " << usersFile << ": ";
        for (const auto& u : existingUsers) {
            std::cout << u << " ";
        }
        std::cout << std::endl;
        
        // Filter out users that already exist
        std::vector<std::string> newUsers;
        for (const std::string &user : users) {
            if (existingSet.find(user) == existingSet.end()) {
                newUsers.push_back(user);
                std::cout << "  User '" << user << "' is NEW, will add" << std::endl;
            } else {
                std::cout << "  User '" << user << "' already exists, skipping" << std::endl;
            }
        }
        
        if (newUsers.empty()) {
            std::cout << "updateAllUsersFile: no new users to add" << std::endl;
            return;
        }
        
        // Now acquire lock and write only new users
        std::string semName = makeSemaphoreName("all_users.txt");
        
        // Try O_EXCL first (create new), if fails then open existing
        sem_t *fileSem = sem_open(semName.c_str(), O_CREAT | O_EXCL, 0666, 1);
        if (fileSem == SEM_FAILED && errno == EEXIST) {
            // Semaphore exists, try to open it
            fileSem = sem_open(semName.c_str(), 0);
            if (fileSem == SEM_FAILED) {
                // Existing semaphore is corrupted, unlink and recreate
                std::cerr << "updateAllUsersFile: existing semaphore corrupted, unlinking and recreating" << std::endl;
                sem_unlink(semName.c_str());
                fileSem = sem_open(semName.c_str(), O_CREAT | O_EXCL, 0666, 1);
            }
        }
        if (fileSem == SEM_FAILED) {
            perror("updateAllUsersFile: sem_open");
            std::cerr << "updateAllUsersFile: semaphore name was: " << semName << std::endl;
            return;
        }
        sem_wait(fileSem);

        std::ofstream userStream(usersFile, std::ios::app);
        if (!userStream.is_open()) {
            std::cerr << "updateAllUsersFile: could not open " << usersFile 
                      << " (errno: " << strerror(errno) << ")" << std::endl;
            sem_post(fileSem);
            sem_close(fileSem);
            return;
        }
        
        for (const std::string &user : newUsers) {
            userStream << user << std::endl;
            std::cout << "  Wrote user '" << user << "' to " << usersFile << std::endl;
        }
        
        // Explicitly close the file to flush buffer before releasing semaphore
        userStream.close();
        
        std::cout << "updateAllUsersFile: added " << newUsers.size() << " new users to " << usersFile << std::endl;

        sem_post(fileSem);
        sem_close(fileSem);


        // here, send a grpc requerst to the server to updat client_db
        std::cerr << "Notifying servers to reload user lists..." << std::endl;
        notifyServersToReloadUsers( std::to_string(clusterID), clusterSubdirectory);
    }
};

void run_synchronizer(std::string coordIP, std::string coordPort, std::string port, int synchID, SynchronizerRabbitMQ &rabbitMQ);

class SynchServiceImpl final : public SynchService::Service
{
    // You do not need to modify this in any way
};

void RunServer(std::string coordIP, std::string coordPort, std::string port_no, int synchID)
{
    // localhost = 127.0.0.1
    std::string server_address("127.0.0.1:" + port_no);
    log(INFO, "Starting synchronizer server at " + server_address);
    SynchServiceImpl service;
    // grpc::EnableDefaultHealthCheckService(true);
    // grpc::reflection::InitProtoReflectionServerBuilderPlugin();
    ServerBuilder builder;
    // Listen on the given address without any authentication mechanism.
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    // Register "service" as the instance through which we'll communicate with
    // clients. In this case it corresponds to an *synchronous* service.
    builder.RegisterService(&service);
    // Finally assemble the server.
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;

    // Initialize RabbitMQ connection
    // SynchronizerRabbitMQ rabbitMQ("localhost", 5672, synchID);
    SynchronizerRabbitMQ rabbitMQ("rabbitmq", 5672, synchID);

    std::thread t1(run_synchronizer, coordIP, coordPort, port_no, synchID, std::ref(rabbitMQ));

    // Create a consumer thread
    std::thread consumerThread([&rabbitMQ]()
                               {
        while (true) {
            try {
                rabbitMQ.consumeUserLists();
                rabbitMQ.consumeClientRelations();
                rabbitMQ.consumeTimelines();
            } catch (const std::exception& e) {
                std::cerr << "Exception in consumer thread: " << e.what() << std::endl;
            } catch (...) {
                std::cerr << "Unknown exception in consumer thread" << std::endl;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            // you can modify this sleep period as per your choice
        } });

    server->Wait();

    //   t1.join();
    //   consumerThread.join();
}

int main(int argc, char **argv)
{
    int opt = 0;
    std::string coordIP;
    std::string coordPort;
    std::string port = "3029";



    // Why not set clusterID?
    while ((opt = getopt(argc, argv, "h:k:p:i:")) != -1)
    {
        switch (opt)
        {
        case 'h':
            coordIP = optarg;
            break;
        case 'k':
            coordPort = optarg;
            break;
        case 'p':
            port = optarg;
            break;
        case 'i':
            synchID = std::stoi(optarg);
            break;
        default:
            std::cerr << "Invalid Command Line Argument\n";
        }
    }



    //TODO: populate global variables
    std::string log_file_name = std::string("synchronizer-") + port;
    google::InitGoogleLogging(log_file_name.c_str());
    log(INFO, "Logging Initialized. Server starting...");

    coordAddr = coordIP + ":" + coordPort;
    const int numClusters = 3;
    /*
        justification:
        i = 1 -> clusterID = 1
        i = 2 -> clusterID = 2
        i = 3 -> clusterID = 3
        i = 4 -> clusterID = 1
        i = 5 -> clusterID = 2
        i = 6 -> clusterID = 3
    */
    clusterID = ((synchID - 1) % numClusters) + 1;


    // caluclate position in cluster
    int posInCluster = ((synchID - 1) / numClusters);
    if (posInCluster == 0){
        isMaster = true;
        clusterSubdirectory = "1";
    }
    else{
        isMaster = false;
        clusterSubdirectory = "2";
    }

    ServerInfo serverInfo;
    serverInfo.set_hostname("localhost");
    serverInfo.set_port(port);
    serverInfo.set_type("synchronizer");
    serverInfo.set_serverid(synchID);
    serverInfo.set_clusterid(clusterID);
    Heartbeat(coordIP, coordPort, serverInfo, synchID);

    // start the hearbeat thread
    // () mutable: allowed to modify captured variables
    // TODO: This might be altered by global variable 
    std::thread hb_thread([coordIP, coordPort, serverInfo, synchID]() mutable {
        while (true) {
            std::this_thread::sleep_for(std::chrono::seconds(5));
            Heartbeat(coordIP, coordPort, serverInfo, synchID);
        }
    });
    hb_thread.detach();

    RunServer(coordIP, coordPort, port, synchID);
    return 0;
}

// TODO: syncID should be passed in by the program input, not the global variable

// overall workflow: 
void run_synchronizer(std::string coordIP, std::string coordPort, std::string port, int synchID, SynchronizerRabbitMQ &rabbitMQ)
{
    // setup coordinator stub
    std::cout << "Setting up coordinator stub at " << coordIP << ":" << coordPort << std::endl;
    std::cout << "entered synchronizer" << std::endl;
    std::string target_str = coordIP + ":" + coordPort;
    std::unique_ptr<CoordService::Stub> coord_stub_;
    coord_stub_ = std::unique_ptr<CoordService::Stub>(CoordService::NewStub(grpc::CreateChannel(target_str, grpc::InsecureChannelCredentials())));

    // // This part seems to be redundant
    ServerInfo msg;
    Confirmation c;

    msg.set_serverid(synchID);
    msg.set_hostname("127.0.0.1");
    msg.set_port(port);
    msg.set_type("follower");

    // TODO: begin synchronization process
    while (true)
    {
        // the synchronizers sync files every 2 seconds
        sleep(2);

        grpc::ClientContext context;
        ServerList followerServers;
        ID id;
        id.set_id(synchID);

        // making a request to the coordinator to see count of follower synchronizers
        //coord_stub_->GetAllFollowerServers(&context, id, &followerServers);

        // ...existing code...
        // making a request to the coordinator to see count of follower synchronizers
        std::cout << "Making GetAllFollowerServers RPC call" << std::endl;
        grpc::Status status = coord_stub_->GetAllFollowerServers(&context, id, &followerServers);
        if (!status.ok()) {
            log(ERROR, "GetAllFollowerServers RPC failed: " + status.error_message());
            std::cout << "GetAllFollowerServers RPC failed: " << status.error_message() << std::endl;
        } else {
            int n = followerServers.serverid_size();
            log(INFO, "GetAllFollowerServers success; returned " + std::to_string(n) + " server(s)");
            // for (int i = 0; i < n; ++i) {
            //     // log(INFO, "  follower id=" + std::to_string(followerServers.serverid(i))
            //     //           + " host=" + followerServers.hostname(i)
            //     //           + " port=" + followerServers.port(i));
            //     std::cout << "  follower id=" << followerServers.serverid(i)
            //               << " host=" << followerServers.hostname(i)
            //               << " port=" << followerServers.port(i) << std::endl;
            // }
        }
        synchRegistry.updateFromServerList(followerServers);
 // ...existing code...

        std::vector<int> server_ids;
        std::vector<std::string> hosts, ports;
        synchRegistry.snapshot(server_ids, hosts, ports);


        // print out the vector
        for (int i = 0; i < server_ids.size(); i++)
        {
            std::cout << "Follower Synchronizer " << server_ids[i] << ": " << hosts[i] << ":" << ports[i] << std::endl;
        }

        // update the count of how many follower sychronizer processes the coordinator has registered
        total_number_of_registered_synchronizers = server_ids.size();
        std::cout << "total_number_of_registered_synchronizers = " << total_number_of_registered_synchronizers << std::endl;
        // below here, you run all the update functions that synchronize the state across all the clusters
        // make any modifications as necessary to satisfy the assignments requirements

        // Publish user list
        rabbitMQ.publishUserList();

        // Publish client relations
        rabbitMQ.publishClientRelations();

        // Publish timelines
        rabbitMQ.publishTimelines();
    }
    return;
}

std::vector<std::string> get_lines_from_file(std::string filename)
{
    std::vector<std::string> users;
    std::string user;
    std::ifstream file;

    
    std::string semName = makeSemaphoreName(filename);
    
    // Try to open with permissive permissions
    sem_t *fileSem = sem_open(semName.c_str(), O_CREAT, 0666, 1);
    if (fileSem == SEM_FAILED) {
        perror("get_lines_from_file: sem_open failed");
        return users;
    }

    auto cleanup = [&fileSem]() {
        sem_post(fileSem);
        sem_close(fileSem);
    };
    sem_wait(fileSem);

    file.open(filename);
    if (!file.is_open()) {
        std::cerr << "get_lines_from_file: could not open " << filename << std::endl;
        cleanup();
        return users; // return empty vector if file cannot be opened
    }

    if (file.peek() == std::ifstream::traits_type::eof())
    {
        // return empty vector if empty file
        // std::cout<<"returned empty vector bc empty file"<<std::endl;
        file.close();
        cleanup();
        return users;
    }

    while (file)
    {
        getline(file, user);

        if (!user.empty())
            users.push_back(user);
    }

    file.close();
    cleanup();

    return users;
}

void Heartbeat(std::string coordinatorIp, std::string coordinatorPort, ServerInfo serverInfo, int syncID)
{
    // For the synchronizer, a single initial heartbeat RPC acts as an initialization method which
    // servers to register the synchronizer with the coordinator and determine whether it is a master

    log(INFO, "Sending initial heartbeat to coordinator");
    std::string coordinatorInfo = coordinatorIp + ":" + coordinatorPort;
    std::unique_ptr<CoordService::Stub> stub = std::unique_ptr<CoordService::Stub>(CoordService::NewStub(grpc::CreateChannel(coordinatorInfo, grpc::InsecureChannelCredentials())));

    // send a heartbeat to the coordinator, which registers your follower synchronizer as either a master or a slave
    Confirmation confirmation;
    grpc::ClientContext context;
    // create a local stub and use it (the global coordinator_stub_ is not initialized here)
    grpc::Status status = stub->Heartbeat(&context, serverInfo, &confirmation);

    // YOUR CODE HERE
    if (!status.ok()){
        log(ERROR, "Heartbeat failed to send from synchronizer " + std::to_string(serverInfo.serverid()) + ": " + status.error_message());

    }
    else {
      log(INFO, "heartbeat sent successfully from synchronizer " + std::to_string(serverInfo.serverid()));
    }
}

bool file_contains_user(std::string filename, std::string user)
{
    std::vector<std::string> users;
    // check username is valid
    std::string semName = makeSemaphoreName(filename);
    sem_t *fileSem = sem_open(semName.c_str(), O_CREAT, 0666, 1);
    if (fileSem == SEM_FAILED) {
        perror("sem_open failed");
        return false; // return false on semaphore error
    }

    users = get_lines_from_file(filename);
    for (size_t i = 0; i < users.size(); i++)
    {
        // std::cout<<"Checking if "<<user<<" = "<<users[i]<<std::endl;
        if (user == users[i])
        {
            // std::cout<<"found"<<std::endl;
            if (fileSem != SEM_FAILED) {
                sem_close(fileSem);
            }
            return true;
        }
    }
    // std::cout<<"not found"<<std::endl;
    sem_close(fileSem);
    return false;
}

std::vector<std::string> get_all_users_func(int synchID)
{
    // read all_users file master and client for correct serverID
    // std::string master_users_file = "./master"+std::to_string(synchID)+"/all_users";
    // std::string slave_users_file = "./slave"+std::to_string(synchID)+"/all_users";
    std::string clusterID = std::to_string(((synchID - 1) % 3) + 1);
    std::string master_users_file = "./cluster/" + clusterID + "/1/all_users.txt";
    std::string slave_users_file = "./cluster/" + clusterID + "/2/all_users.txt";
    
    // print once per process (thread-safe)
    static std::once_flag once;
    std::call_once(once, [&]{
        std::cout << "the master file we are taking from is " << master_users_file << std::endl;
    });
    
    // take longest list and package into AllUsers message
    std::vector<std::string> master_user_list = get_lines_from_file(master_users_file);
    std::vector<std::string> slave_user_list = get_lines_from_file(slave_users_file);

    if (master_user_list.size() >= slave_user_list.size())
        return master_user_list;
    else
        return slave_user_list;
}

std::vector<std::string> get_tl_or_fl(int synchID, int clientID, bool tl)
{
    // std::string master_fn = "./master"+std::to_string(synchID)+"/"+std::to_string(clientID);
    // std::string slave_fn = "./slave"+std::to_string(synchID)+"/" + std::to_string(clientID);
    std::string master_fn = "cluster/" + std::to_string(clusterID) + "/1/" + std::to_string(clientID);
    std::string slave_fn = "cluster/" + std::to_string(clusterID) + "/2/" + std::to_string(clientID);
    if (tl)
    {
        master_fn.append("_timeline.txt");
        slave_fn.append("_timeline.txt");
    }
    else
    {
        master_fn.append("_followers.txt");
        slave_fn.append("_followers.txt");
    }

    std::cout << "Getting timeline/followers from files: " << master_fn << " and " << slave_fn << std::endl;

    std::vector<std::string> m = get_lines_from_file(master_fn);
    std::vector<std::string> s = get_lines_from_file(slave_fn);

    if (m.size() >= s.size())
    {
        return m;
    }
    else
    {
        return s;
    }
}

std::vector<std::string> getFollowersOfUser(int ID)
{
    std::vector<std::string> followers;
    std::string clientID = std::to_string(ID);
    std::vector<std::string> usersInCluster = get_all_users_func(synchID);

    for (auto userID : usersInCluster)
    { // Examine each user's following file
        // if 1 follows 2, 3, 4, there should be 2, 3, 4 in 1_follow_list.txt
        // edit -  change follow_list to followers.txt
        std::string file = "cluster/" + std::to_string(clusterID) + "/" + clusterSubdirectory + "/" + userID + "_followers.txt";
        std::string semName = "/" + std::to_string(clusterID) + "_" + clusterSubdirectory + "_" + userID + "_followers.txt";
        sem_t *fileSem = sem_open(semName.c_str(), O_CREAT);
        //std::cout << "Reading file " << file << std::endl;
        if (file_contains_user(file, clientID))
        {
            followers.push_back(userID);
            std::cout << "User " << userID << " follows " << clientID << std::endl;
        }
        sem_close(fileSem);

    }

    return followers;
}


void notifyServersToReloadUsers(std::string clusterID, std::string clusterSubdirectory) {
    try {
        std::string host = "localhost";

        std::string port = std::to_string( myMap[clusterID + "_" + clusterSubdirectory] );

        // time the RPC calls
        std::cout << "Notifying server at " << host << ":" << port << " to reload user lists." << std::endl;
        auto channel = grpc::CreateChannel(host + ":" + port, grpc::InsecureChannelCredentials());
        std::unique_ptr<SNSService::Stub> stub = SNSService::NewStub(channel);

        Empty req;
        CoordConfirmation resp;
        ClientContext context;
        
        // Add 2 second timeout to prevent blocking indefinitely
        std::chrono::system_clock::time_point deadline = 
            std::chrono::system_clock::now() + std::chrono::seconds(2);
        context.set_deadline(deadline);

        Status status = stub->ReloadAllUsers(&context, req, &resp);

        if (!status.ok()) {
            log(ERROR, "ReloadAllUsers RPC failed: " + status.error_message());
            std::cout << "ReloadAllUsers RPC failed: " << status.error_message() << std::endl;
        } else {
            log(INFO, "ReloadAllUsers RPC succeeded");
            std::cout << "ReloadAllUsers RPC succeeded" << std::endl;
        }
    } catch (const std::exception& e) {
        std::cerr << "Exception in notifyServersToReloadUsers: " << e.what() << std::endl;
    } catch (...) {
        std::cerr << "Unknown exception in notifyServersToReloadUsers" << std::endl;
    }

}

void notifyServersToReloadFollowers(std::string clusterID, std::string clusterSubdirectory){
    try {
        std::string host = "localhost";

        std::string port = std::to_string( myMap[clusterID + "_" + clusterSubdirectory] );

        // time the RPC calls
        std::cout << "Notifying server at " << host << ":" << port << " to reload follower relationships." << std::endl;
        auto channel = grpc::CreateChannel(host + ":" + port, grpc::InsecureChannelCredentials());
        std::unique_ptr<SNSService::Stub> stub = SNSService::NewStub(channel);

        Empty req;
        CoordConfirmation resp;
        ClientContext context;
        
        // Add 2 second timeout to prevent blocking indefinitely
        std::chrono::system_clock::time_point deadline = 
            std::chrono::system_clock::now() + std::chrono::seconds(2);
        context.set_deadline(deadline);

        Status status = stub->ReloadAllFollowers(&context, req, &resp);

        if (!status.ok()) {
            log(ERROR, "ReloadAllFollowers RPC failed: " + status.error_message());
            std::cout << "ReloadAllFollowers RPC failed: " << status.error_message() << std::endl;
        } else {
            log(INFO, "ReloadAllFollowers RPC succeeded");
            std::cout << "ReloadAllFollowers RPC succeeded" << std::endl;
        }
    } catch (const std::exception& e) {
        std::cerr << "Exception in notifyServersToReloadFollowers: " << e.what() << std::endl;
    } catch (...) {
        std::cerr << "Unknown exception in notifyServersToReloadFollowers" << std::endl;
    }
}