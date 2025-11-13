/*
 *
 * Copyright 2015, Google Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *     * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 *     * Neither the name of Google Inc. nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */
#include <chrono>
#include <ctime>
#include <filesystem>  // C++17
#include <regex>

#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>

#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <stdlib.h>
#include <unistd.h>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>
#include<glog/logging.h>
#include<thread>
// #define log(severity, msg) LOG(severity) << msg; google::FlushLogFiles(google::severity); 

#include "sns.grpc.pb.h"
#include "coordinator.grpc.pb.h"

using csce438::CoordService; // the stub to call heartbeat RPC
using csce438::ServerInfo; // The message type containing server metadata
using csce438::Confirmation; // The response message from the coordinator

using google::protobuf::Timestamp;
using google::protobuf::Duration;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;
using csce438::Message;
using csce438::ListReply;
using csce438::Request;
using csce438::Reply;
using csce438::SNSService;
using csce438::CoordService; // newly_added
using csce438::ID; // newly added
using grpc::ClientContext; // newly added

struct Client {
  std::string username;
  bool connected = true;
  int following_file_size = 0;
  std::vector<Client*> client_followers;
  std::vector<Client*> client_following;
  ServerReaderWriter<Message, Message>* stream = 0;
  bool operator==(const Client& c1) const{
    return (username == c1.username);
  }
};

// storing server configuration
struct ServerConfig {
    std::string port;
    std::string clusterId;
    std::string serverId;
    std::string coordinatorIP;
    std::string coordinatorPort;
};


//Vector that stores every client that has been created
std::vector<Client*> client_db;

bool isInVector(const std::vector<Client*>& v, const Client* who) {
  return std::find(v.begin(), v.end(), who) != v.end();
}


template <class T>
void eraseFromVector(std::vector<T*>& v, T* x) {
  v.erase(std::remove(v.begin(), v.end(), x), v.end());
}

Client* findClientByName(const std::string& uname) {
    for (auto* c : client_db) {
        if (c->username == uname) {
            return c;   // found
        }
    }
    return nullptr;     // not found
}

std::string format_file_output(const google::protobuf::Timestamp& ts,
                              const std::string& username,
                              const std::string& m){
    // convert timestamp in google protbuf format to std::time_t
    std::time_t tt = static_cast<std::time_t>(ts.seconds());
    char buf[64];
    std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", std::localtime(&tt));

    /*Format:
      T 2009-06-01 00:00:00
      U http://twitter.com/testuser
      W Post content
      Empty line
    */
    std::ostringstream oss;
    oss << "T " << buf << "\n";
    oss << "U " << username << "\n";
    oss << "W " << m << "\n";
    return oss.str();

} 

// timstamp utility function
google::protobuf::Timestamp toProtoTimestamp(const std::string& datetime) {
    std::tm tm{};
    std::istringstream ss(datetime);
    ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
    if (ss.fail()) {
        throw std::runtime_error("Failed to parse datetime: " + datetime);
    }

    // Convert to time_t (seconds since epoch, UTC)
    time_t t = timegm(&tm);  // use gmtime semantics (non-standard but available on GNU/libc)

    google::protobuf::Timestamp ts;
    ts.set_seconds(static_cast<int64_t>(t));
    ts.set_nanos(0);  // no fractional seconds in your input
    return ts;
}

bool append_to_file(const std::string& filename, const std::string& content) {
    std::string folder = "user";
    std::filesystem::create_directories(folder);  // make sure "user" exists

    std::string full_path = folder + "/" + filename;
    // std::cout << "the full path to write is..." << full_path << std::endl;
    std::ofstream out(full_path, std::ios::app);  // append mode
    if (!out) {
        return false;  // failed to open file
    }
    
    out << content;
    return true;
}

//TODO: implement this function
std::vector<Message> recent_20_messages(std::string username){
  std::string folder = "user";
  std::string filename = username + "_following.txt";
  std::string full_path = folder + "/" + filename;
  std::ifstream input_file(full_path);
  // if there is no file related to this path
  if (!input_file.is_open()){
    return {};
  }
  //std::cout << "start to read from the file " << full_path << std::endl;

  // split the post into blocks
  std::stringstream buffer;
  buffer << input_file.rdbuf();   // read entire file
  std::string content = buffer.str();

  // strip the file into post sections
  std::regex re("\\n\\n"); // split on double newlines
  std::sregex_token_iterator it(content.begin(), content.end(), re, -1);
  std::sregex_token_iterator end;
  std::vector<std::string> paragraphs(it, end);
  std::vector<Message> messages;
  for (auto paragraph: paragraphs){
    // parse the type 
    std::stringstream pg(paragraph);
    std::string tag;
    Message m;
    while (pg >> tag) {
        if (tag == "T") {
            std::string date, time;
            pg >> date >> time;
            // convert the datetime to google protobuffer format
            google::protobuf::Timestamp ts  = toProtoTimestamp(date + " " + time);
            *m.mutable_timestamp() = ts; 
        } else if (tag == "U") {
            std::string temp_username;
            pg >> temp_username;
            m.set_username(temp_username);
        } else if (tag == "W") {
          std::string temp_msg;
          pg >> temp_msg;
          m.set_msg(temp_msg);
        }
    }
    messages.push_back(m);
  }

  // I reverse the array and then take the last 20
  std::reverse(messages.begin(), messages.end());
  // take into account the number of messages in the vector
  int num_consider = std::min<size_t>(20, messages.size());
  std::vector<Message> newest_20_messages(messages.begin(), messages.begin() + num_consider);
  return newest_20_messages;

}

bool on_receiving_message(const Message& m, Client* c) {
    if (!c) return false;
    const std::string& username = m.username();  // or however you store it
    const std::string post = format_file_output(m.timestamp(), username, m.msg());
    bool ok = append_to_file(username + ".txt", post);
    // loop through the follower of the particular user
    for (Client* follower : c->client_followers) {
        if (!follower) continue;
        if (follower->stream) {
            // Write expects a Message datatype
            Message out;
            google::protobuf::Timestamp ts = google::protobuf::util::TimeUtil::GetCurrentTime();
            out.set_username(username);
            out.set_msg(m.msg());
            *out.mutable_timestamp() = ts; 
            follower->stream->Write(out);
        }
        ok = append_to_file(follower->username + "_following.txt", post) && ok;
    }
    return ok;
}



// TODO: may not be necessary in this assignment
google::protobuf::Timestamp createTimeStamp() {
    google::protobuf::Timestamp ts;

    auto now = std::chrono::system_clock::now();
    auto seconds = std::chrono::time_point_cast<std::chrono::seconds>(now);
    auto nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(now - seconds);

    ts.set_seconds(seconds.time_since_epoch().count());
    ts.set_nanos(nanos.count());
    return ts;
}

void printTimestamp(const google::protobuf::Timestamp& ts) {
    std::time_t t = ts.seconds();
    char buffer[30];
    std::strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", std::gmtime(&t));
    std::cout << buffer << "." << ts.nanos() << " UTC\n";
}


class SNSServiceImpl final : public SNSService::Service {


   public:
    // Construct service with server configuration so RPC handlers can access it
    explicit SNSServiceImpl(const ServerConfig& cfg) : server_config_(cfg) {
      if (server_config_.serverId == "1" && !server_config_.coordinatorIP.empty() && !server_config_.coordinatorPort.empty()) {
        // create grpc stub
        std::string coord_addr = server_config_.coordinatorIP + ":" + server_config_.coordinatorPort;
        auto channel = grpc::CreateChannel(coord_addr, grpc::InsecureChannelCredentials());
        coordinator_stub_ = CoordService::NewStub(channel);

      }

  
  }
  
  Status List(ServerContext* context, const Request* request, ListReply* list_reply) override {
    // request is from grpc protobuf message
    std::string uname = request -> username();
    for (Client* c : client_db){
        list_reply -> add_all_users(c -> username);
    }

    Client* c = findClientByName(uname);
    if (c != nullptr){
        for (Client* cf: c -> client_followers){
            list_reply -> add_followers(cf -> username);
        }
    }

    return Status::OK;
  }

  Status Follow(ServerContext* context, const Request* request, Reply* reply) override {

    std::string uname = request -> username();
    std::string target = request -> arguments(0);
    // Failure: A person can't follow himself
    if (uname == target) {
      reply -> set_msg("A person can't follow himself.");
      return Status::OK; 
    }

    // Failure: A person can't follow non-existent person
    if (findClientByName(target) == nullptr){
      reply -> set_msg("Following non-existent user.");
      return Status::OK;
    }

    // handle duplicate push cade
    Client* c1 = findClientByName(uname);
    Client* c2 = findClientByName(target);
    if (c1 != nullptr && c2 != nullptr){
        //  bool isInVector(const std::vector<Client*>& v, const Client* who)
        // both should be false
        bool c1_in_c2_follower = isInVector(c2 -> client_followers, c1); 
        bool c2_in_c1_following = isInVector(c1 -> client_following, c2);
        // std::cout << "c1_in_c2_follower: " << c1_in_c2_follower << std::endl;
        // std::cout << "c2_in_c1_follower: " << c2_in_c1_following << std::endl;
        if (!c1_in_c2_follower && !c2_in_c1_following){
          c1 -> client_following.push_back(c2);
          c2 -> client_followers.push_back(c1);
        }
        else {
          reply ->set_msg("Already followed.");
          return Status::OK;
        }
    }



    reply -> set_msg("Follow successful");
    // write to file
    try {
      std::string server_folder = "./cluster/" + server_config_.clusterId + "/" + server_config_.serverId + "/";
      std::cout << server_folder << std::endl;
      std::filesystem::create_directories(server_folder);
      std::string user_file = server_folder + uname + "_follow_list.txt";
      std::ofstream out(user_file, std::ios::app);
      if (out) {
        out << target << "\n"; // the username is written in the file
      }
    } catch (const std::exception& e) {
      LOG(ERROR) << "Failed to write user file: " << e.what();
    }


    // mirror the operation
    getSlaveStub();
    if (server_config_.serverId == "1" && slave_stub_){
      std::cout << "About to follow on the slave server"<< std::endl;
      Request req;
      req.set_username(uname);
      req.add_arguments(target);
      Reply rep;
      ClientContext ctx;
      Status s = slave_stub_->Follow(&ctx, req, &rep);
      if (!s.ok()) {
        LOG(ERROR) << "Failed to mirror follow to slave: " << s.error_message();
      }
    }

    return Status::OK; 
  }

  Status UnFollow(ServerContext* context, const Request* request, Reply* reply) override {

    // fetch the username and the argument
    std::string uname = request -> username();
    const std::string target = request->arguments(0);
    if (uname == target){
      reply -> set_msg("You Can't unfollow yourself.");
      return Status::OK;
    }


    // c1 follows c2 -> c2 follower [c1]; c1 following [c2] 
    Client* c1 = findClientByName(uname);
    Client* c2 = findClientByName(target);

    if (!c1) { reply->set_msg("Requester does not exist."); return Status::OK; }
    if (!c2) { reply->set_msg("Target user does not exist."); return Status::OK; }
    const bool relation_exists = isInVector(c1->client_following, c2) && isInVector(c2->client_followers, c1);
    if (!relation_exists){
      reply -> set_msg("You are not a follower.");
      return Status::OK;
    }

    eraseFromVector(c1->client_following, c2);
    eraseFromVector(c2->client_followers, c1);

    reply->set_msg("Unfollow successful.");
          

    return Status::OK;
  }

  // RPC Login
  Status Login(ServerContext* context, const Request* request, Reply* reply) override {

    //std::cout << "Hello World!!" << std::endl;
    std::string uname = request -> username();
    // check whether already logged in...
    for (auto* c: client_db){
      if (c -> username == uname && c -> connected){
        reply -> set_msg("ALREADY_LOGGED_IN");
        return Status::OK;
      }
    }

    // not logged in
    Client* newClient = new Client();
    newClient -> username = uname;
    client_db.push_back(newClient);
    reply -> set_msg("SUCCESS");

    // write to the user file
    // Create per-server user folder and write a simple user record so we persist
    // which server the user was created on. This uses the ServerConfig that
    // was injected when the service was constructed.
    try {
      std::string server_folder = "./cluster/" + server_config_.clusterId + "/" + server_config_.serverId + "/";
      std::cout << server_folder << std::endl;
      std::filesystem::create_directories(server_folder);
      std::string user_file = server_folder +  "all_users.txt";
      std::ofstream out(user_file, std::ios::app);
      if (out) {
        out << uname << "\n"; // the username is written in the file
      }
    } catch (const std::exception& e) {
      LOG(ERROR) << "Failed to write user file: " << e.what();
    }


    // mirror the same operation to slave
    // TODO: Implement mirroring logic here
    // std::string  slave_port = getSlaveAddrFromCoord(std::stoi(server_config_.port)); 
    // std::cout << "the server id is " << server_config_.serverId << std::endl;
    getSlaveStub();
    std::cout << "server id is " << server_config_.serverId << std::endl;
    if (server_config_.serverId == "1" && slave_stub_){
      std::cout << "About to log into the slave server"<< std::endl;
      Request req;
      req.set_username(uname);
      Reply rep;
      ClientContext ctx;
      Status s = slave_stub_->Login(&ctx, req, &rep);
      if (!s.ok()) {
        LOG(ERROR) << "Failed to mirror login to slave: " << s.error_message();
      }
    }

    return Status::OK;
  }

  Status Timeline(ServerContext* context, 
		ServerReaderWriter<Message, Message>* stream) override {
    Message first_in;
    if (!stream -> Read(&first_in)){
      return Status::OK;
    }

    const std::string uname = first_in.username();
    Client* c1 = findClientByName(uname);
    c1 -> stream = stream;
    // TODO: when the person enters timeline, push the recent 20 messages from who he is following to him
    std::vector<Message> recent_msgs = recent_20_messages(uname);
    // push the result back to people
    for (auto msg:recent_msgs){
      c1 -> stream -> Write(msg);
    }


    // read the rest of the message
    Message m;
    while (stream -> Read(&m)){
      std::string uname = m.username();
      Client* c = findClientByName(uname);
      c -> stream = stream; // initialize stream
      /* std::string format_file_output(const google.protobuf.Timestamp& ts,
                              const std::string& username,
                              const Message& m) */
      on_receiving_message(m, c);

      

    }



    
    return Status::OK;
    }

   private:
    ServerConfig server_config_;
    std::unique_ptr<SNSService::Stub> slave_stub_;
    std::unique_ptr<CoordService::Stub> coordinator_stub_;

    // a private helper (master asking the address from slave)
    std::string getSlaveAddrFromCoord(int server_id){
      if (!coordinator_stub_) return "";
        ID server_port;

      // turn the string to integer
      server_port.set_id(std::stoi(server_config_.port)); 
      //std::cout << "The new username is..." << std::stoi(this -> username) << std::endl;
      ClientContext ctx;
      ServerInfo svInfo;
      // The client stub expects a reference instead of pointer
      Status s = coordinator_stub_ -> GetServer(&ctx, server_port, &svInfo);
      if (s.ok()) {
        std::cout << "Received server info from coordinator: " << svInfo.hostname() << ":" << svInfo.port() << std::endl;
        return svInfo.hostname() + ":" + svInfo.port();
      } else {
        LOG(ERROR) << "Failed to get server info from coordinator: " << s.error_message();
        return "";
      }
    }

    void getSlaveStub(){
      std::cout << "whether there are information to the slave stub" << !slave_stub_ << std::endl;
      if (!slave_stub_ && server_config_.serverId == "1") {
        std::string slave_addr = getSlaveAddrFromCoord(std::stoi(server_config_.port));
        if (!slave_addr.empty()) {
          auto slave_chan = grpc::CreateChannel(slave_addr, grpc::InsecureChannelCredentials());
          slave_stub_ = SNSService::NewStub(slave_chan);
        }
      }
    }

  };


  // send HeartBeat message
void sendHeartbeat(const ServerConfig& config){
    // Register the server to the coordinator
  std::string coordinator_addr = config.coordinatorIP + ":" + config.coordinatorPort;
  auto channel = grpc::CreateChannel(coordinator_addr, grpc::InsecureChannelCredentials());
  std::unique_ptr<CoordService::Stub> coordinator_stub_; // new added
  coordinator_stub_ = CoordService::NewStub(channel);

  while (true){
    /*
      //server info message definition
      message ServerInfo{
          int32 serverID = 1;
          string hostname = 2;
          string port = 3;
          string type = 4;
      } 
    */
    ServerInfo server_info;
    server_info.set_serverid(std::stoi(config.serverId));
    server_info.set_hostname(config.coordinatorIP); // or get actual hostname
    server_info.set_port(config.port);
    if (config.serverId == "1"){
        server_info.set_type("master");
    } else {
        server_info.set_type("slave");
    }
    server_info.set_clusterid(std::stoi(config.clusterId));

    Confirmation confirmation;
    grpc::ClientContext context;
    grpc::Status status = coordinator_stub_->Heartbeat(&context, server_info, &confirmation);

    // add informative message from client to recipient
    if (!status.ok()){
      LOG(ERROR) << "Heartbeat failed to send from server " + config.serverId + ": " + status.error_message();
    }
    else {
      LOG(INFO) << "heartbeat sent successfully from server " + config.serverId;
    }

    std::this_thread::sleep_for(std::chrono::seconds(5));
  }
}

void RunServer(const ServerConfig& config) {
  std::string server_address = "0.0.0.0:"+ config.port;
  SNSServiceImpl service(config);

  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  LOG(INFO) << "Server listening on " + server_address;

  // Start heartbeat thread
  std::thread heartbeat_thread(sendHeartbeat, config);
  heartbeat_thread.detach(); // Run in background
  server->Wait();
}


int main(int argc, char** argv) {
  
  ServerConfig config;
  config.port = "3010";
    int opt = 0;
  while ((opt = getopt(argc, argv, "p:c:s:h:k:")) != -1){
    switch(opt) {
      case 'p':
          config.port = optarg;break;
      case 'c':
          config.clusterId = optarg;break;
      case 's':
          config.serverId = optarg;break;
      case 'h':
          config.coordinatorIP = optarg;break;
      case 'k':
          config.coordinatorPort = optarg;break;
      default:
    LOG(ERROR) << "Invalid Command Line Argument";
	  std::cerr << "Invalid Command Line Argument\n";
    }
  }
  

  
  std::string log_file_name = std::string("server-") + config.port;
  google::InitGoogleLogging(log_file_name.c_str());
  // google::InitGoogleLogging(argv[0]);
  FLAGS_log_dir = "./logs/cluster/" + config.clusterId + "/" + config.serverId + "/";
  // if the directory is not created, create one
  std::filesystem::create_directories(FLAGS_log_dir);

  // also, create the folder to store user file
  std::filesystem::create_directories("./cluster/" + config.clusterId + "/" + config.serverId + "/");

  //FLAGS_logtostderr = 1;
  FLAGS_alsologtostderr = 1;   // and also to stderr (terminal)
  FLAGS_colorlogtostderr = 1;  // optional: colored terminal logs
  //FLAGS_stderrthreshold = google::GLOG_FATAL;  // only FATAL to stderr
  FLAGS_logbufsecs = 0;  // set once after InitGoogleLogging






  //log(INFO, "Logging Initialized. Server starting...");
  LOG(INFO) << "Logging Initialized. Server starting...";
  RunServer(config);

  return 0;
}


