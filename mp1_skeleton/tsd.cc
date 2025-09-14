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

#include <ctime>

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
#define log(severity, msg) LOG(severity) << msg; google::FlushLogFiles(google::severity); 

#include "sns.grpc.pb.h"


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


class SNSServiceImpl final : public SNSService::Service {


  
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
      reply -> set_msg("Already joined");
      return Status::OK; 
    }

    // Failure: A person can't follow non-existent person
    if (findClientByName(target) == nullptr){
      reply -> set_msg("Following non-existent user.");
      return Status::OK;
    }

    // TODO: handle duplicate push cade
    Client* c1 = findClientByName(uname);
    Client* c2 = findClientByName(target);
    if (c1 != nullptr && c2 != nullptr){
      c1 -> client_following.push_back(c2);
      c2 -> client_followers.push_back(c1);
    }
    reply -> set_msg("Follow successful");


    return Status::OK; 
  }

  Status UnFollow(ServerContext* context, const Request* request, Reply* reply) override {

    // fetch the username and the argument
    std::string uname = request -> username();
    const std::string target = request->arguments(0);
    if (uname == target){
      reply -> set_msg("You Can't unfollow yourself.");
    }


    // c1 follows c2 -> c2 follower [c1]; c1 following [c2] 
    Client* c1 = findClientByName(uname);
    Client* c2 = findClientByName(target);

    if (!c1) { reply->set_msg("Requester does not exist."); return Status::OK; }
    if (!c2) { reply->set_msg("Target user does not exist."); return Status::OK; }
    const bool relation_exists = isInVector(c1->client_following, c2) && isInVector(c2->client_followers, c1);
    if (relation_exists){
      reply -> set_msg("You are not a follower.");
    }

    eraseFromVector(c1->client_following, c2);
    eraseFromVector(c2->client_followers, c1);

    reply->set_msg("Unfollow successful.");
          

    return Status::OK;
  }

  // RPC Login
  Status Login(ServerContext* context, const Request* request, Reply* reply) override {

    std::cout << "Hello World!!" << std::endl;
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

    return Status::OK;
  }

  Status Timeline(ServerContext* context, 
		ServerReaderWriter<Message, Message>* stream) override {
    Message first_in;
    if (!stream -> Read(&first_in)){
      return Status::OK;
    }

    const std::string u1 = first_in.username();
    if (u1.empty()){
      return Status::OK;
    }

    std::cout << "user posted" << u1 << std::endl;



    
    return Status::OK;
  }

};

void RunServer(std::string port_no) {
  std::string server_address = "0.0.0.0:"+port_no;
  SNSServiceImpl service;

  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;
  log(INFO, "Server listening on "+server_address);

  server->Wait();
}




int main(int argc, char** argv) {

  std::string port = "3010";
  
  int opt = 0;
  while ((opt = getopt(argc, argv, "p:")) != -1){
    switch(opt) {
      case 'p':
          port = optarg;break;
      default:
	  std::cerr << "Invalid Command Line Argument\n";
    }
  }
  
  std::string log_file_name = std::string("server-") + port;
  google::InitGoogleLogging(log_file_name.c_str());
  log(INFO, "Logging Initialized. Server starting...");
  RunServer(port);

  return 0;
}


