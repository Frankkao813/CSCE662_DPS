#include <iostream>
#include <memory>
#include <thread>
#include <vector>
#include <string>
#include <unistd.h>
#include <csignal>
#include <grpc++/grpc++.h>
#include "client.h"
#include <sstream>

#include "sns.grpc.pb.h"
using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::ClientReaderWriter;
using grpc::ClientWriter;
using grpc::Status;
using csce438::Message;
using csce438::ListReply;
using csce438::Request;
using csce438::Reply;
using csce438::SNSService;

void sig_ignore(int sig) {
  std::cout << "Signal caught " + sig;
}

Message MakeMessage(const std::string& username, const std::string& msg) {
    Message m;
    m.set_username(username);
    m.set_msg(msg);
    google::protobuf::Timestamp* timestamp = new google::protobuf::Timestamp();
    timestamp->set_seconds(time(NULL));
    timestamp->set_nanos(0);
    m.set_allocated_timestamp(timestamp);
    return m;
}


class Client : public IClient
{
public:
  Client(const std::string& hname,
	 const std::string& uname,
	 const std::string& p)
    :hostname(hname), username(uname), port(p) {}

  
protected:
  virtual int connectTo();
  virtual IReply processCommand(std::string& input);
  virtual void processTimeline();

private:
  std::string hostname;
  std::string username;
  std::string port;
  
  // You can have an instance of the client stub
  // as a member variable.
  std::unique_ptr<SNSService::Stub> stub_;
  
  IReply Login();
  IReply List();
  IReply Follow(const std::string &username);
  IReply UnFollow(const std::string &username);
  void   Timeline(const std::string &username);
};


///////////////////////////////////////////////////////////
//
//////////////////////////////////////////////////////////
int Client::connectTo()
{
  // ------------------------------------------------------------
  // In this function, you are supposed to create a stub so that
  // you call service methods in the processCommand/porcessTimeline
  // functions. That is, the stub should be accessible when you want
  // to call any service methods in those functions.
  // Please refer to gRpc tutorial how to create a stub.
  // ------------------------------------------------------------
    
///////////////////////////////////////////////////////////
// YOUR CODE HERE
//////////////////////////////////////////////////////////
  std::string server_addr = hostname + ":" + port;
  std::cout << "conntecting to ..." << server_addr << std::endl;
  auto channel = grpc::CreateChannel(server_addr, grpc::InsecureChannelCredentials());
  stub_ = SNSService::NewStub(channel);
  // call Login rpc
  IReply ire = Login();

  if (ire.comm_status ==  FAILURE_ALREADY_EXISTS){
    return -1;
  }
  return 1;
}

IReply Client::processCommand(std::string& input)
{
  // ------------------------------------------------------------
  // GUIDE 1:
  // In this function, you are supposed to parse the given input
  // command and create your own message so that you call an 
  // appropriate service method. The input command will be one
  // of the followings:
  //
  // FOLLOW <username>
  // UNFOLLOW <username>
  // LIST
  // TIMELINE
  // ------------------------------------------------------------
  
  // ------------------------------------------------------------
  // GUIDE 2:
  // Then, you should create a variable of IReply structure
  // provided by the client.h and initialize it according to
  // the result. Finally you can finish this function by returning
  // the IReply.
  // ------------------------------------------------------------
  
  
  // ------------------------------------------------------------
  // HINT: How to set the IReply?
  // Suppose you have "FOLLOW" service method for FOLLOW command,
  // IReply can be set as follow:
  // 
  //     // some codes for creating/initializing parameters for
  //     // service method
  //     IReply ire;
  //     grpc::Status status = stub_->FOLLOW(&context, /* some parameters */);
  //     ire.grpc_status = status;
  //     if (status.ok()) {
  //         ire.comm_status = SUCCESS;
  //     } else {
  //         ire.comm_status = FAILURE_NOT_EXISTS;
  //     }
  //      
  //      return ire;
  // 
  // IMPORTANT: 
  // For the command "LIST", you should set both "all_users" and 
  // "following_users" member variable of IReply.
  // ------------------------------------------------------------

    IReply ire;
    
  
    std::cout << "the input typed: " <<  input << std::endl;
    // The function should be able to parse the following four command (FOLLOW, UNFOLLOW, LIST, TIMELINE)
    std::istringstream iss(input);
    std::vector<std::string> tokens;
    std::string token;
    while (iss >>token){
      tokens.push_back(token);
    }

    std::string action = tokens[0];
    if (action == "FOLLOW"){ // TODO: handle the case where tokens > 2?
      ire = Follow(tokens[1]);
    }
    else if (action == "UNFOLLOW"){
      ire = UnFollow(tokens[1]);
    }
    else if (action == "LIST"){
      ire = List();
    }
    else if (action == "TIMELINE"){
      Client::processTimeline();
    }
    else {
      std::cout << "invalid command";
    }

    return ire;
}


// processTimeline() calls Timeline()
void Client::processTimeline()
{
    Timeline(username);
}

// List Command
IReply Client::List() {

  IReply ire;

  Request req;
  req.set_username(username);

  ListReply listRep;
  ClientContext ctx;
  Status s = stub_ -> List(&ctx, req, &listRep);
  ire.grpc_status = s;
  if (!s.ok()) {
    ire.comm_status = FAILURE_UNKNOWN;          
    return ire;
  }

  ire.comm_status = SUCCESS;
    
  ire.all_users.clear();
  for (int i = 0; i < listRep.all_users_size(); ++i) {
    ire.all_users.emplace_back(listRep.all_users(i));
  }

  // Copy repeated followers -> ire.all_followers
  ire.followers.clear();
  for (int i = 0; i < listRep.followers_size(); ++i) {
    ire.followers.emplace_back(listRep.followers(i));
  }

    return ire;
}

// Follow Command        
IReply Client::Follow(const std::string& username2) {

    IReply ire; 
    Request req;
    req.set_username(username);
    req.add_arguments(username2);
    Reply rep;
    ClientContext ctx;
    Status s = stub_ -> Follow(&ctx, req, &rep);
    ire.grpc_status = s;
    if (rep.msg() == "Already joined"){
      // follow failed
      ire.comm_status = FAILURE_INVALID_USERNAME;
      
    }
    else if (rep.msg() == "Follow successful"){
      // follow successful
      ire.comm_status = SUCCESS;
    }


    return ire;
}

// UNFollow Command  
IReply Client::UnFollow(const std::string& username2) {

    IReply ire;

    Request req;
    req.set_username(username);
    req.add_arguments(username2);
    Reply rep;
    ClientContext ctx;
    Status s = stub_ -> UnFollow(&ctx, req, &rep);

    if (reply.msg() == "Not a follower."){
      // unfollow failed
    }
    else if (reply.msg() == "Unfollow successful"){
      // unfollowed succeeded
    }


    return ire;
}

// Login Command  
IReply Client::Login() {
    std::cout << "entered here" << std::endl;
    IReply ire;
  
    Request req;
    req.set_username(username);

    Reply rep;
    ClientContext ctx;
    Status s = stub_ -> Login(&ctx, req, &rep);

    // getter
    if (rep.msg() == "ALREADY_LOGGED_IN"){
      ire.comm_status = FAILURE_ALREADY_EXISTS;
      ire.grpc_status = s;
    }
    else {
      ire.comm_status = SUCCESS;
      ire.grpc_status = s;
    }

    return ire;
}

// Timeline Command
void Client::Timeline(const std::string& username) {

    // ------------------------------------------------------------
    // In this function, you are supposed to get into timeline mode.
    // You may need to call a service method to communicate with
    // the server. Use getPostMessage/displayPostMessage functions 
    // in client.cc file for both getting and displaying messages 
    // in timeline mode.
    // ------------------------------------------------------------

    // ------------------------------------------------------------
    // IMPORTANT NOTICE:
    //
    // Once a user enter to timeline mode , there is no way
    // to command mode. You don't have to worry about this situation,
    // and you can terminate the client program by pressing
    // CTRL-C (SIGINT)
    // ------------------------------------------------------------
  
    /***
    YOUR CODE HERE
    ***/

}



//////////////////////////////////////////////
// Main Function
/////////////////////////////////////////////
int main(int argc, char** argv) {

  std::string hostname = "localhost";
  std::string username = "default";
  std::string port = "3010";
    
  int opt = 0;
  while ((opt = getopt(argc, argv, "h:u:p:")) != -1){
    switch(opt) {
    case 'h':
      hostname = optarg;break;
    case 'u':
      username = optarg;break;
    case 'p':
      port = optarg;break;
    default:
      std::cout << "Invalid Command Line Argument\n";
    }
  }
      
  std::cout << "Logging Initialized. Client starting...";
  
  Client myc(hostname, username, port);
  
  myc.run();
  
  return 0;
}
