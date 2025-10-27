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
#include <glog/logging.h>

#include "sns.grpc.pb.h"

#include "coordinator.grpc.pb.h" // newly added

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
using csce438::CoordService; // newly_added
using csce438::ServerInfo; // new added
using csce438::ID; // newly added

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
	 const std::string& p,
   const std::string& coordinatorPort)
    :hostname(hname), username(uname), port(p), coordinatorPort(coordinatorPort) {}

  
protected:
  virtual int connectTo();
  virtual IReply processCommand(std::string& input);
  virtual void processTimeline();

private:
  std::string hostname;
  std::string username;
  std::string port;
  std::string coordinatorPort;
  
  
  // You can have an instance of the client stub
  // as a member variable.
  std::unique_ptr<SNSService::Stub> stub_;
  std::unique_ptr<CoordService::Stub> coordinator_stub_; // new added
  
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
  // This is the address to the coordinator
  std::string coordinator_addr = hostname + ":" + this -> coordinatorPort;
  auto channel = grpc::CreateChannel(coordinator_addr, grpc::InsecureChannelCredentials());
  coordinator_stub_ = CoordService::NewStub(channel);
  // GetServer(ServerContext* context, const ID* id, ServerInfo* serverinfo) 
  ID user_id;

  // turn the string to integer
  user_id.set_id(std::stoi(this -> username)); 
  //std::cout << "The new username is..." << std::stoi(this -> username) << std::endl;
  ClientContext ctx;
  ServerInfo svInfo;
  // The client stub expects a reference instead of pointer
  Status s = coordinator_stub_ -> GetServer(&ctx, user_id, &svInfo);
  if (!s.ok()){
  //   std::cout << "We received the message from the coordinator, now unpacking the message..." << std::endl;
  // }
  // else {
    return -1;
  }



  std::cout << "Connect to ..." <<std::endl;
  std::cout << "Hostname: " << svInfo.hostname() << std::endl;
  std::cout << "Port: " << svInfo.port() << std::endl;

  
  std::string dest_hostname = svInfo.hostname();
  std::string dest_port = svInfo.port();

  // This is the address to the server
  std::string server_addr = dest_hostname + ":" + dest_port;
  // std::cout << "conntecting to ..." << server_addr << std::endl;
  auto channel_server = grpc::CreateChannel(server_addr, grpc::InsecureChannelCredentials());
  stub_ = SNSService::NewStub(channel_server);

  // call Login rpc
  IReply ire = Login();

  // if (ire.comm_status ==  FAILURE_ALREADY_EXISTS){
  //   return -1;
  // }
  // else if (ire.comm_status == FAILURE_INVALID)

  if (ire.comm_status == SUCCESS){
      // log(INFO, "Client connected to server...");
      return 1;
  }
  else {
     return -1;
  }


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
    
  
    // std::cout << "the input typed: " <<  input << std::endl;
    // The function should be able to parse the following four command (FOLLOW, UNFOLLOW, LIST, TIMELINE)
    std::istringstream iss(input);
    std::vector<std::string> tokens;
    std::string token;
    while (iss >>token){
      tokens.push_back(token);
    }

    std::string action = tokens[0];
    if (action == "FOLLOW"){ 
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
    // std::cout << "Entered here..." <<std::endl;
    // ClientContext ctx;  
    // auto stream = stub_->Timeline(&ctx);
    // if (!stream) {
    //   std::cout << "failed to create stream\n";
    //   return;
    // }

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

    if (rep.msg() == "Follow successful"){
      // follow successful
      ire.comm_status = SUCCESS;
    }
    else if (rep.msg() == "A person can't follow himself."){
      ire.comm_status = FAILURE_ALREADY_EXISTS;
    }
    else {
      ire.comm_status = FAILURE_INVALID_USERNAME;
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
    ire.grpc_status = s;
    if (rep.msg() == "Unfollow successful."){
      // unfollowed succeeded
      ire.comm_status = SUCCESS;
    }
    else {
      // the unfollow failed!
      //std::cout << rep.msg() <<" ..............." << std::endl;
      ire.comm_status =FAILURE_INVALID_USERNAME;
    }


    return ire;
}

// Login Command  
IReply Client::Login() {
    // std::cout << "entered here" << std::endl;
    IReply out;
  
    Request req;
    req.set_username(username);

    Reply rep;
    ClientContext ctx;
    Status s = stub_ -> Login(&ctx, req, &rep);

    if (!s.ok()) {
        out.comm_status = FAILURE_INVALID;   // or map per status.error_code()
        out.grpc_status = s;
        return out;
    }

    
    // OK — now use `rep`
    if (rep.msg() == "ALREADY_LOGGED_IN") {
        out.comm_status = FAILURE_ALREADY_EXISTS;
    } else {
        out.comm_status = SUCCESS;
    }
    out.grpc_status = s;
    return out;

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

    //std::cout << "Now you are in timeline mode!" << std::endl;
    ClientContext ctx;
    std::unique_ptr<grpc::ClientReaderWriter<Message, Message>> stream(stub_->Timeline(&ctx));
    

    // initialize the first message
    Message hello;
    hello.set_username(username);
    // stream->Write(hello);
    // Try writing the first message
    if (!stream->Write(hello)) {
        //std::cerr << "Initial write failed (server may be down or closed stream)\n";
        std::cerr << "Command failed \n";
        stream->WritesDone();
        grpc::Status s = stream->Finish();
        // std::cerr << "Status: " << s.error_message() 
        //           << " (code=" << s.error_code() << ")\n";
        
        return;
    }

    std::atomic<bool> done{false};

    std::thread writer([&]{
      while (!done.load()){
        std::string text = getPostMessage();
        if (text.empty()) continue;
        // in tsc.cc
        Message msg = MakeMessage(username, text);
        if (!stream->Write(msg)) {
            break; // server closed
        }
      }
      stream->WritesDone();
    });


    std::thread reader([&] {
      Message in;
        // when the server closes the stream, Read() returns falsethread 
        while (stream->Read(&in)) {
            // in client.cc
            std::time_t tt = static_cast<std::time_t>(in.timestamp().seconds());
            displayPostMessage(in.username(), in.msg(), tt);
        }
        // if the server disconnects, the reader threads sets done = true, but the
        // writer thread might still be stuck inside getPostMessage()
        done.store(true);
    });

    reader.join();
    writer.join();

    grpc::Status s = stream->Finish();

}



//////////////////////////////////////////////
// Main Function
/////////////////////////////////////////////
int main(int argc, char** argv) {

  google::InitGoogleLogging(argv[0]);
  FLAGS_log_dir = "./logs";
  FLAGS_alsologtostderr = 1;   // and also to stderr (terminal)
  FLAGS_colorlogtostderr = 1;  // optional: colored terminal logs
  FLAGS_logbufsecs = 0;  // set once after InitGoogleLogging

  LOG(INFO) << "Client starting...";

  std::string hostname = "localhost";
  std::string username = "default";
  std::string port = "3010";
  std::string coordinatorPort = "9090";
    
  int opt = 0;
  while ((opt = getopt(argc, argv, "h:u:p:k:")) != -1){
    switch(opt) {
    case 'h':
      hostname = optarg;break;
    case 'u':
      username = optarg;break;
    case 'p':
      port = optarg;break;
    case 'k':
      coordinatorPort = optarg;break;
    default:
      std::cout << "Invalid Command Line Argument\n";
    }
  }

  // std::cout << "hostname " << hostname << " " << "username " << username << " " << "port " << port << " " << "coordinatorPort " << " " <<  coordinatorPort <<std::endl;
      
  std::cout << "Logging Initialized. Client starting...";
  
  Client myc(hostname, username, port, coordinatorPort);
  
  myc.run();
  
  return 0;
}
