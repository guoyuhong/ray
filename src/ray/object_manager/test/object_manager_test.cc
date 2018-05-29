#include <iostream>
#include <thread>

#include "gtest/gtest.h"

#include "ray/object_manager/object_manager.h"

namespace ray {

static inline void flushall_redis(void) {
  redisContext *context = redisConnect("127.0.0.1", 6379);
  freeReplyObject(redisCommand(context, "FLUSHALL"));
  redisFree(context);
}

std::string store_executable;

class MockServer {
 public:
  MockServer(boost::asio::io_service &main_service,
             const ObjectManagerConfig &object_manager_config,
             std::shared_ptr<gcs::AsyncGcsClient> gcs_client)
      : object_manager_acceptor_(
            main_service, boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), 0)),
        object_manager_socket_(main_service),
        gcs_client_(gcs_client),
        object_manager_(main_service, object_manager_config, gcs_client) {
    RAY_CHECK_OK(RegisterGcs(main_service));
    // Start listening for clients.
    DoAcceptObjectManager();
  }

  ~MockServer() { RAY_CHECK_OK(gcs_client_->client_table().Disconnect()); }

 private:
  ray::Status RegisterGcs(boost::asio::io_service &io_service) {
    RAY_RETURN_NOT_OK(gcs_client_->Connect("127.0.0.1", 6379));
    RAY_RETURN_NOT_OK(gcs_client_->Attach(io_service));

    boost::asio::ip::tcp::endpoint endpoint = object_manager_acceptor_.local_endpoint();
    std::string ip = endpoint.address().to_string();
    unsigned short object_manager_port = endpoint.port();

    ClientTableDataT client_info = gcs_client_->client_table().GetLocalClient();
    client_info.node_manager_address = ip;
    client_info.node_manager_port = object_manager_port;
    client_info.object_manager_port = object_manager_port;
    ray::Status status = gcs_client_->client_table().Connect(client_info);
    object_manager_.RegisterGcs();
    return status;
  }

  void DoAcceptObjectManager() {
    object_manager_acceptor_.async_accept(
        object_manager_socket_, boost::bind(&MockServer::HandleAcceptObjectManager, this,
                                            boost::asio::placeholders::error));
  }

  void HandleAcceptObjectManager(const boost::system::error_code &error) {
    ClientHandler<boost::asio::ip::tcp> client_handler =
        [this](TcpClientConnection &client) { object_manager_.ProcessNewClient(client); };
    MessageHandler<boost::asio::ip::tcp> message_handler = [this](
        std::shared_ptr<TcpClientConnection> client, int64_t message_type,
        const uint8_t *message) {
      object_manager_.ProcessClientMessage(client, message_type, message);
    };
    // Accept a new local client and dispatch it to the node manager.
    auto new_connection = TcpClientConnection::Create(client_handler, message_handler,
                                                      std::move(object_manager_socket_));
    DoAcceptObjectManager();
  }

  friend class TestObjectManagerCommands;

  boost::asio::ip::tcp::acceptor object_manager_acceptor_;
  boost::asio::ip::tcp::socket object_manager_socket_;
  std::shared_ptr<gcs::AsyncGcsClient> gcs_client_;
  ObjectManager object_manager_;
};

class TestObjectManager : public ::testing::Test {
 public:
  TestObjectManager() {}

  std::string StartStore(const std::string &id) {
    std::string store_id = "/tmp/store";
    store_id = store_id + id;
    std::string store_pid = store_id + ".pid";
    std::string plasma_command = store_executable + " -m 1000000000 -s " + store_id +
                                 " 1> /dev/null 2> /dev/null &" + " echo $! > " +
                                 store_pid;

    RAY_LOG(DEBUG) << plasma_command;
    int ec = system(plasma_command.c_str());
    RAY_CHECK(ec == 0);
    sleep(1);
    return store_id;
  }

  void StopStore(std::string store_id) {
    std::string store_pid = store_id + ".pid";
    std::string kill_1 = "kill -9 `cat " + store_pid + "`";
    ASSERT_TRUE(!system(kill_1.c_str()));
  }

  void SetUp() {
    flushall_redis();

    // start store
    store_id_1 = StartStore(UniqueID::from_random().hex());
    store_id_2 = StartStore(UniqueID::from_random().hex());
    store_id_3 = StartStore(UniqueID::from_random().hex());

    uint pull_timeout_ms = 1;
    int max_sends = 2;
    int max_receives = 2;
    uint64_t object_chunk_size = static_cast<uint64_t>(std::pow(10, 3));
    server3_push_timeout_ms = 1000;

    // start first server
    gcs_client_1 = std::shared_ptr<gcs::AsyncGcsClient>(new gcs::AsyncGcsClient());
    ObjectManagerConfig om_config_1;
    om_config_1.store_socket_name = store_id_1;
    om_config_1.pull_timeout_ms = pull_timeout_ms;
    om_config_1.max_sends = max_sends;
    om_config_1.max_receives = max_receives;
    om_config_1.object_chunk_size = object_chunk_size;
    // Push will stop immediately if local object is not satisfied.
    om_config_1.push_timeout_ms = 0;
    server1.reset(new MockServer(main_service, om_config_1, gcs_client_1));

    // start second server
    gcs_client_2 = std::shared_ptr<gcs::AsyncGcsClient>(new gcs::AsyncGcsClient());
    ObjectManagerConfig om_config_2;
    om_config_2.store_socket_name = store_id_2;
    om_config_2.pull_timeout_ms = pull_timeout_ms;
    om_config_2.max_sends = max_sends;
    om_config_2.max_receives = max_receives;
    om_config_2.object_chunk_size = object_chunk_size;
    // Push will wait infinitely until local object is satisfied.
    om_config_2.push_timeout_ms = -1;
    server2.reset(new MockServer(main_service, om_config_2, gcs_client_2));

    // start third server
    gcs_client_3 = std::shared_ptr<gcs::AsyncGcsClient>(new gcs::AsyncGcsClient());
    ObjectManagerConfig om_config_3;
    om_config_3.store_socket_name = store_id_3;
    om_config_3.pull_timeout_ms = pull_timeout_ms;
    om_config_3.max_sends = max_sends;
    om_config_3.max_receives = max_receives;
    om_config_3.object_chunk_size = object_chunk_size;
    // Push will stop after waiting for 1 second.
    om_config_3.push_timeout_ms = server3_push_timeout_ms;
    server3.reset(new MockServer(main_service, om_config_3, gcs_client_3));

    // connect to stores.
    ARROW_CHECK_OK(client1.Connect(store_id_1, "", plasma::kPlasmaDefaultReleaseDelay));
    ARROW_CHECK_OK(client2.Connect(store_id_2, "", plasma::kPlasmaDefaultReleaseDelay));
    ARROW_CHECK_OK(client3.Connect(store_id_3, "", plasma::kPlasmaDefaultReleaseDelay));
  }

  void TearDown() {
    arrow::Status client1_status = client1.Disconnect();
    arrow::Status client2_status = client2.Disconnect();
    arrow::Status client3_status = client3.Disconnect();
    ASSERT_TRUE(client1_status.ok() && client2_status.ok() && client3_status.ok());

    this->server1.reset();
    this->server2.reset();
    this->server3.reset();

    StopStore(store_id_1);
    StopStore(store_id_2);
    StopStore(store_id_3);
  }

  ObjectID WriteDataToClient(plasma::PlasmaClient &client, int64_t data_size,
                             ObjectID object_id) {
    RAY_LOG(DEBUG) << "ObjectID Created: " << object_id;
    uint8_t metadata[] = {5};
    int64_t metadata_size = sizeof(metadata);
    std::shared_ptr<Buffer> data;
    ARROW_CHECK_OK(client.Create(object_id.to_plasma_id(), data_size, metadata,
                                 metadata_size, &data));
    ARROW_CHECK_OK(client.Seal(object_id.to_plasma_id()));
    return object_id;
  }

  void object_added_handler_1(ObjectID object_id) { v1.push_back(object_id); };

  void object_added_handler_2(ObjectID object_id) { v2.push_back(object_id); };

  void object_added_handler_3(ObjectID object_id) { v3.push_back(object_id); };

 protected:
  std::thread p;
  boost::asio::io_service main_service;
  std::shared_ptr<gcs::AsyncGcsClient> gcs_client_1;
  std::shared_ptr<gcs::AsyncGcsClient> gcs_client_2;
  std::shared_ptr<gcs::AsyncGcsClient> gcs_client_3;
  std::unique_ptr<MockServer> server1;
  std::unique_ptr<MockServer> server2;
  std::unique_ptr<MockServer> server3;

  plasma::PlasmaClient client1;
  plasma::PlasmaClient client2;
  plasma::PlasmaClient client3;
  std::vector<ObjectID> v1;
  std::vector<ObjectID> v2;
  std::vector<ObjectID> v3;

  std::string store_id_1;
  std::string store_id_2;
  std::string store_id_3;

  uint server3_push_timeout_ms;
};

class TestObjectManagerCommands : public TestObjectManager {
 public:
  int num_connected_clients = 0;
  ClientID client_id_1;
  ClientID client_id_2;
  ClientID client_id_3;

  ObjectID created_object_id1;
  ObjectID created_object_id2;
  ObjectID created_object_id3;
  ObjectID created_object_id4;

  std::unique_ptr<boost::asio::deadline_timer> timer1;
  std::unique_ptr<boost::asio::deadline_timer> timer2;

  void WaitConnections() {
    client_id_1 = gcs_client_1->client_table().GetLocalClientId();
    client_id_2 = gcs_client_2->client_table().GetLocalClientId();
    client_id_3 = gcs_client_3->client_table().GetLocalClientId();
    gcs_client_1->client_table().RegisterClientAddedCallback([this](
        gcs::AsyncGcsClient *client, const ClientID &id, const ClientTableDataT &data) {
      ClientID parsed_id = ClientID::from_binary(data.client_id);
      if (parsed_id == client_id_1 || parsed_id == client_id_2 ||
          parsed_id == client_id_3) {
        num_connected_clients += 1;
      }
      if (num_connected_clients == 3) {
        StartTests();
      }
    });
  }

  void StartTests() {
    TestConnections();
    TestNotifications();
  }

  void TestNotifications() {
    ray::Status status = ray::Status::OK();
    status = server1->object_manager_.SubscribeObjAdded(
        [this](const ObjectInfoT &object_info) {
          object_added_handler_1(ObjectID::from_binary(object_info.object_id));
          NotificationTestCompleteIfSatisfied();
        });
    RAY_CHECK_OK(status);
    status = server2->object_manager_.SubscribeObjAdded(
        [this](const ObjectInfoT &object_info) {
          object_added_handler_2(ObjectID::from_binary(object_info.object_id));
          NotificationTestCompleteIfSatisfied();
        });
    RAY_CHECK_OK(status);
    status = server3->object_manager_.SubscribeObjAdded(
        [this](const ObjectInfoT &object_info) {
          object_added_handler_3(ObjectID::from_binary(object_info.object_id));
          NotificationTestCompleteIfSatisfied();
        });
    RAY_CHECK_OK(status);

    uint data_size = 1000000;

    created_object_id1 = ObjectID::from_random();
    WriteDataToClient(client1, data_size, created_object_id1);
    // Server1 holds Object1 so this request from Server2 will be satisfied.
    status = server2->object_manager_.Pull(created_object_id1);

    created_object_id2 = ObjectID::from_random();
    // Object2 will be added to server2 in 2s.
    // Server1 won't have Object2, so Push will stop retrying acorrding to the setting.
    status = server1->object_manager_.Push(
        created_object_id2, gcs_client_2->client_table().GetLocalClientId());
    // Server2 will wait infinitely. After 2s waiting, it will do the Push.
    status = server2->object_manager_.Push(
        created_object_id2, gcs_client_1->client_table().GetLocalClientId());

    created_object_id3 = ObjectID::from_random();
    created_object_id4 = ObjectID::from_random();
    // Object3 will be added to Server3 in 0.1s, which won't cause Push timeout.
    status = server3->object_manager_.Push(
        created_object_id3, gcs_client_1->client_table().GetLocalClientId());
    status = server3->object_manager_.Push(
        created_object_id3, gcs_client_2->client_table().GetLocalClientId());
    // Object4 will be added to server3 in 2s, which will cause Push timeout.
    status = server3->object_manager_.Push(
        created_object_id4, gcs_client_1->client_table().GetLocalClientId());
    status = server3->object_manager_.Push(
        created_object_id4, gcs_client_2->client_table().GetLocalClientId());

    // Write Object3 to Server3 after 0.1s waiting.
    timer1.reset(new boost::asio::deadline_timer(main_service));
    auto period1 = boost::posix_time::milliseconds(server3_push_timeout_ms / 10);
    timer1->expires_from_now(period1);
    timer1->async_wait([this, data_size](const boost::system::error_code &error) {
      WriteDataToClient(client3, data_size, created_object_id3);
    });
    // Write Object2 to Server2 and Object4 to Server3 after 2s waiting.
    timer2.reset(new boost::asio::deadline_timer(main_service));
    auto period2 = boost::posix_time::milliseconds(2 * server3_push_timeout_ms);
    timer2->expires_from_now(period2);
    timer2->async_wait([this, data_size](const boost::system::error_code &error) {
      WriteDataToClient(client2, data_size, created_object_id2);
      WriteDataToClient(client3, data_size, created_object_id4);
    });
  }

  void NotificationTestCompleteIfSatisfied() {
    uint num_expected_objects1 = 3;
    uint num_expected_objects2 = 3;
    uint num_expected_objects3 = 2;
    if (v1.size() == num_expected_objects1 && v2.size() == num_expected_objects2 &&
        v3.size() == num_expected_objects3) {
      main_service.stop();
    }
  }

  void TestConnections() {
    RAY_LOG(DEBUG) << "\n"
                   << "Server client ids:"
                   << "\n";
    const ClientTableDataT &data = gcs_client_1->client_table().GetClient(client_id_1);
    RAY_LOG(DEBUG) << (ClientID::from_binary(data.client_id) == ClientID::nil());
    RAY_LOG(DEBUG) << "Server 1 ClientID=" << ClientID::from_binary(data.client_id);
    RAY_LOG(DEBUG) << "Server 1 ClientIp=" << data.node_manager_address;
    RAY_LOG(DEBUG) << "Server 1 ClientPort=" << data.node_manager_port;
    ASSERT_EQ(client_id_1, ClientID::from_binary(data.client_id));
    const ClientTableDataT &data2 = gcs_client_1->client_table().GetClient(client_id_2);
    RAY_LOG(DEBUG) << "Server 2 ClientID=" << ClientID::from_binary(data2.client_id);
    RAY_LOG(DEBUG) << "Server 2 ClientIp=" << data2.node_manager_address;
    RAY_LOG(DEBUG) << "Server 2 ClientPort=" << data2.node_manager_port;
    ASSERT_EQ(client_id_2, ClientID::from_binary(data2.client_id));
    const ClientTableDataT &data3 = gcs_client_1->client_table().GetClient(client_id_3);
    RAY_LOG(DEBUG) << "Server 3 ClientID=" << ClientID::from_binary(data3.client_id);
    RAY_LOG(DEBUG) << "Server 3 ClientIp=" << data3.node_manager_address;
    RAY_LOG(DEBUG) << "Server 3 ClientPort=" << data3.node_manager_port;
    ASSERT_EQ(client_id_3, ClientID::from_binary(data3.client_id));
  }
};

TEST_F(TestObjectManagerCommands, StartTestObjectManagerCommands) {
  auto AsyncStartTests = main_service.wrap([this]() { WaitConnections(); });
  AsyncStartTests();
  main_service.run();
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  ray::store_executable = std::string(argv[1]);
  return RUN_ALL_TESTS();
}
