#include "gtest/gtest.h"
#include <memory>
#include <list>
#include <utility>
#include <iostream>
#include <string>
#include <exception>
#include <functional>
#include "test.h"
#include "priocpp/api.h"
#include "priocpp/task.h"
#include <signal.h>
#include <reproredis/redis.h>
 
using namespace prio;
using namespace reproredis;


class BasicTest : public ::testing::Test {
 protected:

  static void SetUpTestCase() {


  }

  virtual void SetUp() {
	 // MOL_TEST_PRINT_CNTS();
  }

  virtual void TearDown() {
	  MOL_TEST_PRINT_CNTS();
  }

}; // end test setup



#ifdef _RESUMABLE_FUNCTIONS_SUPPORTED

repro::Future<> coroutine_example(reproredis::RedisPool& redis, std::string& result);


TEST_F(BasicTest, Coroutine) {

	std::string result;
	{
		signal(SIGINT).then([](int s) {theLoop().exit(); });

		reproredis::RedisPool Redis("redis://localhost:6379/", 4);

		coroutine_example(Redis,result).then([](){});

		theLoop().run();
	}

	EXPECT_EQ("promised", result);
	MOL_TEST_ASSERT_CNTS(0, 0);
}


repro::Future<> coroutine_example(reproredis::RedisPool& redis, std::string& result)
{
	try
	{
		reproredis::RedisResult::Ptr r = co_await redis.cmd("SET", "promise-test", "promised");

		std::cout << "did set" << std::endl;

		reproredis::RedisResult::Ptr r2 = co_await r->cmd("GET", "promise-test");

		result = r2->str();
		std::cout << "did get " << result << std::endl;

		theLoop().exit();
	}
	catch (const std::exception& ex)
	{
		std::cout << "ex:t " << ex.what() << std::endl;
		theLoop().exit();
	}
	co_return;
}

#endif



TEST_F(BasicTest, RawRedis) 
{
	std::string result;
	{
		signal(SIGINT).then([](int s) {theLoop().exit(); });

		RedisPool redis("redis://localhost:6379");

		redis.cmd("LRANGE", "mylist", 0, -1)
		.then([](RedisResult::Ptr r)
		{
			std::cout  << "mylist size:" <<  r->size() << std::endl;
			theLoop().exit();
		})		
		.otherwise([](const std::exception& ex)
		{
			std::cout << ex.what() << std::endl;
			theLoop().exit();
		});

		theLoop().run();
	}

	MOL_TEST_ASSERT_CNTS(0, 0);		
}


TEST_F(BasicTest, RawRedisChained) 
{
	std::string result;
	{
		signal(SIGINT).then([](int s) {theLoop().exit(); });

		RedisPool redis("redis://localhost:6379");

		redis.cmd("LRANGE", "mylist", 0, -1)
		.then([](RedisResult::Ptr r)
		{
			std::cout  << "mylist size:" <<  r->size() << std::endl;
			return r->cmd("INFO");
		})			
		.then([](RedisResult::Ptr r)
		{
			std::cout << "INFO:" << r->str() << std::endl;
			theLoop().exit();
		})		
		.otherwise([](const std::exception& ex)
		{
			std::cout << ex.what() << std::endl;
			theLoop().exit();
		});

		theLoop().run();
	}
	MOL_TEST_ASSERT_CNTS(0, 0);		
}


TEST_F(BasicTest, RawRedisSubscribe) 
{
	std::string result;
	{
		signal(SIGINT).then([](int s) {theLoop().exit(); });

		RedisPool redis("redis://localhost:6379");

		RedisSubscriber sub(redis);


		prio::timeout([&redis]()
		{
			redis.cmd("publish", "mytopic", "HELO WORLD")
			.then([](RedisResult::Ptr r)
			{
				//std::cout  << "publisher" <<  r->str() << std::endl;
			})			
			.otherwise([](const std::exception& ex)
			{
				std::cout << ex.what() << std::endl;
				theLoop().exit();
			});
		}
		,1,0);
		

		sub.subscribe("mytopic")
		.then([&sub,&result](std::pair<std::string,std::string> msg)
		{
			std::cout  << "msg: " << msg.first << ": " << msg.second << std::endl;
			result = msg.second;
			theLoop().exit();
		})			
		.otherwise([](const std::exception& ex)
		{
			std::cout << ex.what() << std::endl;
			theLoop().exit();
		});

		theLoop().run();
	}


	EXPECT_EQ("HELO WORLD", result);
	MOL_TEST_ASSERT_CNTS(0, 0);	
}

/*

TEST_F(BasicTest, RawRedis) {

	std::string result;
	{
		signal(SIGINT).then([](int s) {theLoop().exit(); });

		RedisParser parser;
		prio::Connection::connect("localhost", 6379)
		.then([&parser](prio::Connection::Ptr con)
		{
			parser.con = con;
			//return con->write("*1\r\n$4\r\nINFO\r\n");
			//llen mylist
			//return con->write("*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n");
			// rpush mylist XXX
			//return con->write("*3\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$3\r\nXXX\r\n");
			// simple str set a b
			//return con->write("*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\nb\r\n");
			Serializer serializer;
			std::string cmd = serializer.serialize("LRANGE", "mylist", 0, -1);
			return con->write(cmd);
			//return con->write("*4\r\n$6\r\nLRANGE\r\n$6\r\nmylist\r\n$1\r\n0\r\n$2\r\n-1\r\n");
			//return con->write("*2\r\n$3\r\nGET\r\n$2\r\naa\r\n");
			//return con->write("*4\r\n$4\r\nLSET\r\n$6\r\nmylist\r\n$3\r\n777\r\n$2\r\n-1\r\n");
		})
		.then([&parser](prio::Connection::Ptr con)
		{
			return parser.parse();
		})
		.then([](RedisResult* r)
		{
			theLoop().exit();
		})		
		.otherwise([](const std::exception& ex)
		{
			std::cout << ex.what() << std::endl;
			theLoop().exit();
		});

		theLoop().run();
	}

	EXPECT_EQ("promised", result);
	MOL_TEST_ASSERT_CNTS(0, 0);
}

*/

int main(int argc, char **argv) {

	prio::init();

    ::testing::InitGoogleTest(&argc, argv);
    int r = RUN_ALL_TESTS();

    return r;
}
