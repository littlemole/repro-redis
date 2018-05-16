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
			sub.unsubscribe(); // will cause a read error
//			theLoop().exit();
			timeout([]()
			{
			
				nextTick([]()
				{
					theLoop().exit();
				});
				
			},0,500);
			
		})			
		.otherwise([](const std::exception& ex)
		{
			std::cout << "!" << ex.what() << std::endl;
			theLoop().exit();
		});

		theLoop().run();
	}


	EXPECT_EQ("HELO WORLD", result);
	MOL_TEST_ASSERT_CNTS(0, 0);	
}


int main(int argc, char **argv) {

	prio::init();

    ::testing::InitGoogleTest(&argc, argv);
    int r = RUN_ALL_TESTS();

    return r;
}
