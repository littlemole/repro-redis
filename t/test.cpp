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


TEST_F(BasicTest, SimpleRedis) {

	std::string result;
	{
		signal(SIGINT).then([](int s){theLoop().exit(); });

		reproredis::RedisPool Redis("redis://localhost:6379/",4);

		{
			Redis->cmd("SET", "promise-test", "promised")
			.then([&Redis](reproredis::Reply r)
			{
				std::cout << "did set" << std::endl;
				return Redis
				->cmd("GET", "promise-test");
			})
			.then( [&result](reproredis::Reply r)
			{
				result = r.asString();
				std::cout << "did get " << result << std::endl;
				timeout( []() {
					theLoop().exit();
				},0,1);
			})
			.otherwise( [](const std::exception& ex)
			{
				std::cout << "Ex: " << ex.what() << std::endl;
			});
		}
		theLoop().run();
	}

	EXPECT_EQ("promised",result);
	MOL_TEST_ASSERT_CNTS(0,0);
}



TEST_F(BasicTest, SeveralRedisRequestsWithoutPoolResourceAffinitiy) {

	std::string result;
	{
		signal(SIGINT).then([](int s){theLoop().exit();});

		reproredis::RedisPool Redis("redis://localhost:6379/",2);

		int c = 0;

		for(int i = 0; i < 4; i++)
		{
			timeout([&c,&result,&Redis]()
			{
				Redis
				->cmd("SET", "promise-test", "promised")
				.then( [&Redis](reproredis::Reply r)
				{
					std::cout << "did set" << std::endl;
					return
						Redis// r() // will hang on win32
						->cmd("GET", "promise-test");
				})
				.then( [&c,&result](reproredis::Reply r)
				{
					std::cout << "did get" << std::endl;
					result = r.asString();
					c++;
					if(c>3) {
						theLoop().exit();
					}
				})
				.otherwise( [](const std::exception& ex)
				{
					std::cout << "Ex: " << ex.what() << std::endl;
				});
			},0,300);
		}
		theLoop().run();
	}

	EXPECT_EQ("promised",result);
	MOL_TEST_ASSERT_CNTS(0,0);
}


TEST_F(BasicTest, PublishSubscribeTest) 
{
	ThreadPool redisThreadPool(1);

	std::string result;
	{
		signal(SIGINT).then([](int s) { theLoop().exit(); });

		reproredis::RedisPool Redis("redis://localhost:6379/", 4);

		
		reproredis::RedisPtr rPtr = reproredis::Redis::create(Redis, redisThreadPool);
		rPtr->subscribe("test-topic")
		.then( [&result,&rPtr](reproredis::Reply r) 
		{
			std::cout << "REDIS SUBSCRIPTION NOTIFY" << std::endl;
			if (r.size() != 3) return;
			if (!r.isArray()) return;

			result += r.element(2).asString();

			std::cout << result << std::endl;

			if (result == "promised")
			{
				// we're done. gently tear down subscription
				// and exit test after a short wait
				// otherwise we would leak some promises
				rPtr->dispose();
				timeout([]()
				{
					theLoop().exit();
				},0,100);
			}
		})
		.otherwise([](const std::exception& ex)
		{
			std::cout << "Ex: " << ex.what() << std::endl;
		});
		
		timeout( [&Redis]()
		{
			std::cout << "timeout" << std::endl;
			Redis->cmd("publish", "test-topic", "promi")
			.then([ &Redis](reproredis::Reply r)
			{
				std::cout << "did publish" << std::endl;
				return Redis->cmd("publish", "test-topic", "sed");
			})
			.then([&Redis](reproredis::Reply r)
			{
				std::cout << "did publish" << std::endl;
			})
			.otherwise([](const std::exception& ex)
			{
				std::cout << "Ex: " << ex.what() << std::endl;
			});

		}, 0, 500);

		std::cout << "loop start" << std::endl;
		redisThreadPool.start();
		theLoop().run();
		redisThreadPool.stop();

		std::cout << "loop end" << std::endl;
	}

	EXPECT_EQ("promised", result);
	MOL_TEST_ASSERT_CNTS(0, 0);
}

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
		reproredis::Reply r = co_await redis->cmd("SET", "promise-test", "promised");

		std::cout << "did set" << std::endl;

		reproredis::Reply r2 = co_await r()->cmd("GET", "promise-test");

		result = r2.asString();
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

int main(int argc, char **argv) {

	prio::init();

    ::testing::InitGoogleTest(&argc, argv);
    int r = RUN_ALL_TESTS();

    return r;
}
