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

/*
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
*/

#include <stack>


class RedisResult 
{
public:
	virtual ~RedisResult() {}
	virtual bool isNill()     					{ return false; }
	virtual bool isError()    					{ return false; }
	virtual std::string str() 					{ return ""; }
	virtual long integer()    					{ return 0; }
	virtual long size()   						{ return 0; }
	virtual RedisResult* element(std::size_t i) { return nullptr; }
};

class RedisParser;

class RedisConnection
{
public:

	prio::Connection::Ptr con;

	static Future<RedisConnection*> connect(const std::string& url)
	{
		auto p = repro::promise<RedisConnection*>();

		prio::Url parsed_url(url);
		prio::Connection::connect(parsed_url.getHost(), parsed_url.getPort())
		.then([p](prio::Connection::Ptr con)
		{
			RedisConnection* rc = new RedisConnection();
			rc->con = con;
			p.resolve(rc);
		})
		.otherwise([p](const std::exception& ex)
		{
			std::cout << ex.what() << std::endl;
			p.reject(ex);
		});
		return p.future();
	}
};

struct MyRedisLocator
{
	typedef RedisConnection type;

	static repro::Future<type*> retrieve(const std::string& url)
	{
		return RedisConnection::connect(url);
	}

	static void free( type* t)
	{
		t->con->close();
		delete t;
	}
};

class MyRedisPool
{
public:
	typedef repro::Future<RedisResult*> FutureType;
	typedef prio::ResourcePool<MyRedisLocator> Pool;
	typedef Pool::ResourcePtr ResourcePtr;

	MyRedisPool(const std::string& url, int capacity = 4)
		: url_(url), pool_(capacity)
	{}

	MyRedisPool() {}

	~MyRedisPool() {}

	repro::Future<ResourcePtr> get();
	//repro::Future<ResourcePtr> operator()();

    template<class ... Args>
	repro::Future<RedisResult*> cmd(const Args& ... args);

    //FutureType subscribe( const std::string& topic );

	void shutdown()
	{
		pool_.shutdown();
	}


private:

	std::string url_;
	Pool pool_;
};


class RedisBulkStringResult : public RedisResult
{
public:


	RedisBulkStringResult(RedisParser& p, long s)
		: size_(s), parser_(p)
	{}
	
	virtual bool isNill()     		{ return nil_; }
	virtual std::string str() 		{ return str_; }
	virtual long size()   			{ return size_; }
	virtual long integer()    		{ std::istringstream iss(str_); long r; iss >> r; return r; }

	Future<RedisResult*> parse()
	{
		if(size_==-1)
		{
			nil_ = true;
			return prio::resolved((RedisResult*)this);
		}

		auto p = repro::promise<RedisResult*>();

		parse_response(p);

		return p.future();
	}

private:

	bool nil_ = false;
	long size_ = 0;
	std::string str_;
	RedisParser& parser_;
		
	void parse_response(repro::Promise<RedisResult*> p);
	void read(repro::Promise<RedisResult*> p);
};

class RedisSimpleStringResult : public RedisResult
{
public:

	RedisSimpleStringResult(RedisParser& p, std::string s)
		: str_(s), parser_(p)
	{}
	
	virtual std::string str() 		{ return str_; }
	virtual long size()   			{ return str_.size(); }
	virtual long integer()    		{ std::istringstream iss(str_); long r; iss >> r; return r; }

	Future<RedisResult*> parse()
	{
		auto p = repro::promise<RedisResult*>();

		nextTick().then([this,p]()
		{
			p.resolve((RedisResult*)this);
		});

		return p.future();
	}

private:

	std::string str_;	
	RedisParser& parser_;
};


class RedisErrorResult : public RedisResult
{
public:

	RedisErrorResult(RedisParser& p, std::string s)
		: str_(s), parser_(p)
	{}

	virtual std::string str() 		{ return str_; }
	virtual long size()   			{ return str_.size(); }
	virtual long integer()    		{ std::istringstream iss(str_); long r; iss >> r; return r; }

	Future<RedisResult*> parse()
	{
		auto p = repro::promise<RedisResult*>();

		nextTick().then([this,p]()
		{
			p.resolve((RedisResult*)this);
		});

		return p.future();
	}

private:	
	std::string str_;
	RedisParser& parser_;
};

class RedisIntegerResult : public RedisResult
{
public:

	RedisIntegerResult(RedisParser& p, std::string s)
		: str_(s), parser_(p)
	{}
	
	virtual std::string str() 		{ return str_; }
	virtual long size()   			{ return str_.size(); }
	virtual long integer()    		{ std::istringstream iss(str_); long r; iss >> r; return r; }
	
	Future<RedisResult*> parse()
	{
		auto p = repro::promise<RedisResult*>();

		nextTick().then([this,p]()
		{
			p.resolve((RedisResult*)this);
		});

		return p.future();
	}
		
private:		
	std::string str_;
	RedisParser& parser_;
};


class RedisArrayResult : public RedisResult
{
public:

	RedisArrayResult(RedisParser& p, long s)
		:parser_(p),size_(s),p_(repro::promise<RedisResult*>() )
	{
		std::cout << "RedisArrayResult("<<s<<")" << std::endl;
	}
	
	virtual bool isNill()     		{ return nil_; }
	virtual long size()   			{ return size_; }
	virtual RedisResult* element(std::size_t i)  { return elements_[i]; }

	Future<RedisResult*> parse()
	{
		if(size_==-1)
		{
			nil_ = true;
			return prio::resolved((RedisResult*)this);
		}

		read()
		.then([this](RedisResult* r)
		{
			elements_.push_back(r);
			std::cout << size_ << " <?> " << elements_.size() << std::endl;
			if(size_ == elements_.size())
			{
				p_.resolve(this);
				std::cout << "!!!!!!ccc!!!!!!" << std::endl;
				return;
			}
			parse();
		})		
		.otherwise([this](const std::exception& ex)
		{
			std::cout << ex.what() << std::endl;
			p_.reject(ex);
		});	
		return p_.future();
	}

private:

	bool nil_ = false;
	long size_ = 0;
	RedisParser& parser_;
	std::vector<RedisResult*> elements_;
	repro::Promise<RedisResult*> p_;

	Future<RedisResult*> read();
	void parse_response(std::string cmd,repro::Promise<RedisResult*> p);		
};

class RedisParser
{
public:

	MyRedisPool::ResourcePtr con;
	std::string buffer;
	size_t pos = 0;


	RedisParser()
		: p_ (repro::promise<RedisResult*>())
	{
		result_ = new RedisArrayResult(*this,1);		
	}

	Future<RedisResult*> parse()
	{
		result_->parse()
		.then([this](RedisResult* r)
		{
			std::cout << "#########################" << std::endl;
			p_.resolve(r->element(0));
		})		
		.otherwise([this](const std::exception& ex)
		{
			std::cout << ex.what() << std::endl;
			p_.reject(ex);
		});	

		return p_.future();
	}

	void consume(size_t n)
	{
		pos += n;
	}

private:

	repro::Promise<RedisResult*> p_;
	RedisArrayResult* result_;	
};

void RedisBulkStringResult::parse_response(repro::Promise<RedisResult*> p)
{
	int s = parser_.buffer.size() - parser_.pos;

	if( s >= size_ + 2)
	{
		str_ = parser_.buffer.substr(parser_.pos, size_);
		parser_.consume(size_ + 2);
		nextTick().then([this,p]()
		{
			p.resolve((RedisResult*)this);
		});
		return;
	}
	read(p);
}

void RedisBulkStringResult::read(repro::Promise<RedisResult*> p)
{
	(*(parser_.con))->con->read()
	.then([this,p](prio::Connection::Ptr con, std::string data)
	{
		//std::cout << datRedisBulkStringResult:endl;
		parser_.buffer.append(data);
		parse_response(p);
	})		
	.otherwise([p](const std::exception& ex)
	{
		std::cout << ex.what() << std::endl;
		p.reject(ex);
	});			
}

void RedisArrayResult::parse_response( std::string cmd, repro::Promise<RedisResult*> p)
{
	std::cout << "RedisArrayResult::parse_response " << cmd << std::endl;

	switch(cmd[0])
	{
		case '-' : 
		{
			// error
			RedisErrorResult* r = new RedisErrorResult(parser_,cmd.substr(1));
			r->parse()
			.then([p](RedisResult* r)
			{
				std::cout << "got error! " << r->str() << std::endl;
				p.resolve(r);					
			});
			break;
		}
		case '+' : 
		{
			// simple string
			RedisSimpleStringResult* r = new RedisSimpleStringResult(parser_,cmd.substr(1));
			r->parse()
			.then([p](RedisResult* r)
			{
				std::cout << "got sstring! " << r->str() << std::endl;
				p.resolve(r);					
			});
			break;
		}
		case ':' : 
		{
			// simple integer
			RedisIntegerResult* r = new RedisIntegerResult(parser_,cmd.substr(1));
			r->parse()
			.then([p](RedisResult* r)
			{
				std::cout << "got int! " << r->integer() << std::endl;
				p.resolve(r);					
			});
			break;
		}			
		case '$' :
		{
			// bulk string
			std::istringstream iss(cmd.substr(1));
			long size;
			iss >> size;

			std::cout << "size:" << size << std::endl;

			RedisBulkStringResult* r = new RedisBulkStringResult(parser_,size);
			r->parse()
			.then([p](RedisResult* r)
			{
				std::cout << "got bstring! " << r->str() << std::endl;
				p.resolve(r);					
			});
			break;
		}
		case '*' :
		{
			// array 
			std::istringstream iss(cmd.substr(1));
			long size;
			iss >> size;

			std::cout << "array size:" << size << std::endl;

			RedisArrayResult* r = new RedisArrayResult(parser_,size);
			r->parse()
			.then([p](RedisResult* r)
			{
				std::cout << "got array! " << r->size() << std::endl;
				p.resolve(r);					
			});
			break;
		}			
	}
}


repro::Future<RedisResult*> RedisArrayResult::read()
{
	auto p = repro::promise<RedisResult*>();

	std::cout << "RedisArrayResult::read() " << parser_.pos << std::endl;

	std::size_t pos = parser_.buffer.find("\r\n",parser_.pos);
	if ( pos != std::string::npos )
	{
		std::string tmp = parser_.buffer.substr(parser_.pos,pos-parser_.pos);
		std::cout << ">>>>>>>>>>< " << pos << ":" << tmp << std::endl << "---" << std::endl;
		parser_.consume(pos-parser_.pos+2);
		parse_response(tmp,p );
		return p.future();
	}
	
	(*(parser_.con))->con->read()
	.then([this,p](prio::Connection::Ptr con, std::string data)
	{
		parser_.buffer.append(data);
		read()
		.then([this,p](RedisResult* r)
		{
			p.resolve(r);
		})
		.otherwise([p](const std::exception& ex)
		{
			std::cout << ex.what() << std::endl;
			p.reject(ex);
		});			
	})		
	.otherwise([p](const std::exception& ex)
	{
		std::cout << ex.what() << std::endl;
		p.reject(ex);
	});			

	return p.future();
}

class Serializer
{
public:

	template<class ... Args>
	std::string serialize(Args ... args )
	{
		const int n = sizeof...(Args);
		oss_ << "*" << n << "\r\n";
		do_serialize(args...);
		std::string r = oss_.str();

		oss_.str("");
		oss_.clear();

		return r;
	}

private:

	template<class T, class ... Args>
	void do_serialize(T t,Args ... args )
	{
		std::ostringstream tmp_oss_;
		tmp_oss_ << t;
		std::string tmp_str = tmp_oss_.str();

		oss_ << "$" << tmp_str.size() << "\r\n" << tmp_str << "\r\n";
		do_serialize(args...);
	}

	void do_serialize( )
	{
	}

	std::ostringstream oss_;
};


template<class ... Args>
repro::Future<RedisResult*> MyRedisPool::cmd(const Args& ... args)
{
	auto p =  repro::promise<RedisResult*>();

	Serializer serializer;
	std::string cmd = serializer.serialize(args...);

	RedisParser* parser = new RedisParser();

	get()
	.then([p,cmd,parser](MyRedisPool::ResourcePtr redis)
	{
		parser->con = redis;
		return (*(redis))->con->write(cmd);
	})
	.then([parser](prio::Connection::Ptr con)
	{
		return parser->parse();
	})
	.then([p](RedisResult* r)
	{
		p.resolve(r);
	})	
	.otherwise([p](const std::exception& ex)
	{
		p.reject(ex);
	});

    return p.future();
}


Future<MyRedisPool::ResourcePtr> MyRedisPool::get()
{
	auto p = repro::promise<ResourcePtr>();
	pool_.get(url_)
	.then( [p](ResourcePtr r)
	{
		p.resolve(r);
	});
	return p.future();
}

TEST_F(BasicTest, RawRedis) 
{
	std::string result;
	{
		signal(SIGINT).then([](int s) {theLoop().exit(); });

		MyRedisPool redis("redis://localhost:6379");

		redis.cmd("LRANGE", "mylist", 0, -1)
		.then([](RedisResult* r)
		{
			std::cout << "DONE" << std::endl;
			theLoop().exit();
		})		
		.otherwise([](const std::exception& ex)
		{
			std::cout << ex.what() << std::endl;
			theLoop().exit();
		});

		theLoop().run();
	}
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
