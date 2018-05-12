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

class RedisResult;

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
	typedef repro::Future<std::shared_ptr<RedisResult>> FutureType;
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
	repro::Future<std::shared_ptr<RedisResult>> cmd( Args ... args);

    //FutureType subscribe( const std::string& topic );

	void shutdown()
	{
		pool_.shutdown();
	}


private:

	std::string url_;
	Pool pool_;
};


class RedisResult : public std::enable_shared_from_this<RedisResult>
{
public:

	typedef std::shared_ptr<RedisResult> Ptr;

	MyRedisPool::ResourcePtr con;

	virtual ~RedisResult() {}
	virtual bool isNill()     						{ return false; }
	virtual bool isError()    						{ return false; }
	virtual std::string str() 						{ return ""; }
	virtual long integer()    						{ return 0; }
	virtual long size()   							{ return 0; }
	virtual RedisResult::Ptr element(std::size_t i) { return nullptr; }
	virtual Future<RedisResult::Ptr> parse()		{}
	template<class ... Args>
	repro::Future<RedisResult::Ptr> cmd(Args ... args);

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

	virtual Future<RedisResult::Ptr> parse()
	{
		if(size_==-1)
		{
			nil_ = true;
			return prio::resolved(shared_from_this());
		}

		auto p = repro::promise<RedisResult::Ptr>();

		parse_response(p);

		return p.future();
	}

private:

	bool nil_ = false;
	long size_ = 0;
	std::string str_;
	RedisParser& parser_;
		
	void parse_response(repro::Promise<RedisResult::Ptr> p);
	void read(repro::Promise<RedisResult::Ptr> p);
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

	virtual Future<RedisResult::Ptr> parse()
	{
		auto p = repro::promise<RedisResult::Ptr>();

		nextTick().then([this,p]()
		{
			p.resolve(shared_from_this());
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

	virtual Future<RedisResult::Ptr> parse()
	{
		auto p = repro::promise<RedisResult::Ptr>();

		nextTick().then([this,p]()
		{
			p.resolve(shared_from_this());
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
	
	virtual Future<RedisResult::Ptr> parse()
	{
		auto p = repro::promise<RedisResult::Ptr>();

		nextTick().then([this,p]()
		{
			p.resolve(shared_from_this());
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
		:parser_(p),size_(s),p_(repro::promise<RedisResult::Ptr>() )
	{
		std::cout << "array size " << s << std::endl;
	}
	
	virtual bool isNill()     						 { return nil_; }
	virtual long size()   							 { return size_; }
	virtual RedisResult::Ptr element(std::size_t i)  { return elements_[i]; }

	virtual Future<RedisResult::Ptr> parse()
	{
		if(size_==-1)
		{
			nil_ = true;
			return prio::resolved(shared_from_this());
		}

		read()
		.then([this](RedisResult::Ptr r)
		{
			if(size_ == elements_.size())
			{
				std::cout << size_ << " <-> " << elements_.size() << std::endl;
				p_.resolve(shared_from_this());
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

	void clear()
	{
		//size_ = 0;
		//elements_.clear();
	}

private:

	RedisResult::Ptr resultFactory(const std::string& cmd);

	bool nil_ = false;
	long size_ = 0;
	RedisParser& parser_;
	std::vector<RedisResult::Ptr> elements_;
	repro::Promise<RedisResult::Ptr> p_;

	Future<RedisResult::Ptr> read();
	void parse_response(std::string cmd,repro::Promise<RedisResult::Ptr> p);		
};

class RedisParser
{
public:

	MyRedisPool::ResourcePtr con;
	std::string buffer;
	size_t pos = 0;


	RedisParser()
		: p_ (repro::promise<RedisResult::Ptr>())
	{
		result_ = std::make_shared<RedisArrayResult>(*this,1);		
	}

	Future<RedisResult::Ptr> parse()
	{
		RedisArrayResult* rar = (RedisArrayResult*)result_.get();
		rar->parse()
		.then([this,rar](RedisResult::Ptr r)
		{
			RedisResult::Ptr res = r->element(0);
			std::cout << "----> " << res->size() << std::endl;
			//rar->clear();
			res->con = con;
			auto tmp = p_;
			delete this;
			tmp.resolve(res);
		})		
		.otherwise([this](const std::exception& ex)
		{
			std::cout << ex.what() << std::endl;
			auto tmp = p_;
			delete this;			
			tmp.reject(ex);
		});	

		return p_.future();
	}

	void consume(size_t n)
	{
		pos += n;
	}

private:

	repro::Promise<RedisResult::Ptr> p_;
	RedisArrayResult::Ptr result_;	
};

void RedisBulkStringResult::parse_response(repro::Promise<RedisResult::Ptr> p)
{
	int s = parser_.buffer.size() - parser_.pos;

	if( s >= size_ + 2)
	{
		str_ = parser_.buffer.substr(parser_.pos, size_);
		parser_.consume(size_ + 2);
		nextTick().then([this,p]()
		{
			p.resolve(shared_from_this());
		});
		return;
	}
	read(p);
}

void RedisBulkStringResult::read(repro::Promise<RedisResult::Ptr> p)
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

RedisResult::Ptr RedisArrayResult::resultFactory(const std::string& cmd)
{
	RedisResult::Ptr r;
	switch(cmd[0])
	{
		case '-' : // error
		{
			r = std::make_shared<RedisErrorResult>(parser_,cmd.substr(1));
			break;
		}
		case '+' : // simple string
		{
			r = std::make_shared<RedisSimpleStringResult>(parser_,cmd.substr(1));
			break;
		}
		case ':' : // simple integer
		{
			r = std::make_shared<RedisIntegerResult>(parser_,cmd.substr(1));
			break;
		}			
		case '$' : // bulk string
		{
			std::istringstream iss(cmd.substr(1));
			long size;
			iss >> size;

			r = std::make_shared<RedisBulkStringResult>(parser_,size);
			break;
		}
		case '*' : // array 
		{
			std::istringstream iss(cmd.substr(1));
			long size;
			iss >> size;

			r = std::make_shared<RedisArrayResult>(parser_,size);
			break;
		}			
	}
	elements_.push_back(r);
	return r;
}


void RedisArrayResult::parse_response( std::string cmd, repro::Promise<RedisResult::Ptr> p)
{
	std::cout << "parse: " << cmd  << std::endl;
	RedisResult::Ptr r = resultFactory(cmd);
	r->parse()
	.then([p](RedisResult::Ptr r)
	{
		std::cout << "got error! " << r->str() << "/" << r->size() <<  std::endl;
		p.resolve(r);					
	})
	.otherwise([p](const std::exception& ex)
	{
		p.reject(ex);
	});
/*
	switch(cmd[0])
	{
		case '-' : 
		{
			// error
			auto r = std::make_shared<RedisErrorResult>(parser_,cmd.substr(1));
			elements_.push_back(r);
			r->parse()
			.then([p](RedisResult::Ptr r)
			{
				std::cout << "got error! " << r->str() << std::endl;
				p.resolve(r);					
			});
			break;
		}
		case '+' : 
		{
			// simple string
			auto r = std::make_shared<RedisSimpleStringResult>(parser_,cmd.substr(1));
			elements_.push_back(r);
			r->parse()
			.then([p](RedisResult::Ptr r)
			{
				std::cout << "got sstring! " << r->str() << std::endl;
				p.resolve(r);					
			});
			break;
		}
		case ':' : 
		{
			// simple integer
			auto r = std::make_shared<RedisIntegerResult>(parser_,cmd.substr(1));
			elements_.push_back(r);
			r->parse()
			.then([p](RedisResult::Ptr r)
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

			auto r = std::make_shared<RedisBulkStringResult>(parser_,size);
			elements_.push_back(r);
			
			r->parse()
			.then([p](RedisResult::Ptr r)
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

			auto r = std::make_shared<RedisArrayResult>(parser_,size);
			elements_.push_back(r);
			
			r->parse()
			.then([p](RedisResult::Ptr r)
			{
				std::cout << "got array! " << r->size() << std::endl;
				p.resolve(r);					
			});
			break;
		}			
	}
	*/
}


repro::Future<RedisResult::Ptr> RedisArrayResult::read()
{
	auto p = repro::promise<RedisResult::Ptr>();

	std::size_t pos = parser_.buffer.find("\r\n",parser_.pos);
	if ( pos != std::string::npos )
	{
		std::string tmp = parser_.buffer.substr(parser_.pos,pos-parser_.pos);
		parser_.consume(pos-parser_.pos+2);
		parse_response(tmp,p );
		return p.future();
	}
	
	(*(parser_.con))->con->read()
	.then([this,p](prio::Connection::Ptr con, std::string data)
	{
		parser_.buffer.append(data);
		read()
		.then([this,p](RedisResult::Ptr r)
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
repro::Future<RedisResult::Ptr> MyRedisPool::cmd( Args ... args)
{
	auto p =  repro::promise<RedisResult::Ptr>();

	Serializer serializer;
	std::string cmd = serializer.serialize(args...);
	std::cout << "CMD:" << cmd << std::endl;

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
	.then([p](RedisResult::Ptr r)
	{
		p.resolve(r);
	})	
	.otherwise([p](const std::exception& ex)
	{
		p.reject(ex);
	});

    return p.future();
}



template<class ... Args>
repro::Future<RedisResult::Ptr> RedisResult::cmd( Args ... args)
{
	auto p =  repro::promise<RedisResult::Ptr>();

	Serializer serializer;
	std::string cmd = serializer.serialize(args...);

	std::cout << "CMD:" << cmd << std::endl;

	RedisParser* parser = new RedisParser();

	parser->con = con;
	(*(con))->con->write(cmd)
	.then([parser](prio::Connection::Ptr con)
	{
		return parser->parse();
	})
	.then([p](RedisResult::Ptr r)
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
}


TEST_F(BasicTest, RawRedisChained) 
{
	std::string result;
	{
		signal(SIGINT).then([](int s) {theLoop().exit(); });

		MyRedisPool redis("redis://localhost:6379");

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
