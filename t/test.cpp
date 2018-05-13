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

/*

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

/*

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

    template<class ... Args>
	FutureType cmd( Args ... args);

    FutureType subscribe( const std::string& topic );

	void shutdown()
	{
		pool_.shutdown();
	}

private:

	std::string url_;
	Pool pool_;
};


class RedisSubscriber
{
public:

	MyRedisPool& pool_;
	MyRedisPool::ResourcePtr res_;
	repro::Promise<std::pair<std::string,std::string>> p_;
	std::shared_ptr<RedisParser> parser_;

	RedisSubscriber(MyRedisPool& p);
	~RedisSubscriber();

	repro::Future<std::pair<std::string,std::string>> subscribe(const std::string& topic);

	void unsubscribe();
};

class RedisResult : public std::enable_shared_from_this<RedisResult>
{
public:

	typedef std::shared_ptr<RedisResult> Ptr;

	MyRedisPool::ResourcePtr con;

	virtual ~RedisResult() {}
	virtual bool isNill()     						{ return false; }
	virtual bool isError()    						{ return false; }
	virtual bool isArray()    						{ return false; }
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
	}
	
	virtual bool isArray()    						 { return true; }
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
		size_ = 1;
		elements_.clear();
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

	~RedisParser()
	{
	}

	Future<RedisResult::Ptr> parse()
	{
		RedisArrayResult* rar = (RedisArrayResult*)result_.get();
		rar->parse()
		.then([this,rar](RedisResult::Ptr r)
		{
			RedisResult::Ptr res = r->element(0);
			res->con = con;
			auto tmp = p_;
			tmp.resolve(res);
		})		
		.otherwise([this](const std::exception& ex)
		{
			con->markAsInvalid();
			std::cout << ex.what() << std::endl;
			auto tmp = p_;
			tmp.reject(ex);
		});	

		return p_.future();
	}

	void listen( repro::Promise<std::pair<std::string,std::string>> p);


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
	RedisResult::Ptr r = resultFactory(cmd);
	r->parse()
	.then([p](RedisResult::Ptr r)
	{
		p.resolve(r);					
	})
	.otherwise([p](const std::exception& ex)
	{
		p.reject(ex);
	});
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
	.then([p,parser](RedisResult::Ptr r)
	{
		p.resolve(r);
		delete parser;
	})	
	.otherwise([p,parser](const std::exception& ex)
	{
		delete parser;
		p.reject(ex);
	});

    return p.future();
}

RedisSubscriber::RedisSubscriber(MyRedisPool& p)
	: pool_(p)
{
	p_ = repro::promise<std::pair<std::string,std::string>>();
	parser_ = std::make_shared<RedisParser>();
}

RedisSubscriber::~RedisSubscriber()
{
}

void RedisSubscriber::unsubscribe()
{
	(*(parser_->con))->con->close();
}

repro::Future<std::pair<std::string,std::string>> RedisSubscriber::subscribe(const std::string& topic)
{
	Serializer serializer;
	std::string cmd = serializer.serialize("subscribe", topic);

	RedisParser* parser = new RedisParser();

	pool_.get()
	.then([this,cmd,parser](MyRedisPool::ResourcePtr redis)
	{
		parser->con = redis;
		return (*(redis))->con->write(cmd);
	})
	.then([this,parser](prio::Connection::Ptr con)
	{				
		parser_->con = parser->con;
		return parser->parse();
	})		
	.then([this,parser](RedisResult::Ptr r)
	{				
		
		if ( r->isError() || r->isNill() || !r->isArray() || r->element(0)->str() != "subscribe" )
		{
			//delete parser;
			throw repro::Ex("redis subscribe failed");
		}
		
		parser_->listen(p_);
		//RedisParser* p = parser;
		//parser = nullptr;
		delete parser;
	})	
	.otherwise([this,parser](const std::exception& ex)
	{
		std::cout << ex.what() << std::endl;
		delete parser;
		p_.reject(ex);
	});		

	return p_.future();
}


void RedisParser::listen(repro::Promise<std::pair<std::string,std::string>> p )
{
	RedisArrayResult* rar = (RedisArrayResult*)result_.get();
	rar->clear();
	rar->parse()
	.then([this,p,rar](RedisResult::Ptr r)
	{
		RedisResult::Ptr res = r->element(0);

		if( res->isError() || res->isNill() || !res->isArray() || res->size() < 3 || res->element(0)->str() != "message")
		{
			throw repro::Ex("invalid redis channel reply");
		}

		std::string channel = res->element(1)->str();
		std::string msg     = res->element(2)->str();

		p.resolve(std::make_pair(channel,msg));
		listen(p);
	})		
	.otherwise([p,this](const std::exception& ex)
	{
		con->markAsInvalid();
		std::cout << ex.what() << std::endl;
		p.reject(ex);
	});	
}


template<class ... Args>
repro::Future<RedisResult::Ptr> RedisResult::cmd( Args ... args)
{
	auto p =  repro::promise<RedisResult::Ptr>();

	Serializer serializer;
	std::string cmd = serializer.serialize(args...);

	RedisParser* parser = new RedisParser();

	parser->con = con;
	(*(con))->con->write(cmd)
	.then([parser](prio::Connection::Ptr con)
	{
		return parser->parse();
	})
	.then([p,parser](RedisResult::Ptr r)
	{
		p.resolve(r);
		delete parser;
	})	
	.otherwise([p,parser](const std::exception& ex)
	{
		delete parser;
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
*/
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
