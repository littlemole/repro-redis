#ifndef _MOL_DEF_GUARD_DEFINE_MOD_HTTP_REDIS_DEF_GUARD_
#define _MOL_DEF_GUARD_DEFINE_MOD_HTTP_REDIS_DEF_GUARD_

#include "priocpp/common.h"
#include "priocpp/api.h"
#include "priocpp/ResourcePool.h"
#include <set>

//////////////////////////////////////////////////////////////


namespace reproredis   {


class RedisResult;
class RedisParser;
class RedisArrayResult;

class RedisConnection
{
public:

	prio::Connection::Ptr con;

	static repro::Future<RedisConnection*> connect(const std::string& url);
};


struct RedisLocator
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

class RedisPool
{
public:
	typedef repro::Future<std::shared_ptr<RedisResult>> FutureType;
	typedef prio::ResourcePool<RedisLocator> Pool;
	typedef Pool::ResourcePtr ResourcePtr;

	RedisPool(const std::string& url, int capacity = 4);
	RedisPool();
	~RedisPool();

	repro::Future<ResourcePtr> get();

    template<class ... Args>
	FutureType cmd( Args ... args);

	void shutdown();

private:

	std::string url_;
	Pool pool_;
};


class RedisSubscriber
{
public:

	RedisPool& pool_;
	RedisPool::ResourcePtr res_;
	repro::Promise<std::pair<std::string,std::string>> p_;
	std::shared_ptr<RedisParser> parser_;

	RedisSubscriber(RedisPool& p);
	~RedisSubscriber();

	repro::Future<std::pair<std::string,std::string>> subscribe(const std::string& topic);

	void unsubscribe();
};

class RedisResult : public std::enable_shared_from_this<RedisResult>
{
public:

	typedef std::shared_ptr<RedisResult> Ptr;

	RedisPool::ResourcePtr con;

	virtual ~RedisResult() {}
	virtual bool isNill()     						{ return false; }
	virtual bool isError()    						{ return false; }
	virtual bool isArray()    						{ return false; }
	virtual std::string str() 						{ return ""; }
	virtual long integer()    						{ return 0; }
	virtual long size()   							{ return 0; }
	virtual RedisResult::Ptr element(std::size_t i) { return nullptr; }
	virtual repro::Future<RedisResult::Ptr> parse()	= 0;

	template<class ... Args>
	repro::Future<RedisResult::Ptr> cmd(Args ... args);
};


class RedisParser
{
public:

	RedisPool::ResourcePtr con;
	std::string buffer;
	size_t pos = 0;

	RedisParser();
	~RedisParser();

	repro::Future<RedisResult::Ptr> parse();
	void listen( repro::Promise<std::pair<std::string,std::string>> p);
	void consume(size_t n);

private:

	repro::Promise<RedisResult::Ptr> p_;
	std::shared_ptr<RedisArrayResult> result_;	
};


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
	{}

	std::ostringstream oss_;
};


template<class ... Args>
repro::Future<RedisResult::Ptr> RedisPool::cmd( Args ... args)
{
	auto p =  repro::promise<RedisResult::Ptr>();

	Serializer serializer;
	std::string cmd = serializer.serialize(args...);

	RedisParser* parser = new RedisParser();

	get()
	.then([p,cmd,parser](RedisPool::ResourcePtr redis)
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



} // close namespaces

#endif

