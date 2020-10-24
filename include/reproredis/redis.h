#ifndef _MOL_DEF_GUARD_DEFINE_MOD_HTTP_REDIS_DEF_GUARD_
#define _MOL_DEF_GUARD_DEFINE_MOD_HTTP_REDIS_DEF_GUARD_

#include "priocpp/common.h"
#include "priocpp/api.h"
#include "priocpp/res.h"
#include <set>

//////////////////////////////////////////////////////////////


namespace reproredis   {


class RedisResult;
class RedisParser;
class RedisArrayResult;

class RedisConnection
{
public:

	RedisConnection()
	{
		REPRO_MONITOR_INCR(RedisConnection);	
	}

	~RedisConnection()
	{
		REPRO_MONITOR_DECR(RedisConnection);	
	}

	prio::Connection::Ptr con;

	static repro::Future<RedisConnection*> connect(const std::string& url);

private:
	RedisConnection(const RedisConnection&) = delete;
	RedisConnection& operator=(const RedisConnection&) = delete;
};


struct RedisLocator
{
	typedef RedisConnection* type;

	static repro::Future<type> retrieve(const std::string& url);

	static void free(type t);
};

class RedisPool
{
public:
	typedef repro::Future<std::shared_ptr<RedisResult>> FutureType;
	typedef prio::Resource::Pool<RedisLocator> Pool;
	typedef Pool::ResourcePtr ResourcePtr;

	RedisPool(const std::string& url, int capacity = 4);
	RedisPool();
	~RedisPool();

	repro::Future<ResourcePtr> get();

	repro::Future<ResourcePtr> get_new();

    template<class ... Args>
	FutureType cmd( Args ... args);

	void shutdown();

private:

	FutureType do_cmd(const std::string& cmd);

	std::string url_;
	Pool pool_;
};


class RedisSubscriber
{
public:

	RedisSubscriber(RedisPool& p);
	~RedisSubscriber();

	prio::Callback<std::pair<std::string,std::string>>& subscribe(const std::string& topic);

	void unsubscribe();

private:
	RedisPool& pool_;
	prio::Callback<std::pair<std::string,std::string>> cb_;
	std::shared_ptr<RedisParser> parser_;
	bool shutdown_ = false;
};

class RedisResult : public std::enable_shared_from_this<RedisResult>
{
public:

	typedef std::shared_ptr<RedisResult> Ptr;

	RedisPool::ResourcePtr con;
	
	RedisResult()
	{
		REPRO_MONITOR_INCR(RedisResult);	
	}

	RedisResult(const RedisResult& rhs)
		: con(rhs.con)
	{
		REPRO_MONITOR_INCR(RedisResult);	
	}

	virtual ~RedisResult() 
	{
		REPRO_MONITOR_DECR(RedisResult);
	}
	
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

private:
	RedisResult& operator=(const RedisResult&) = delete;
	RedisResult& operator=(RedisResult&&) = delete;
	
	repro::Future<RedisResult::Ptr> do_cmd(std::string cmd);	
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
	Serializer serializer;
	std::string cmd = serializer.serialize(args...);

	return do_cmd(cmd);
}

template<class ... Args>
repro::Future<RedisResult::Ptr> RedisResult::cmd( Args ... args)
{
	Serializer serializer;
	std::string cmd = serializer.serialize(args...);

	return do_cmd(cmd);
}



} // close namespaces

#endif

