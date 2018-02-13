#ifndef _MOL_DEF_GUARD_DEFINE_MOD_HTTP_REDIS_DEF_GUARD_
#define _MOL_DEF_GUARD_DEFINE_MOD_HTTP_REDIS_DEF_GUARD_

#include "priocpp/common.h"
#include "priocpp/api.h"
#include "priocpp/ResourcePool.h"
#include <set>
#include <hiredis/hiredis.h>

//////////////////////////////////////////////////////////////


namespace reproredis   {

template<class T>
void populate_args(std::vector<std::string>& v, const T& t)
{
    std::ostringstream oss;
    oss << t;
    v.push_back(oss.str());
}

template<class T, class ... Args>
void populate_args(std::vector<std::string>& v, const T& t, Args ... args )
{
    populate_args(v,t);
    populate_args(v,args...);
}

//////////////////////////////////////////////////////////////

class Redis;
typedef std::shared_ptr<Redis> RedisPtr;

//////////////////////////////////////////////////////////////

struct RedisLocator
{
	typedef redisContext type;

	static repro::Future<type*> retrieve(const std::string& url);
	static void free( type* t);
};

//////////////////////////////////////////////////////////////

class Reply;

class RedisPool
{
public:
	typedef repro::Future<Reply> FutureType;
	typedef prio::ResourcePool<RedisLocator> Pool;
	typedef Pool::ResourcePtr ResourcePtr;

	RedisPool(const std::string& url, int capacity = 4)
		: url_(url), pool_(capacity)
	{}

	RedisPool() {}

	~RedisPool() {}

	repro::Future<ResourcePtr> get();
	repro::Future<ResourcePtr> operator()();

    template<class ... Args>
	repro::Future<Reply> cmd(const Args& ... args);

    FutureType subscribe( const std::string& topic );

	void shutdown()
	{
		pool_.shutdown();
	}

	RedisPool* operator->()
	{
		return this;
	}

private:

	std::string url_;
	Pool pool_;
};


//////////////////////////////////////////////////////////////

class Reply
{
public:
	Reply();
    Reply( RedisPtr redis, redisReply* r );
	Reply(const Reply& rhs);// = default;
	Reply(Reply&& rhs);// = default;
    Reply( RedisPtr redis, redisReply* r, bool freeReply );
    ~Reply();

	Reply& operator=(const Reply& rhs);// = default;
	Reply& operator=(Reply&& rhs);// = default;

    bool isError();
    bool isStatus();
    bool isString();
    bool isInteger();
    bool isNil();
    bool isArray();
    size_t size();
    Reply element(size_t i);
    std::string asString();
    long long asLong();
    size_t asInt();

    std::string str();

    RedisPtr operator()()
    {
    	return redis_;
    }

private:
    RedisPtr redis_;
    std::shared_ptr<redisReply> reply_;
};



//////////////////////////////////////////////////////////////

class Redis : public std::enable_shared_from_this<Redis>
{
public:

	typedef repro::Future<Reply> FutureType;

	static RedisPtr create(RedisPool& p);
	static RedisPtr create(RedisPool& p, prio::ThreadPool& tp);

	Redis(RedisPool& p, prio::ThreadPool& tp);
	~Redis();


    template<class ... Args>
    FutureType cmd(const Args& ... args)
    {
    	v_.clear();
        populate_args(v_,args...);

        return cmd();
    }


    FutureType cmd(const std::vector<std::string>& v)
    {
    	v_ = v;
        return cmd();
    }

    FutureType subscribe( const std::string& topic );

    void dispose();

private:

	prio::ThreadPool& threadPool_;

    FutureType cmd();
	void schedule_read(int flags);
	void schedule_write();
	void doRead();
	void doWrite();


    RedisPool& pool_;
    RedisPool::ResourcePtr ctx_;
	int done_;
	bool isSubscription_;
	std::string topic_;

	std::vector<std::string> v_;
	std::vector<const char*> argv_;
	std::vector<size_t> arglen_;

	repro::Promise<Reply> promise_;
	RedisPtr self_;
	prio::IO io_;
};


template<class ... Args>
repro::Future<Reply> RedisPool::cmd(const Args& ... args)
{
	auto p =  repro::promise<Reply>();

	std::vector<std::string> v;
	populate_args(v,args...);

	Redis::create(*this)->cmd(v)
	.then([p](Reply reply)
	{
		p.resolve(std::move(reply));
	})
	.otherwise(prio::reject(p));

    return p.future();
}

inline RedisPool::FutureType RedisPool::subscribe( const std::string& topic )
{
	auto p =  repro::promise<Reply>();

	std::vector<std::string> v;
	v.push_back("SUBSCRIBE");
	v.push_back(topic);

	Redis::create(*this)->cmd(v)
	.then([p](Reply reply)
	{
		p.resolve(std::move(reply));
	})
	.otherwise(prio::reject(p));

    return p.future();
}



} // close namespaces

#endif

