#include <sstream>
#include <cstring>
#include <hiredis/hiredis.h>
#include <priocpp/api.h>
#include "reproredis/redis.h"

using namespace prio;


namespace reproredis {


///////////////////////////////////////////////////////////////


Future<RedisLocator::type*> RedisLocator::retrieve(const std::string& u)
{
	auto p = repro::promise<type*>();

	task( [u]()
	{
		Url url(u);
#ifndef _WIN32
		return redisConnectNonBlock(url.getHost().c_str(), url.getPort());
#else
		return redisConnect( url.getHost().c_str(), url.getPort() );
#endif
	})
	.then( [p](type* r)
	{
		p.resolve(r);
	})
	.otherwise( [p](const std::exception& ex)
	{
		p.reject(ex);
	});

	return p.future();
}

void RedisLocator::free( RedisLocator::type* t)
{
	redisFree(t);
}


///////////////////////////////////////////////////////////////


Reply::Reply()
{}

Reply::Reply( RedisPtr redis, redisReply* r )
	: redis_(redis), 
	  reply_(
		r, 
		[](redisReply* r) 
		{
			freeReplyObject((void*)r);
		}
	  )
{}

Reply::Reply( RedisPtr redis, redisReply* r, bool freeReply )
	: redis_(redis), 
	  reply_( 
		r, 
		[freeReply](redisReply* r) 
		{
			if(freeReply) 
				freeReplyObject((void*)r);
		}
	  )
{}

Reply::Reply( Reply&& rhs )
	: redis_( std::move(rhs.redis_) ),
	  reply_( std::move(rhs.reply_) )
{}

Reply::Reply(const Reply& rhs)
	: redis_(rhs.redis_),
	  reply_(rhs.reply_)
{}

Reply::~Reply()
{}

Reply& Reply::operator=(Reply&& rhs)
{
	if ( &rhs == this )
	{
		return *this;
	}

	reply_ = std::move(rhs.reply_);
	redis_ = std::move(rhs.redis_);

	return *this;
}


Reply& Reply::operator=(const Reply& rhs)
{
	if (&rhs == this)
	{
		return *this;
	}

	reply_ = rhs.reply_;
	redis_ = rhs.redis_;

	return *this;
}

bool Reply::isError()
{
	if ( isNil() ) return false;

	return reply_->type == REDIS_REPLY_ERROR;
}

bool Reply::isStatus()
{
	if ( isNil() ) return false;
	return reply_->type == REDIS_REPLY_STATUS;
}


bool Reply::isString()
{
	if ( isNil() ) return false;
	return reply_->type == REDIS_REPLY_STRING;
}

bool Reply::isInteger()
{
	if ( isNil() ) return false;
	return reply_->type == REDIS_REPLY_INTEGER;
}

bool Reply::isNil()
{
	return !reply_ || (reply_->type == REDIS_REPLY_NIL);
}

bool Reply::isArray()
{
	if ( isNil() ) return false;
	return reply_->type == REDIS_REPLY_ARRAY;
}

size_t Reply::size()
{
	if (!isArray()) return 0;
	return reply_->elements;
}

Reply Reply::element(size_t i)
{
	return Reply( RedisPtr(nullptr), reply_->element[i], false );
}

std::string Reply::asString()
{
	return std::string( reply_->str, reply_->len );
}

long long Reply::asLong()
{
	return reply_->integer;
}

size_t Reply::asInt()
{
	return (size_t)(reply_->integer);
}

std::string Reply::str()
{
	if ( isNil() || isError() || isArray() ) {
		return "";
	}
	return asString();
}


///////////////////////////////////////////////////////////////


RedisPtr Redis::create(RedisPool& p)
{
	return create(p,thePool());
}

RedisPtr Redis::create(RedisPool& p, ThreadPool& tp)
{
	auto ptr = std::make_shared<Redis>(p,tp);

	ptr->self_ = ptr;
	return ptr;
}

Redis::Redis(RedisPool& p, ThreadPool& tp)
	:
	  threadPool_(tp),
	  pool_(p),
	  done_(0),
	  isSubscription_(false)
{}

Redis::~Redis()
{}

void Redis::dispose()
{
	isSubscription_ = false;
	self_.reset();
}

Redis::FutureType Redis::subscribe( const std::string& topic )
{
	topic_ = topic;
	isSubscription_ = true;
	std::vector<std::string> v;
	v.push_back("subscribe");
	v.push_back(topic_);
	return cmd( v );
}

Redis::FutureType Redis::cmd()
{
	promise_ = repro::promise<Reply>();
	argv_.clear();
	arglen_.clear();
	for ( size_t i = 0; i < v_.size(); i++)
	{
		argv_.push_back(v_[i].c_str());
		arglen_.push_back(v_[i].size());
	}

	self_ = shared_from_this();
#ifndef _WIN32

	if(ctx_)
	{
		schedule_write();
	}
	else
	{
		pool_()
		.then( [this](RedisPool::ResourcePtr redis)
		{
			ctx_ = redis;
			schedule_write();
		})
		.otherwise( [this](const std::exception& ex)
		{
			promise_.reject(ex);
			self_->dispose();
		});
	}
#else

	pool_()
	.then([this](RedisPool::ResourcePtr redis)
	{
		ctx_ = redis;
		return task([this]()
		{
			redisReply* r = (redisReply*)redisCommandArgv(*ctx_, argv_.size(), &argv_[0], &arglen_[0]);

			if ((*ctx_)->err != 0)
			{
				ctx_->markAsInvalid();
				throw(repro::Ex((*ctx_)->errstr));
			}

			Reply rep(shared_from_this(), r);

			if (rep.isError())
			{
				throw(repro::Ex((*ctx_)->errstr));
			}

			// shameful workaround to make subscriptions "work" on win32
			// this will burn a thread from default threadpool if you do not pass
			// a dedicated pool when creating the redis ptr :-O
			while (isSubscription_)
			{
				redisBufferRead(*ctx_);
				redisReply* r = 0;
				redisGetReply(*ctx_, (void**)&r);

				if ((*ctx_)->err != 0)
				{
					ctx_->markAsInvalid();
					throw(repro::Ex((*ctx_)->errstr));
				}

				Reply reply(shared_from_this(), r);

				if (reply.isError())
				{
					throw(repro::Ex((*ctx_)->errstr));
				}

				promise_.resolve(std::move(reply));

				if (!isSubscription_)
				{
					rep = Reply();
					break;
				}
			}

			return std::move(rep);
		}, threadPool_);
	})
	.then([this](Reply r)
	{
		auto tmp = self_;
		auto prot = promise_;
		if (!isSubscription_)
		{
			dispose();
			prot.resolve(std::move(r));
		}
		else
		{ 
			dispose();
		}
	})
	.otherwise([this](const std::exception& ex)
	{
		promise_.reject(ex);
		self_->dispose();
	});

#endif
	return promise_.future();
}

void Redis::schedule_read(int flags)
{
	io_.onRead((*ctx_)->fd)
	.then( [this] ()
	{
		doRead();
	})
	.otherwise([](const std::exception& ex)
	{
		std::cout << ex.what() << std::endl;
	});
}

void Redis::schedule_write()
{
	io_.onWrite((*ctx_)->fd)
	.then( [this] ()
	{
		doWrite();
	})
	.otherwise([](const std::exception& ex)
	{
		std::cout << ex.what() << std::endl;
	});
}

void Redis::doRead()
{
	RedisPtr ptr = shared_from_this();

	redisBufferRead(*ctx_);
	redisReply* r = 0;
	redisGetReply(*ctx_, (void**)&r);

	if( (*ctx_)->err != 0 )
	{
		promise_.reject(repro::Ex( (*ctx_)->errstr));
		ctx_->markAsInvalid();
		return;
	}

	Reply rep(shared_from_this(),r);

	if(rep.isError())
	{
		promise_.reject(repro::Ex( rep.asString() ));
		return;
	}

	if(!isSubscription_)
	{
		dispose();
	}
	promise_.resolve(std::move(rep));
	if (isSubscription_)
	{
		schedule_read(0);
	}	
}


void Redis::doWrite()
{
	redisCommandArgv(* ctx_, argv_.size(), &argv_[0], &arglen_[0] );
	redisBufferWrite(*ctx_, &done_);

	if( (*ctx_)->err != 0 )
	{
		promise_.reject(repro::Ex( (*ctx_)->errstr));
		ctx_->markAsInvalid();
		return;
	}

	if(done_)
	{
		schedule_read(0);
		return;
	}

	schedule_write();
}


///////////////////////////////////////////////////////////////



Future<RedisPool::ResourcePtr> RedisPool::get()
{
	auto p = repro::promise<ResourcePtr>();
	pool_.get(url_)
	.then( [p](ResourcePtr r)
	{
		p.resolve(r);
	});
	return p.future();
}

Future<RedisPool::ResourcePtr> RedisPool::operator()()
{
	return get();
}



} // close namespaces

