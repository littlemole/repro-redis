#include <sstream>
#include <cstring>
#include <priocpp/api.h>
#include "reproredis/redis.h"

using namespace prio;
using namespace repro;


namespace reproredis {


///////////////////////////////////////////////////////////////


repro::Future<RedisLocator::type> RedisLocator::retrieve(const std::string& url)
{
	return RedisConnection::connect(url);
}

void RedisLocator::free(RedisLocator::type t)
{
	t->con->close();
	delete t;
}


Future<RedisConnection*> RedisConnection::connect(const std::string& url)
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
	.otherwise([p](const std::exception_ptr& ex)
	{
		p.reject(ex);
	});
	return p.future();
}


class RedisParser
{
public:

	RedisPool::ResourcePtr con;
	std::string buffer;
	size_t pos = 0;

	RedisParser();
	~RedisParser();

	repro::Future<RedisResult::Ptr> parse();
	repro::Future<std::pair<std::string, std::string>> listen( bool& shutdown);
	void consume(size_t n);

	prio::ConnectionPtr connection() {	return con->con; }

	repro::Future<RedisResult::Ptr> do_cmd(const std::string& cmd, repro::Promise<RedisResult::Ptr> p)
	{
		con->con->write(cmd)
		.then([this](prio::Connection::Ptr con)
		{
			return parse();
		})
		.then([this,p](RedisResult::Ptr r)
		{
			r->con = con;			
			p.resolve(r);
			delete this;
		})	
		.otherwise([this,p](const std::exception_ptr& ex)
		{
			markAsInvalid();
			p.reject(ex);
			delete this;
		});

		return p.future();
	}

	void markAsInvalid()
	{
		if(con)
		{
			prio::Resource::invalidate(con);
//			con->markAsInvalid();
		}
	}

private:

	repro::Promise<RedisResult::Ptr> p_;
	repro::Promise<std::pair<std::string, std::string>> p2_;
	std::shared_ptr<RedisArrayResult> result_;	
};


repro::Future<RedisResult::Ptr> RedisResult::do_cmd(std::string cmd)
{
	auto p =  repro::promise<RedisResult::Ptr>();

	RedisParser* parser = new RedisParser();
	parser->con = con;

	return parser->do_cmd(cmd,p);

}


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
		auto p = repro::promise<RedisResult::Ptr>();
		
		if(size_==-1)
		{
			nil_ = true;
			return p.resolved(shared_from_this());
		}
		if(size_==0)
		{
			return p.resolved(shared_from_this());
		}


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
	parser_.connection()->read()
	.then([this,p](prio::Connection::Ptr con, std::string data)
	{
		parser_.buffer.append(data);
		parse_response(p);
	})		
	.otherwise([p](const std::exception_ptr& ex)
	{
		p.reject(ex);
	});			
}


class RedisSimpleStringResult : public RedisResult
{
public:

	RedisSimpleStringResult( std::string s)
		: str_(s)
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
};


class RedisErrorResult : public RedisResult
{
public:

	RedisErrorResult(std::string s)
		: str_(s)
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
};

class RedisIntegerResult : public RedisResult
{
public:

	RedisIntegerResult(std::string s)
		: str_(s)
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
};


class RedisArrayResult : public RedisResult
{
public:

	RedisArrayResult(RedisParser& p, long s)
		:size_(s),parser_(p),p_(repro::promise<RedisResult::Ptr>() )
	{}

	~RedisArrayResult()
	{}
	
	virtual bool isArray()    						 { return true; }
	virtual bool isNill()     						 { return nil_; }
	virtual long size()   							 { return size_; }
	virtual RedisResult::Ptr element(std::size_t i)  { return elements_[i]; }

	virtual Future<RedisResult::Ptr> parse()
	{
		std::cout << "RA parse" << std::endl;

		if(size_==-1)
		{
			std::cout << "RA: size_==-1" << std::endl;
			nil_ = true;
			return p_.resolved(shared_from_this());
		}
		if(size_==0)
		{
			std::cout << "RA: size_== 0" << std::endl;
			return p_.resolved(shared_from_this());
		}


		read()
		.then([this](RedisResult::Ptr r)
		{
			if(size_ == (long)elements_.size())
			{
				std::cout << "RA: size_== " << size_ << std::endl;
				p_.resolve(shared_from_this());
				return;
			}
			parse();
		})		
		.otherwise([this](const std::exception_ptr& ex)
		{
			p_.reject(ex);
		});	
		return p_.future();
	}

	void clear()
	{
		size_ = 1;
		elements_.clear();
		p_ = repro::promise<RedisResult::Ptr>();
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


RedisResult::Ptr RedisArrayResult::resultFactory(const std::string& cmd)
{
	RedisResult::Ptr r;
	switch(cmd[0])
	{
		case '-' : // error
		{
			r = std::make_shared<RedisErrorResult>(cmd.substr(1));
			break;
		}
		case '+' : // simple string
		{
			r = std::make_shared<RedisSimpleStringResult>(cmd.substr(1));
			break;
		}
		case ':' : // simple integer
		{
			r = std::make_shared<RedisIntegerResult>(cmd.substr(1));
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
	.otherwise([p](const std::exception_ptr& ex)
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

	parser_.connection()->read()
	.then([this,p](prio::Connection::Ptr con, std::string data)
	{
		parser_.buffer.append(data);
		read()
		.then([p](RedisResult::Ptr r)
		{
			p.resolve(r);
		})
		.otherwise([p](const std::exception_ptr& ex)
		{
			p.reject(ex);
		});			
	})		
	.otherwise([p](const std::exception_ptr& ex)
	{
		p.reject(ex);
	});			

	return p.future();
}




RedisParser::RedisParser()
	: p_ (repro::promise<RedisResult::Ptr>())
{
	result_ = std::make_shared<RedisArrayResult>(*this,1);		
	p2_ = repro::promise<std::pair<std::string, std::string>>();	
}

RedisParser::~RedisParser()
{
}

repro::Future<RedisResult::Ptr> RedisParser::parse()
{
	RedisArrayResult* rar = (RedisArrayResult*)result_.get();
	rar->parse()
	.then([this](RedisResult::Ptr r)
	{
		RedisResult::Ptr res = r->element(0);
		res->con = con;
		auto tmp = p_;

		tmp.resolve(res);
	})		
	.otherwise([this](const std::exception_ptr& ex)
	{
		markAsInvalid();
		auto tmp = p_;
		tmp.reject(ex);
	});	

	return p_.future();
}

RedisPool::FutureType RedisPool::do_cmd(const std::string& cmd)
{
	auto p = repro::promise<RedisResult::Ptr>();

	RedisParser* parser = new RedisParser();

	get()
	.then([p, cmd, parser](RedisPool::ResourcePtr redis)
	{
		parser->con = redis;
		parser->do_cmd(cmd,p);
	})
	.otherwise([p, parser](const std::exception_ptr& ex)
	{
		parser->markAsInvalid();
		delete parser;
		p.reject(ex);
	});

	return p.future();
}


repro::Future<std::pair<std::string, std::string>> RedisParser::listen( bool& shutdown )
{

	if (shutdown) 
	{
		return p2_.future();
	}

	bool& b = shutdown;

	RedisArrayResult* rar = (RedisArrayResult*)result_.get();
	rar->clear();
	rar->parse()
	.then([this,&b](RedisResult::Ptr r)
	{
		if(!r || r->isError() || r->isNill() || !r->isArray() || r->size() < 1 )
		{
			std::cout << (bool)(!r) << std::endl;// << " " << r->isError() << " " <<  r->isNill() << " :: " << r->str() << std::endl;
			throw repro::Ex("invalid redis channel reply 1");
		}

		RedisResult::Ptr res = r->element(0);

		if( res->isError() || res->isNill() || !res->isArray() || res->size() < 3 || res->element(0)->str() != "message")
		{
			std::cout << (bool)(!r) << " " << r->isError() << " " <<  r->isNill() << " :: " << r->str() << std::endl;
			throw repro::Ex("invalid redis channel reply");
		}

		std::string channel = res->element(1)->str();
		std::string msg     = res->element(2)->str();

		p2_.resolve(std::make_pair(channel,msg));
		if (!b)
		{
			listen( b);
		}
	})		
	.otherwise([this](const std::exception_ptr& ex)
	{
		markAsInvalid();
		p2_.reject(ex);
	});	

	return p2_.future();
}

void RedisParser::consume(size_t n)
{
	pos += n;
	
}




RedisSubscriber::RedisSubscriber(RedisPool& p)
	: pool_(p)
{
	parser_ = std::make_shared<RedisParser>();
}

RedisSubscriber::~RedisSubscriber()
{
	unsubscribe();
	//shutdown_ = true;
}

void RedisSubscriber::unsubscribe()
{
	if(parser_->con && 
	 !::prio::Resource::InvalidResources<RedisConnection*>::is_invalid(parser_->con.get()))
	//parser_->con->valid())
	{
		parser_->connection()->cancel();
		parser_->connection()->close();
	}
	shutdown_ = true;
}


prio::Callback<std::pair<std::string,std::string>>& RedisSubscriber::subscribe(const std::string& topic)
{
	Serializer serializer;
	std::string cmd = serializer.serialize("subscribe", topic);

	RedisParser* parser = new RedisParser();

	pool_.get()
	.then([cmd,parser](RedisPool::ResourcePtr redis)
	{
		parser->con = redis;
		return parser->connection()->write(cmd);
	})
	.then([this,parser](prio::Connection::Ptr con)
	{				
		parser_->con = parser->con;
		parser->markAsInvalid();
		
		parser->parse()
		.then([this,parser](RedisResult::Ptr r)
		{				
			if ( r->isError() || r->isNill() || !r->isArray() || r->element(0)->str() != "subscribe" )
			{
				delete parser;
				throw repro::Ex("redis subscribe failed");
			}

			auto f = parser_->listen(shutdown_);
			delete parser;
			return f;
		})
		.then([this](std::pair<std::string, std::string> r)
		{
			try
			{
				cb_.resolve(r);
			}
			catch (...)
			{}
		})
		.otherwise([this, parser](const std::exception_ptr& eptr)
		{
			try {
				std::rethrow_exception(eptr);
			}
			catch(const std::exception& ex)
			{
				std::cout << "!!!" << ex.what() << std::endl;
			}
			cb_.reject(eptr);
			//parser->markAsInvalid();
			//delete parser;
		});		
	})		
	.otherwise([this, parser](const std::exception_ptr& eptr)
	{
		cb_.reject(eptr);
		parser->markAsInvalid();
		delete parser;
	});

	return cb_;
}


RedisPool::RedisPool(const std::string& url, int capacity)
	: url_(url), pool_(capacity)
{}

RedisPool::RedisPool() {}

RedisPool::~RedisPool() {}


void RedisPool::shutdown()
{
	pool_.shutdown();
}


Future<RedisPool::ResourcePtr> RedisPool::get()
{
	auto p = repro::promise<ResourcePtr>();
	pool_.get(url_)
	.then( [p](ResourcePtr r)
	{
		p.resolve(r);
	})
	.otherwise( reject(p) );
	return p.future();
}

} // close namespaces

