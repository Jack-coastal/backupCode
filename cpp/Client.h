#ifndef _COASTAL_DB_CLIENT_HH_
#define _COASTAL_DB_CLIENT_HH_

#include <string>
#include <boost\noncopyable.hpp>
#include <boost\date_time.hpp>
#include <boost\date_time\posix_time\posix_time.hpp>
#include <vector>

namespace Coastal
{
	namespace DB
	{
		enum SubscriptionFlags;
		struct Trade;
		struct Quote;
		struct Imbalance;
		struct Daily;
		

		class Client : private boost::noncopyable
		{
		public:
			/*!
			\param user username for coastal market data database
			\param passord password for coastal market data database
			\param server coastal market data database ip address
			*/
			Client(const std::string &user, const std::string &password, const std::string& server);
			virtual ~Client();

		public:
			//! Subscribe to marketdata for the given symbols within the provided date/time range.
			//! Query runs between begin/finish time daily.
			/*!
			\param flags bitflag of market data types to request
			\param symbols include symbol list to query -- there may be restrictions on the list size
			\param start the start date of the query -- inclusive
			\param end the end date of the query -- inclusive
			\param begin the start time of the query -- inclusive
			\param end the end time of the query -- inclusive
			*/
			void Subscribe(const SubscriptionFlags flags, 
				const std::vector<std::string> &symbols,
				const boost::gregorian::date &start,
				const boost::gregorian::date &end,
				const boost::posix_time::time_duration &begin,
				const boost::posix_time::time_duration &finish);
			
			//! Subscribe to all symbols within provided date/time range.
			//! Query runs between begin/finish time daily.
			/*!
			*/
			void SubscribeAll(const SubscriptionFlags flags,
				const boost::gregorian::date &start,
				const boost::gregorian::date &end,
				const boost::posix_time::time_duration &begin,
				const boost::posix_time::time_duration &finish);

		public:
			//! virtual methods to be implemented by user
			
			//! Called on every Imbalance update
			/*!
			\param imbalance imbalance data point
			*/
			virtual void OnImbalance( const Imbalance &imbalance ) = 0;
			
			//! Called on every Trade update
			/*!
			\param trade trade data point
			*/
			virtual void OnTrade( const Trade &trade ) = 0;
			
			//! Called on every Quote update
			/*!
			\param quote quote data point
			*/
			virtual void OnQuote( const Quote &quote ) = 0; 
			
			//! Called on every Daily update
			/*!
			\param daily daily data point
			*/
			virtual void OnDaily( const Daily &daily ) = 0;
			
			
			//! Called on every error during query
			/*!
			\param ex contains specific error information.
			*/
			virtual void OnError( const std::exception &ex ) = 0;


		private:
			const std::string user_;
			const std::string password_;
			const std::string server_;
		}; // client class
	}
}

#endif //_COASTAL_DB_CLIENT_HH_