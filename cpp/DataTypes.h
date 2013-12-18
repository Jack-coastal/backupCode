#ifndef _COASTAL_DB_DATATYPES_HH_
#define _COASTAL_DB_DATATYPES_HH_

#include <string>
#include <cstdint>

#include <boost/date_time/gregorian/gregorian_types.hpp>
#include <boost/date_time/posix_time/posix_time_types.hpp>

namespace Coastal
{
	namespace DB
	{
		 /*! Market data subscription flags */
		enum SubscriptionFlags
		{
			DAILY = 1, /*! Requests Daily updates */
			TRADE = 2, /*! Requests Trade updates */
			QUOTE = 4, /*! Requests Quote updates */
			NYSE_IMBALANCE = 8, /*! Requests NYSE Imbalance updates */
			NASD_IMBALANCE = 16, /*! Requests NASD Imbalance updates */
			AMEX_IMBALANCE = 32, /*! Requests AMEX Imbalance updates */
			ARCA_IMBALANCE = 64 /*! Requests ARCA Imbalance updates */
		};

		inline SubscriptionFlags operator|(SubscriptionFlags a, SubscriptionFlags b)
		{
			return static_cast<SubscriptionFlags>(static_cast<int>(a) | static_cast<int>(b));
		}

		struct MarketData
		{
			std::string symbol;
			boost::gregorian::date date;
			boost::posix_time::time_duration time;
		};

		struct Trade : public MarketData
		{
			double open;
			double high;
			double low;
			double last;
			double price;
			uint32_t size;
			uint64_t volume;

			uint16_t exg;
		};

		struct Quote : public MarketData
		{
			double bid;
			uint32_t bid_size;
			uint16_t bid_exg;

			double ask;
			uint32_t ask_size;
			uint16_t ask_exg;

			double best_bid;
			uint32_t best_bid_size;
			uint16_t best_bid_exg;

			double best_ask;
			uint32_t best_ask_size;
			uint16_t best_ask_exg;
		};

		struct Imbalance : public MarketData
		{
			enum Side
			{
				NONE = 0,
				BUY = 1,
				SELL = 2
			};

			enum ImbType {
				UNKNOWN = 0,
				OPEN = 1,
				MARKET = 2,
				HALT = 3,
				CLOSE = 4,
				NO_IMBALANCE = 5
			};
			
			std::string flag;
			ImbType imbalance_type;
			Side side;
			uint32_t paired_shares;
			int32_t market_imbalance;
			int32_t total_imbalance;
			double ref_price;
			double far_price;
			double near_price;
			double clearing_price;
			uint32_t tag;
		};

		struct Daily : public MarketData
		{
			double open;
			double high;
			double low;
			double close;
			uint64_t volume;
		};

		
	}
}

#endif //_COASTAL_DB_DATATYPES_HH_