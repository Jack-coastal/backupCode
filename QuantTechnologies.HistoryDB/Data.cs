using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using QuantTechnologies.API;

namespace QuantTechnologies.HistoryDB
{
    /// <summary>
    /// Container for data message.  Refer to individual fields to get data pertaining to the request made.
    /// </summary>
    public class Data
    {
        /// <summary>
        /// Used to denote the data type for a data message.
        /// </summary>
        public enum DataType
        {
            BAR,
            TRADE,
            QUOTE,
            DAILY,
            IMBALANCE,
            PARTITIONS,
            EXGPRINTS,
            NEWS
        }

        /// <summary>
        /// Contains strings representing the dates available on the table.
        /// </summary>
        public class Partitions 
        {
            public string Dates;
        }

        /// <summary>
        /// Data type of the data message.  See DataType enumeration for possible values.
        /// </summary>
        public DataType type { get; internal set; }

        /// <summary>
        /// Contains a tick.  Will be null unless user invokes FetchTrades in a client request.
        /// </summary>
        /// <remarks>
        /// <pre>Fields:
        ///     BestBid         decimal
        ///     BestBidSize     uint
        ///     BestOffer       decimal
        ///     BestOfferSize   uint
        ///     Date            DateTime
        ///     High            decimal     High of the day so far.
        ///     Last            decimal     Last trade.  Will not change outside of market hours.
        ///     Low             decimal     Low of the day so far.
        ///     Open            decimal
        ///     Price           decimal     Last trade, inclusive of pre-and-post market trading.
        ///     Size            uint        Volume for the last trade.
        ///     Symbol          string
        ///     Time            TimeSpan
        ///     TotalVolume     ulong       Volume for the day so far.</pre>
        /// </remarks>
        public Tick trade { get; internal set; }

        /// <summary>
        /// Contains a quote.  Will be null unless user invokes FetchQuotes in a client request.
        /// </summary>
        /// <remarks>
        /// <pre>Fields:
        ///     Ask         decimal     If quote is BEST_ASK, the new best offer price.  Otherwise 0.
        ///     AskSize     uint        If quote is BEST_ASK, the new best offer size.  Otherwise 0.
        ///     AskExchange char        see http://nxcoreapi.com/doc/table_NxST_EXCHANGE.html
        ///     Bid         decimal     If quote is BEST_BID, the new best bid price.  Otherwise 0.
        ///     BidSize     uint        If quote is BEST_BID, the new best bid size.  Otherwise 0.
        ///     BidExchange char        see http://nxcoreapi.com/doc/table_NxST_EXCHANGE.html
        ///     Date        DateTime
        ///     Symbol      string
        ///     Time        TimeSpan
        ///     Type        QuoteType   Indicates whether this quote is a new best bid or new best offer/ask (or both.)
        ///     
        /// Class-specific data types:
        ///     QuoteType   enum
        ///         NONE        0
        ///         BEST_BID    1
        ///         BEST_ASK    2</pre>
        /// </remarks>
        public Quote quote { get; internal set; }

        /// <summary>
        /// Contains an imbalance tick.  Will be null unless user invokes FetchImbalances in a client request.
        /// </summary>
        /// <remarks>
        /// <pre>Fields:
        ///     BuyVolume               uint        If buy imbalance, same as net imbalance.  Otherwise 0.
        ///     ClearingPrice           decimal
        ///     ClosingClearingPrice    decimal
        ///     Date                    DateTime
        ///     NetImbalance            uint        Unsigned imbalance quantity.  Utilize Side to get a signed quantity.
        ///     PairedVolume            uint
        ///     ReferencePrice          decimal
        ///     SellVolume              uint        If sell imbalance, same as net imbalance.  Otherwise 0.
        ///     Side                    ImbSide     Indicates this is either a buy or a sell imbalance.
        ///     Symbol                  string
        ///     Time                    TimeSpan
        ///     Type                    ImbType     Indicates this is an opening, closing, or other imbalance.
        ///     
        /// Class-specific data types:
        ///     ImbSide enum
        ///         BUY     1
        ///         SELL    2
        ///         NONE    0
        ///     ImbType enum
        ///         OPEN    1
        ///         CLOSE   2
        ///         OTHER   3</pre>
        /// </remarks>
        public Imbalance imbalance { get; internal set; }

        /// <summary>
        /// Contains a daily bar.  Will be null unless user invokes FetchDaily in a client request.
        /// </summary>
        /// <remarks>
        /// <pre>Fields:
        ///     Close   decimal
        ///     Date    DateTime
        ///     High    decimal
        ///     Low     decimal
        ///     Open    decimal
        ///     Symbol  string
        ///     Time    TimeSpan
        ///     Volume  ulong</pre>
        /// </remarks>
        public OHLC daily { get; internal set; }

        public Partitions dates { get; internal set; }

        /// <summary>
        /// Contains a bar.  Will be null unless user invokes FetchBars in a client request.
        /// </summary>
        /// <remarks>
        /// <pre>Fields:
        ///     Close   decimal
        ///     Date    DateTime
        ///     Freq    Frequency       Indicates the periodicity (ie, 5) and frequency (ie, minutes) of this bar.
        ///     High    decimal
        ///     Low     decimal
        ///     Open    decimal
        ///     Symbol  string
        ///     Time    TimeSpan
        ///     Volume  uint
        ///     
        /// Class-specific data types:
        ///     Frequency:  struct
        ///         Period  uint
        ///         Freq    FreqType
        ///         
        ///         FreqType:   enum
        ///             TICKS   0
        ///             SECONDS 1
        ///             MINUTES 2
        ///             HOURS   3
        ///             DAYS    4</pre>
        /// </remarks>
        public Bar bar { get; internal set; }

        public Exgprints exgPrints { get; internal set; }

        public News news { get; internal set; }
    }
}
