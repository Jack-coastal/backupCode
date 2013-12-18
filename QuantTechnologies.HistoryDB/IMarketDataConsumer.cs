using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace QuantTechnologies.HistoryDB
{
    /// <summary>
    /// Responder for incoming data requested by a Client class.
    /// </summary>
    /// <remarks>Example Code:
    /// <pre>class MyMarketDataObserver : IMarketDataObserver
    ///{
    ///    public void Notify(decimal pctCompleted) { Console.WriteLine("{0:P} completed...", pctCompleted); }
    ///    public void OnCompleted()                { Console.WriteLine("Done..."); }
    ///    public void OnError(Exception error)     { Console.WriteLine(error.ToString()); }
    ///
    ///    public void OnNext(Data value)
    ///    {
    ///        try
    ///        {
    ///            // TODO: process the fields as you want.
    ///            // It is imperative that this code isn't slow!
    ///            switch (value.type)
    ///            {
    ///                case Data.DataType.DAILY:
    ///                    Console.WriteLine("D: {0}", value.daily.Symbol);
    ///                    break;
    ///                case Data.DataType.IMBALANCE:
    ///                    Console.WriteLine("I: {0}|{1}", value.imbalance.Symbol, value.imbalance.Date);
    ///                    break;
    ///                case Data.DataType.QUOTE:
    ///                    Console.WriteLine("Q: {0}", value.quote.Symbol);
    ///                    break;
    ///                case Data.DataType.TRADE:
    ///                    Console.WriteLine("T: {0}", value.trade.Symbol);
    ///                    break;
    ///            }
    ///        }
    ///        catch (Exception ex)
    ///        {
    ///            // TODO: Handle exception
    ///        }
    ///    }
    ///}</pre>
    /// </remarks>
    public interface IMarketDataObserver : IObserver<Data>
    {
        /// <summary>
        /// Periodically reports completion state for the query.
        /// </summary>
        /// <param name="pctCompleted">Percentage completion.  Decimal form. (ie, .034 = 3.4% complete)</param>
        void Notify(Decimal pctCompleted);

        /// <summary>
        /// Raised when the query has received all requested data.
        /// </summary>
        void OnCompleted();

        /// <summary>
        /// Raised when an error has occured in the query.  This aborts the rest of the query.
        /// </summary>
        /// <param name="error">Exception data raised by the query.</param>
        void OnError(Exception error);

        /// <summary>
        /// Raised whenever a new data point is received.
        /// </summary>
        /// <param name="value">Data container containing the new data point.</param>
        /// <remarks>
        /// Utilize the Data.type switch to determine which type of data is being received, and respond accordingly
        /// by accessing the Data.X member, corresponding to the type.
        /// 
        /// <para/>Example code:
        /// <pre>        public void OnNext(Data value)
        ///{
        ///    try
        ///    {
        ///        // TODO: process the fields as you want.
        ///        // It is imperative that this code isn't slow!
        ///        switch (value.type)
        ///        {
        ///            case Data.DataType.DAILY:
        ///                Console.WriteLine("D: {0}", value.daily.Symbol);
        ///                break;
        ///            case Data.DataType.IMBALANCE:
        ///                Console.WriteLine("I: {0}|{1}", value.imbalance.Symbol, value.imbalance.Date);
        ///                break;
        ///            case Data.DataType.QUOTE:
        ///                Console.WriteLine("Q: {0}", value.quote.Symbol);
        ///                break;
        ///            case Data.DataType.TRADE:
        ///                Console.WriteLine("T: {0}", value.trade.Symbol);
        ///                break;
        ///        }
        ///    }
        ///    catch (Exception ex)
        ///    {
        ///        // TODO: Handle exception
        ///    }
        ///}</pre>
        /// </remarks>
        void OnNext(Data value);
    }
}
