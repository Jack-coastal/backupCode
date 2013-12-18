using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using QuantTechnologies.API.MarketData;
using System.Reactive.Linq;
using System.Threading;
using System.Reactive.Disposables;
using System.Threading.Tasks;
using Thrift.Transport;
using Thrift.Protocol;
using System.Diagnostics.Contracts;
using System.IO;
using System.Text.RegularExpressions;

namespace QuantTechnologies.HistoryDB
{
    public class Client : IDisposable
    {
        private string unionType = "";
        private int unionNum = 0;        
		private uint Maxrows=uint.MaxValue;
        private string db = "marketdata";

        #region Fields
        private readonly string mUser;
        private readonly string mPassword;
        private readonly string mServer;
        private ManualResetEventSlim mBarrier = new ManualResetEventSlim(true);
        CancellationTokenSource _cancellationSource;
        #endregion

        #region Properties
        internal SubscriptionOptions Options { get; set; }
        internal IEnumerable<string> Symbols { get; set; }
        internal IMarketDataObserver Observer { get; set; }
        internal DateTime StartDate { get; set; }
        internal DateTime EndDate { get; set; }
        internal TimeSpan StartTime { get; set; }
        internal TimeSpan EndTime { get; set; }
        internal ImbalanceSource ImbSource { get; set; }
        internal Table Tablesource { get; set; }
        #endregion

        #region Constructors
        private Client(string user, string password, string server)
        {
            mUser = user;
            mPassword = password;
            mServer = server;
            Options = SubscriptionOptions.NONE;
        }
        #endregion

        #region Exchange_struct
        struct nxST_EXCHANGE //(3) - Exchange Codes
        {
            public enum String
            {
                NQEX = 1,
                NQAD = 2,
                NYSE = 3,
                AMEX = 4,
                CBOE = 5,
                ISEX = 6,
                PACF = 7,
                CINC = 8,
                PHIL = 9,
                OPRA = 10,
                BOST = 11,
                NQNM = 12,
                NQSC = 13,
                NQBB = 14,
                NQPK = 15,
                NQAG = 16,
                CHIC = 17,
                TSE = 18,
                CDNX = 19,
                CME = 20,
                NYBT = 21,
                NYBA = 22,
                COMX = 23,
                CBOT = 24,
                NYMX = 25,
                KCBT = 26,
                MGEX = 27,
                WCE = 28,
                ONEC = 29,
                DOWJ = 30,
                NNEX = 31,
                SIMX = 32,
                FTSE = 33,
                EURX = 34,
                ENXT = 35,
                DTN = 36,
                LMT = 37,
                LME = 38,
                IPEX = 39,
                MX = 40,
                WSE = 41,
                C2 = 42,
                MDAM = 43,
                CLRP = 44,
                BARK = 45,
                TEN4 = 46,
                NQBX = 47,
                HOTS = 48,
                EUUS = 49,
                EUEU = 50,
                ENCM = 51,
                ENID = 52,
                ENIR = 53,
                CFE = 54,
                PBOT = 55,
                HWTB = 56,
                NQNX = 57,
                BTRF = 58,
                NTRF = 59,
                BATS = 60,
                NYLF = 61,
                PINK = 62,
                BATY = 63,
                EDGE = 64,
                EDGX = 65,
                RUSL = 66
            }
        }
        #endregion

        #region Public Methods
        public static Client Create(string user, string password, string server, string dataDB = "marketdata")
        {
            Contract.Requires(!string.IsNullOrEmpty(user));
            Contract.Requires(!string.IsNullOrEmpty(password));
            Contract.Requires(!string.IsNullOrEmpty(server));

            return new Client(user, password, server) { db = dataDB };
        }

        /// <summary>
        /// Get trades for given set of symbols
        /// </summary>
        /// <param name="symbols"></param>
        /// <returns></returns>
        public Client FetchTrades(IEnumerable<string> symbols = null)
        {
            return Subscribe(SubscriptionOptions.TRADE, symbols);
        }

        /// <summary>
        /// Get Quotes for the given set of symbols
        /// </summary>
        /// <param name="symbols"></param>
        /// <returns></returns>
        public Client FetchQuotes(IEnumerable<string> symbols = null)
        {
            return Subscribe(SubscriptionOptions.QUOTE, symbols);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="symbols"></param>
        /// <returns></returns>
        public Client FetchImbalances(ImbalanceSource source, IEnumerable<string> symbols = null)
        {
            this.ImbSource = source;
            return Subscribe(SubscriptionOptions.IMBALANCE, symbols);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="symbols"></param>
        /// <returns></returns>
        public Client FetchDaily(IEnumerable<string> symbols = null)
        {
            return Subscribe(SubscriptionOptions.DAILY, symbols);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="symbols"></param>
        /// <returns></returns>
        public Client ShowDate(Table source)
        {
            this.Tablesource = source;
           // SubscriptionOptions p;
            Options = SubscriptionOptions.ALL;
            return Subscribe(Options,null);
        }

        /// <summary>
        /// Get Exgprints for given set of symbols
        /// </summary>
        /// <param name="symbols"></param>
        /// <returns></returns>
        public Client FetchExgprints(IEnumerable<string> symbols = null)
        {
            return Subscribe(SubscriptionOptions.EXGPRINTS, symbols);
        }

        /// <summary>
        /// Get Exgprints for given set of symbols
        /// </summary>
        /// <param name="symbols"></param>
        /// <returns></returns>
        public Client FetchNews(IEnumerable<string> symbols = null)
        {
            return Subscribe(SubscriptionOptions.NEWS, symbols);
        }


        /// <summary>
        /// Query from startDate, running daily from startTime
        /// </summary>
        /// <param name="startDate"></param>
        /// <param name="startTime"></param>
        /// <returns></returns>
        public Client From(DateTime startDate, TimeSpan? startTime = null)
        {
            StartDate = startDate;
            if (startTime.HasValue)
                StartTime = startTime.Value;
            else
                StartTime = TimeSpan.MinValue;
            return this;
        }

        public Client To(DateTime endDate, TimeSpan? endTime = null)
        {
            Contract.Requires(StartDate != null);
            Contract.Requires(endDate != null);
            Contract.Requires(StartDate <= endDate);

            EndDate = endDate;
            if (endTime.HasValue)
                EndTime = endTime.Value;
            else
                EndTime = TimeSpan.MaxValue;

            Contract.Ensures(StartTime <= EndTime);
            return this;
        }

        public Client With(IMarketDataObserver observer)
        {
            Observer = observer;
            return this;
        }

        public IDisposable Query()
        {
            Contract.Requires(Observer != null);
            Contract.Requires(StartDate != null);
            Contract.Requires(EndDate != null);

            // restrict to 1 concurrent active query
            mBarrier.Wait();
            mBarrier.Reset();

            if (Observer != null) // won't ever be null, we're using code contracts to check this!
            {
                try
                {
                    // create an observable to request the data and encapsulate all the processing logic
                    return Observable.Create<Data>(o =>
                    {
                        var tokenSource = new CancellationTokenSource();
                        var cancelToken = tokenSource.Token;
                        _cancellationSource = tokenSource;

                        var task = Task.Factory.StartNew(() =>
                        {
                            // create thrift client connection
                            var socket = new TSocket(mServer, 5049);
                            var transport = new TBufferedTransport(socket);
                            var protocol = new TBinaryProtocol(transport);
                            var client = new TCLIService.Client(protocol);

                            // open transport/establish server connection
                            transport.Open();
                            var req = new TOpenSessionReq()
                            {
                                Client_protocol = TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V1,
                                Username = mUser,
                                Password = mPassword
                            };
                            var openResp = client.OpenSession(req);
                            var sessHandle = openResp.SessionHandle;

                            //-----------------QUERY---------------------------------
                            #region Query
                            //Perform actual query -- by switching on option from subscribe

                            #region  check UnionType
                            bool imbExist = false;

                          /*  if ((Options & SubscriptionOptions.IMBALANCE) == SubscriptionOptions.IMBALANCE)
                            {
                                //Console.WriteLine("IMBALANCES");
                                Options = Options
                                    | SubscriptionOptions.AMEX_IMBALANCE
                                    | SubscriptionOptions.ARCA_IMBALANCE
                                    | SubscriptionOptions.NYSE_IMBALANCE
                                    | SubscriptionOptions.NASD_IMBALANCE;
                            }*/

                            if ((Options & SubscriptionOptions.TRADE) == SubscriptionOptions.TRADE)
                            {
                                //Console.WriteLine("T");
                                unionType = "T";
                                unionNum++;
                            }
                            if ((Options & SubscriptionOptions.QUOTE) == SubscriptionOptions.QUOTE)
                            {
                                //Console.WriteLine("Q");
                                unionType = unionType + "Q";
                                unionNum++;
                            }                           
                            if ((Options & SubscriptionOptions.AMEX_IMBALANCE) == SubscriptionOptions.AMEX_IMBALANCE)
                            {
                                //Console.WriteLine("AMEX");
                                unionType = unionType + "I";
                                imbExist = true;
                                unionNum++;
                            }
                            if ((Options & SubscriptionOptions.ARCA_IMBALANCE) == SubscriptionOptions.ARCA_IMBALANCE)
                            {
                                //Console.WriteLine("ARCA");
                                if (imbExist == false)
                                {
                                    unionType = unionType + "I";
                                    imbExist = true;
                                }
                                unionNum++;
                            }
                            if ((Options & SubscriptionOptions.NYSE_IMBALANCE) == SubscriptionOptions.NYSE_IMBALANCE)
                            {
                                //Console.WriteLine("NYSE");
                                if (imbExist == false)
                                {
                                    unionType = unionType + "I";
                                    imbExist = true;
                                }
                                unionNum++;
                            }
                            if ((Options & SubscriptionOptions.NASD_IMBALANCE) == SubscriptionOptions.NASD_IMBALANCE)
                            {
                                //Console.WriteLine("NASD");
                                if (imbExist == false)
                                {
                                    unionType = unionType + "I";
                                    imbExist = true;
                                }
                                unionNum++;
                            }
                            #endregion


                            if ( unionNum == 1 || unionNum==0)
                            {
                                #region benchmark test

                                /*Console.WriteLine("no block marketdata 1");
                                doFetchTradeNo(client, sessHandle,StartDate,EndDate);
                                Console.WriteLine("no block marketdata 2");
                                doFetchTrade2No(client, sessHandle, StartDate, EndDate);
                                Console.WriteLine("block marketdata 1");
                                #region single table 
                                for (DateTime QueryDate = StartDate; QueryDate <= EndDate; QueryDate = QueryDate.AddDays(1))
                                {
                                    if ((Options & SubscriptionOptions.TRADE) == SubscriptionOptions.TRADE)
                                    {
                                        doFetchTrade(client, sessHandle, QueryDate);
                                    }
                                    if ((Options & SubscriptionOptions.QUOTE) == SubscriptionOptions.QUOTE)
                                    {
                                        doFetchQuote(client, sessHandle, QueryDate);
                                    }
                                    if ((Options & SubscriptionOptions.AMEX_IMBALANCE) == SubscriptionOptions.AMEX_IMBALANCE)
                                    {
                                        doFetchAMEXImbalance(client, sessHandle, QueryDate);
                                    }
                                    if ((Options & SubscriptionOptions.ARCA_IMBALANCE) == SubscriptionOptions.ARCA_IMBALANCE)
                                    {
                                        doFetchARCAImbalance(client, sessHandle, QueryDate);
                                    }
                                    if ((Options & SubscriptionOptions.NYSE_IMBALANCE) == SubscriptionOptions.NYSE_IMBALANCE)
                                    {
                                        doFetchNYSEImbalance(client, sessHandle, QueryDate);
                                    }
                                    if ((Options & SubscriptionOptions.NASD_IMBALANCE) == SubscriptionOptions.NASD_IMBALANCE)
                                    {
                                        doFetchNASDImbalance(client, sessHandle, QueryDate);
                                    }
                                    if ((Options & SubscriptionOptions.DAILY) == SubscriptionOptions.DAILY)
                                    {
                                        doFetchDaily(client, sessHandle, QueryDate);
                                    }

                                }
                                #endregion
                                Console.WriteLine("block marketdata 2");*/

                                #endregion

                                #region single table
                                for (DateTime QueryDate = StartDate; QueryDate <= EndDate; QueryDate = QueryDate.AddDays(1))
                                {
                                    if ((Options & SubscriptionOptions.TRADE) == SubscriptionOptions.TRADE)
                                    {
                                        doFetchTrade2(client, sessHandle, QueryDate, QueryDate.Date==EndDate.Date);
                                    }
                                    if ((Options & SubscriptionOptions.QUOTE) == SubscriptionOptions.QUOTE)
                                    {
                                        doFetchQuote(client, sessHandle, QueryDate, QueryDate.Date == EndDate.Date);
                                    }
                                    if ((Options & SubscriptionOptions.AMEX_IMBALANCE) == SubscriptionOptions.AMEX_IMBALANCE)
                                    {
                                        doFetchAMEXImbalance(client, sessHandle, QueryDate);
                                    }
                                    if ((Options & SubscriptionOptions.ARCA_IMBALANCE) == SubscriptionOptions.ARCA_IMBALANCE)
                                    {
                                        doFetchARCAImbalance(client, sessHandle, QueryDate);
                                    }
                                    if ((Options & SubscriptionOptions.NYSE_IMBALANCE) == SubscriptionOptions.NYSE_IMBALANCE)
                                    {
                                        doFetchNYSEImbalance(client, sessHandle, QueryDate, QueryDate.Date==EndDate.Date);
                                    }
                                    if ((Options & SubscriptionOptions.NASD_IMBALANCE) == SubscriptionOptions.NASD_IMBALANCE)
                                    {
                                        doFetchNASDImbalance(client, sessHandle, QueryDate);
                                    }
                                    if ((Options & SubscriptionOptions.DAILY) == SubscriptionOptions.DAILY)
                                    {
                                        doFetchDaily(client, sessHandle, QueryDate, QueryDate.Date == EndDate.Date);
                                    }
                                    if ((Options & SubscriptionOptions.EXGPRINTS) == SubscriptionOptions.EXGPRINTS)
                                    {
                                        doFetchExgprints(client, sessHandle, QueryDate, QueryDate.Date == EndDate.Date);
                                    }
                                    if ((Options & SubscriptionOptions.NEWS) == SubscriptionOptions.NEWS)
                                    {
                                        doFetchNews(client, sessHandle, QueryDate, QueryDate.Date == EndDate.Date);
                                    }

                                }
                                #endregion
                            }
                            else
                            {
                                #region Union fetchdata

                                for (DateTime QueryDate = StartDate; QueryDate <= EndDate; QueryDate=QueryDate.AddDays(1))
                                {
                                    string query = "";
                                    bool firstUnion = false;
                                    if ((Options & SubscriptionOptions.TRADE) == SubscriptionOptions.TRADE)
                                    {
                                        if (firstUnion == true)
                                        {
                                            query = query + " union all " + createUnionTradeQuery(QueryDate);
                                        }
                                        else
                                        {
                                            query = createUnionTradeQuery(QueryDate);
                                        }
                                        firstUnion = true;
                                    }
                                    if ((Options & SubscriptionOptions.QUOTE) == SubscriptionOptions.QUOTE)
                                    {
                                        if (firstUnion == true)
                                        {
                                            query = query + " union all " + createUnionQuoteQuery(QueryDate);
                                        }
                                        else
                                        {
                                            query = createUnionQuoteQuery(QueryDate);
                                        }
                                        firstUnion = true;

                                    }
                                    if ((Options & SubscriptionOptions.AMEX_IMBALANCE) == SubscriptionOptions.AMEX_IMBALANCE)
                                    {
                                        if (firstUnion == true)
                                        {
                                            query = query + " union all " + createUnionAmexQuery(QueryDate);
                                        }
                                        else
                                        {
                                            query = createUnionAmexQuery(QueryDate);
                                        }
                                        firstUnion = true;
                                    }
                                    if ((Options & SubscriptionOptions.ARCA_IMBALANCE) == SubscriptionOptions.ARCA_IMBALANCE)
                                    {

                                        if (firstUnion == true)
                                        {
                                            query = query + " union all " + createUnionArcaQuery(QueryDate);
                                        }
                                        else
                                        {
                                            query = createUnionArcaQuery(QueryDate);
                                        }
                                        firstUnion = true;
                                    }
                                    if ((Options & SubscriptionOptions.NYSE_IMBALANCE) == SubscriptionOptions.NYSE_IMBALANCE)
                                    {
                                        if (firstUnion == true)
                                        {
                                            query = query + " union all " + createUnionNyseQuery(QueryDate);
                                        }
                                        else
                                        {
                                            query = createUnionNyseQuery(QueryDate);
                                        }
                                        firstUnion = true;
                                    }
                                    if ((Options & SubscriptionOptions.NASD_IMBALANCE) == SubscriptionOptions.NASD_IMBALANCE)
                                    {
                                        if (firstUnion == true)
                                        {
                                            query = query + " union all " + createUnionNasdQuery(QueryDate);
                                        }
                                        else
                                        {
                                            query = createUnionNasdQuery(QueryDate);
                                        }
                                        firstUnion = true;

                                    }
                                    if ((Options & SubscriptionOptions.DAILY) == SubscriptionOptions.DAILY)
                                    {
                                        doFetchDaily(client, sessHandle,QueryDate);
                                    }


                                    query = "select * from ( " + query + " ) log order by log.seqno limit " + Maxrows;
                                  //  query = "select * from ( " + query + " )";
                                    //Console.WriteLine(query);
                                    doFetchUnion(client, sessHandle, query);
                                }
                                #endregion
                            }
                            #endregion
                            //-------------------------------------------------------

                            // close the session/transport
                            var closeReq = new TCloseSessionReq
                            {
                                SessionHandle = sessHandle
                            };
                            client.CloseSession(closeReq);
                            transport.Close();
                        }, cancelToken);

                        return Disposable.Create(tokenSource.Cancel);
                    }).Subscribe(Observer);

                }
                catch (Exception ex)
                {
                    Observer.OnError(ex);
                }
                finally
                {
                    // reset barrier
                    mBarrier.Set();
                }
            }
            return null;
        }
        #endregion

        #region IDisposable
        public void Dispose()
        {
            //TODO cleanup
            mBarrier.Reset();
            if (_cancellationSource != null)
            {
                _cancellationSource.Cancel();
            }
        }
        #endregion

        #region Subscribe Methods

        public Client Subscribe(SubscriptionOptions options, IEnumerable<string> symbols = null)
        {
            Options = options;
            Symbols = symbols;
            return this;
        }

        public Client SubscribeALL(SubscriptionOptions options)
        {
            Options = options;
            Symbols = null;
            return this;
        }
        #endregion

        #region Union Query Methods

        private string createUnionTradeQuery(DateTime QueryDate)
        {
            double st = StartTime.TotalMilliseconds;
            double et = EndTime.TotalMilliseconds;
            string query = "";
            query = "select seqno,day,time, symbol ,'T' as flag,conditionflags,exchangesequence,";
            query = query + "price,open,high,low,last,size,exchangetime,listedexchange,netchange,priceflags,reportingexchange,tradecondition,totalvolume,";
            if (unionType == "TI")
            {
                query = query + "0.0 as clearing_price,0 as exg_specific_info,0 as exg_time,0.0 as far_price,0 as market_imbalance,";
                query = query + "0.0 as near_price,0 as paired_shares,0.0 as ref_price,0 as side,'' as source,0 as total_imbalance,0 as type";
            }
            if (unionType == "TQI")
            {
                query = query + "0.0 as clearing_price,0 as exg_specific_info,0 as exg_time,0.0 as far_price,0 as market_imbalance,";
                query = query + "0.0 as near_price,0 as paired_shares,0.0 as ref_price,0 as side,'' as source,0 as total_imbalance,0 as type,";
                query = query + "0.0 as ask,0 as asksize,0 as bbochangeflags,0.0 as bestask,";
                query = query + "0 as bestaskexchange,0 as bestasksize,0.0 as bestbid,0 as bestbidexchange,0 as bestbidsize,0.0 as bid,0 as bidsize,0 as quotecondition";
            }
            if (unionType == "TQ")
            {
                query = query + "0.0 as ask,0 as asksize,0 as bbochangeflags,0.0 as bestask,";
                query = query + "0 as bestaskexchange,0 as bestasksize,0.0 as bestbid,0 as bestbidexchange,0 as bestbidsize,0.0 as bid,0 as bidsize,0 as quotecondition";
            }
            query = String.Format("{0} from {1}.trades as T where day = {2:yyyyMMdd}", query, db, QueryDate);
            if (StartTime.TotalMilliseconds != 0)
            {
                query = query + " and time >=" + st.ToString();
            }

            if (EndTime.TotalMilliseconds != 0)
            {
                query = query + " and time <=" + et.ToString();
            }
            string symbolArry = "";
            if (Symbols != null)
            {
                foreach (string s in Symbols)
                {
                    symbolArry = symbolArry + "','" + s;
                }
                symbolArry = "(" + symbolArry.Remove(0, 2) + "')";

                query = query + " and symbol in " + symbolArry;

            }
            return query;
        }

        private string createUnionQuoteQuery(DateTime QueryDate)
        {
            double st = StartTime.TotalMilliseconds;
            double et = EndTime.TotalMilliseconds;
            string query = "";
            query = "select seqno,day,time, symbol ,'Q' as flag,";
            if (unionType == "QI")
            {
                query = query + "0.0 as clearing_price,0 as exg_specific_info,0 as exg_time,0.0 as far_price,0 as market_imbalance,0.0 as near_price,";
                query = query + "0 as paired_shares,0.0 as ref_price,0 as side,'' as source,0 as total_imbalance,0 as type,ask,asksize,bbochangeflags,";
                query = query + "bestask,bestaskexchange,bestasksize,bestbid,bestbidexchange,bestbidsize,bid,bidsize,exchangetime,listedexchange,reportingexchange,quotecondition";
            }
            if (unionType == "TQI")
            {
                query = query + "0 as conditionflags,0 as exchangesequence,0.0 as price,0.0 as open,0.0 as high,0.0 as low,0.0 as last,0 as size,exchangetime,listedexchange,";
                query = query + "0.0 as netchange,0 as priceflags,reportingexchange,0 as tradecondition,0 as totalvolume,0.0 as clearing_price,0 as exg_specific_info,";
                query = query + "0 as exg_time,0.0 as far_price,0 as market_imbalance,0.0 as near_price,0 as paired_shares,0.0 as ref_price,0 as side,'' as source,";
                query = query +  "0 as total_imbalance,0 as type,ask,asksize,bbochangeflags,bestask,bestaskexchange,bestasksize,bestbid,bestbidexchange,bestbidsize,bid,bidsize,quotecondition";
            }
            if (unionType == "TQ")
            {
                query = query + "0 as conditionflags,0 as exchangesequence,0.0 as price,0.0 as open,0.0 as high,0.0 as low,0.0 as last,0 as size,exchangetime,listedexchange,";
                query = query + "0.0 as netchange,0 as priceflags,reportingexchange,0 as tradecondition,0 as totalvolume,ask,asksize,bbochangeflags,bestask,bestaskexchange,";
                query = query +  "bestasksize,bestbid,bestbidexchange,bestbidsize,bid,bidsize,quotecondition";
            }
            query = String.Format("{0} from {1}.quotes as Q where day = {2:yyyyMMdd}", query, db, QueryDate);
            if (StartTime.TotalMilliseconds != 0)
            {
                query = query + " and time >=" + st.ToString();
            }

            if (EndTime.TotalMilliseconds != 0)
            {
                query = query + " and time <=" + et.ToString();
            }
            string symbolArry = "";
            if (Symbols != null)
            {
                foreach (string s in Symbols)
                {
                    symbolArry = symbolArry + "','" + s;
                }
                symbolArry = "(" + symbolArry.Remove(0, 2) + "')";

                query = query + " and symbol in " + symbolArry;

            }
            return query;
        }

        private string createUnionNyseQuery(DateTime QueryDate)
        {
            double st = StartTime.TotalMilliseconds;
            double et = EndTime.TotalMilliseconds;
            string query = "";
            query = "select seqno,day,time, symbol ,'NYSE' as flag,";
            if (unionType == "TI")
            {
                query = query + "0 as conditionflags,0 as exchangesequence,0.0 as price,0.0 as open,0.0 as high,0.0 as low,0.0 as last,";
                query = query + "0 as size,0 as exchangetime,0 as listedexchange,0.0 as netchange,0 as priceflags,0 as reportingexchange,0 as tradecondition,0 as totalvolume,";
                query = query +  "clearing_price,exg_specific_info,exg_time,far_price,market_imbalance,near_price,paired_shares,ref_price,side,source,total_imbalance,type ";
            }
            if (unionType == "TQI")
            {
                query = query + "0 as conditionflags,0 as exchangesequence,0.0 as price,0.0 as open,0.0 as high,0.0 as low,0.0 as last,";
                query = query + "0 as size,0 as exchangetime,0 as listedexchange,0.0 as netchange,0 as priceflags,0 as reportingexchange,0 as tradecondition,0 as totalvolume,";
                query = query +   "clearing_price,exg_specific_info,exg_time,far_price,market_imbalance,near_price,paired_shares,ref_price,side,source,total_imbalance,type,";
                query = query + "0.0 as ask,0 as asksize,0 as bbochangeflags,0.0 as bestask,0 as bestaskexchange,0 as bestasksize,0.0 as bestbid,";
                query = query +   "0 as bestbidexchange,0 as bestbidsize,0.0 as bid,0 as bidsize,0 as quotecondition ";
            }
            if (unionType == "QI")
            {
              query = query +  "clearing_price,exg_specific_info,exg_time,far_price,market_imbalance,near_price,paired_shares,ref_price,side,source,total_imbalance,type,";
              query = query +   "0.0 as ask,0 as asksize,0 as bbochangeflags,0.0 as bestask,0 as bestaskexchange,0 as bestasksize,0.0 as bestbid,0 as bestbidexchange,0 as bestbidsize,";
              query = query + "0.0 as bid,0 as bidsize,0 as exchangetime,0 as listedexchange,0 as reportingexchange,0 as quotecondition ";
            }
            if (unionType == "I")
            {
                query = query + "clearing_price,exg_specific_info,exg_time,far_price,market_imbalance,near_price,paired_shares,ref_price,side,source,total_imbalance,type ";
            }
            query = String.Format("{0} from {1}.imb_nyse as I where I.day = {2:yyyyMMdd}", query, db, QueryDate);
            if (StartTime.TotalMilliseconds != 0)
            {
                query = query + " and time >=" + st.ToString();
            }

            if (EndTime.TotalMilliseconds != 0)
            {
                query = query + " and time <=" + et.ToString();
            }
            string symbolArry = "";
            if (Symbols != null)
            {
                foreach (string s in Symbols)
                {
                    symbolArry = symbolArry + "','" + s;
                }
                symbolArry = "(" + symbolArry.Remove(0, 2) + "')";

                query = query + " and symbol in " + symbolArry;

            }
            return query;
        }

        private string createUnionNasdQuery(DateTime QueryDate)
        {
            double st = StartTime.TotalMilliseconds;
            double et = EndTime.TotalMilliseconds;
            string query = "";
            query = "select seqno,day,time, symbol ,'NASD' as flag,";
            if (unionType == "TI")
            {
                query = query + "0 as conditionflags,0 as exchangesequence,0.0 as price,0.0 as open,0.0 as high,0.0 as low,0.0 as last,";
                query = query + "0 as size,0 as exchangetime,0 as listedexchange,0.0 as netchange,0 as priceflags,0 as reportingexchange,0 as tradecondition,0 as totalvolume,";
                query = query + "0.0 as clearing_price,exg_specific_info,exg_time,far_price,market_imbalance,near_price,paired_shares,ref_price,side,source,total_imbalance,type ";
            }
            if (unionType == "TQI")
            {
                query = query + "0 as conditionflags,0 as exchangesequence,0.0 as price,0.0 as open,0.0 as high,0.0 as low,0.0 as last,";
                query = query + "0 as size,0 as exchangetime,0 as listedexchange,0.0 as netchange,0 as priceflags,0 as reportingexchange,0 as tradecondition,0 as totalvolume,";
                query = query + "0.0 as clearing_price,exg_specific_info,exg_time,far_price,market_imbalance,near_price,paired_shares,ref_price,side,source,total_imbalance,type,";
                query = query + "0.0 as ask,0 as asksize,0 as bbochangeflags,0.0 as bestask,0 as bestaskexchange,0 as bestasksize,0.0 as bestbid,";
                query = query + "0 as bestbidexchange,0 as bestbidsize,0.0 as bid,0 as bidsize,0 as quotecondition ";
            }
            if (unionType == "QI")
            {
                query = query + "0.0 as clearing_price,exg_specific_info,exg_time,far_price,market_imbalance,near_price,paired_shares,ref_price,side,source,total_imbalance,type,";
                query = query + "0.0 as ask,0 as asksize,0 as bbochangeflags,0.0 as bestask,0 as bestaskexchange,0 as bestasksize,0.0 as bestbid,0 as bestbidexchange,0 as bestbidsize,";
                query = query + "0.0 as bid,0 as bidsize,0 as exchangetime,0 as listedexchange,0 as reportingexchange,0 as quotecondition ";
            }
            if (unionType == "I")
            {
                query = query + "0.0 as clearing_price,exg_specific_info,exg_time,far_price,market_imbalance,near_price,paired_shares,ref_price,side,source,total_imbalance,type ";
            }
            query = String.Format("{0} from {1}.imb_nasd as I where I.day = {2:yyyyMMdd}", query, db, QueryDate);
            if (StartTime.TotalMilliseconds != 0)
            {
                query = query + " and time >=" + st.ToString();
            }

            if (EndTime.TotalMilliseconds != 0)
            {
                query = query + " and time <=" + et.ToString();
            }
            string symbolArry = "";
            if (Symbols != null)
            {
                foreach (string s in Symbols)
                {
                    symbolArry = symbolArry + "','" + s;
                }
                symbolArry = "(" + symbolArry.Remove(0, 2) + "')";

                query = query + " and symbol in " + symbolArry;

            }
            return query;
        }

        private string createUnionAmexQuery(DateTime QueryDate)
        {
            double st = StartTime.TotalMilliseconds;
            double et = EndTime.TotalMilliseconds;
            string query = "";
            query = "select seqno,day,time, symbol ,'AMEX' as flag,";
            if (unionType == "TI")
            {
                query = query + "0 as conditionflags,0 as exchangesequence,0.0 as price,0.0 as open,0.0 as high,0.0 as low,0.0 as last,";
                query = query + "0 as size,0 as exchangetime,0 as listedexchange,0.0 as netchange,0 as priceflags,0 as reportingexchange,0 as tradecondition,0 as totalvolume,";
                query = query + "clearing_price,exg_specific_info,exg_time,far_price,market_imbalance,near_price,paired_shares,ref_price,side,source,total_imbalance,type ";
            }
            if (unionType == "TQI")
            {
                query = query + "0 as conditionflags,0 as exchangesequence,0.0 as price,0.0 as open,0.0 as high,0.0 as low,0.0 as last,";
                query = query + "0 as size,0 as exchangetime,0 as listedexchange,0.0 as netchange,0 as priceflags,0 as reportingexchange,0 as tradecondition,0 as totalvolume,";
                query = query + "clearing_price,exg_specific_info,exg_time,far_price,market_imbalance,near_price,paired_shares,ref_price,side,source,total_imbalance,type,";
                query = query + "0.0 as ask,0 as asksize,0 as bbochangeflags,0.0 as bestask,0 as bestaskexchange,0 as bestasksize,0.0 as bestbid,";
                query = query + "0 as bestbidexchange,0 as bestbidsize,0.0 as bid,0 as bidsize,0 as quotecondition ";
            }
            if (unionType == "QI")
            {
                query = query + "clearing_price,exg_specific_info,exg_time,far_price,market_imbalance,near_price,paired_shares,ref_price,side,source,total_imbalance,type,";
                query = query + "0.0 as ask,0 as asksize,0 as bbochangeflags,0.0 as bestask,0 as bestaskexchange,0 as bestasksize,0.0 as bestbid,0 as bestbidexchange,0 as bestbidsize,";
                query = query + "0.0 as bid,0 as bidsize,0 as exchangetime,0 as listedexchange,0 as reportingexchange,0 as quotecondition ";
            }
            if (unionType == "I")
            {
                query = query + "clearing_price,exg_specific_info,exg_time,far_price,market_imbalance,near_price,paired_shares,ref_price,side,source,total_imbalance,type ";
            }
            query = String.Format("{0} from {1}.imb_amex as I where I.day = {2:yyyyMMdd}", query, db, QueryDate);
            if (StartTime.TotalMilliseconds != 0)
            {
                query = query + " and time >=" + st.ToString();
            }

            if (EndTime.TotalMilliseconds != 0)
            {
                query = query + " and time <=" + et.ToString();
            }
            string symbolArry = "";
            if (Symbols != null)
            {
                foreach (string s in Symbols)
                {
                    symbolArry = symbolArry + "','" + s;
                }
                symbolArry = "(" + symbolArry.Remove(0, 2) + "')";

                query = query + " and symbol in " + symbolArry;

            }
            return query;
        }

        private string createUnionArcaQuery(DateTime QueryDate)
        {
            double st = StartTime.TotalMilliseconds;
            double et = EndTime.TotalMilliseconds;
            string query = "";
            query = "select seqno,day,time, symbol ,'ARCA' as flag,";
            if (unionType == "TI")
            {
                query = query + "0 as conditionflags,0 as exchangesequence,0.0 as price,0.0 as open,0.0 as high,0.0 as low,0.0 as last,";
                query = query + "0 as size,0 as exchangetime,0 as listedexchange,0.0 as netchange,0 as priceflags,0 as reportingexchange,0 as tradecondition,0 as totalvolume,";
                query = query + "clearing_price,exg_specific_info,exg_time,far_price,market_imbalance,near_price,paired_shares,ref_price,side,source,total_imbalance,type ";
            }
            if (unionType == "TQI")
            {
                query = query + "0 as conditionflags,0 as exchangesequence,0.0 as price,0.0 as open,0.0 as high,0.0 as low,0.0 as last,";
                query = query + "0 as size,0 as exchangetime,0 as listedexchange,0.0 as netchange,0 as priceflags,0 as reportingexchange,0 as tradecondition,0 as totalvolume,";
                query = query + "clearing_price,exg_specific_info,exg_time,far_price,market_imbalance,near_price,paired_shares,ref_price,side,source,total_imbalance,type,";
                query = query + "0.0 as ask,0 as asksize,0 as bbochangeflags,0.0 as bestask,0 as bestaskexchange,0 as bestasksize,0.0 as bestbid,";
                query = query + "0 as bestbidexchange,0 as bestbidsize,0.0 as bid,0 as bidsize,0 as quotecondition ";
            }
            if (unionType == "QI")
            {
                query = query + "clearing_price,exg_specific_info,exg_time,far_price,market_imbalance,near_price,paired_shares,ref_price,side,source,total_imbalance,type,";
                query = query + "0.0 as ask,0 as asksize,0 as bbochangeflags,0.0 as bestask,0 as bestaskexchange,0 as bestasksize,0.0 as bestbid,0 as bestbidexchange,0 as bestbidsize,";
                query = query + "0.0 as bid,0 as bidsize,0 as exchangetime,0 as listedexchange,0 as reportingexchange,0 as quotecondition ";
            }
            if (unionType == "I")
            {
                query = query + "clearing_price,exg_specific_info,exg_time,far_price,market_imbalance,near_price,paired_shares,ref_price,side,source,total_imbalance,type ";
            }
            query = String.Format("{0} from {1}.imb_arca as I where I.day = {2:yyyyMMdd}", query, db, QueryDate);
            if (StartTime.TotalMilliseconds != 0)
            {
                query = query + " and time >=" + st.ToString();
            }

            if (EndTime.TotalMilliseconds != 0)
            {
                query = query + " and time <=" + et.ToString();
            }
            string symbolArry = "";
            if (Symbols != null)
            {
                foreach (string s in Symbols)
                {
                    symbolArry = symbolArry + "','" + s;
                }
                symbolArry = "(" + symbolArry.Remove(0, 2) + "')";

                query = query + " and symbol in " + symbolArry;

            }
            return query;
        }

        #endregion

        #region doFetch Methods

        private void doFetchTradeNo(TCLIService.Client client, TSessionHandle sessHandle, DateTime StartDate,DateTime EndDate)
        {
            #region Construct Query

            double st = StartTime.TotalMilliseconds;
            double et = EndTime.TotalMilliseconds;

            var q = string.Format("select * from {1}.trades where day >= {0:yyyyMMdd} and day <= {2:yyyyMMdd}",
                    StartDate,  db , EndDate);

            string symbolArry = "";
            if (Symbols != null)
            {
                foreach (string s in Symbols)
                {
                    symbolArry = symbolArry + "','" + s;
                }
                symbolArry = "(" + symbolArry.Remove(0, 2) + "')";

                q = q + " and symbol in " + symbolArry;

            }


            if (StartTime.TotalMilliseconds != 0)
            {
                q = q + " and time >=" + st.ToString();
            }

            if (EndTime.TotalMilliseconds != 0)
            {
                q = q + " and time <=" + et.ToString();
            }

            q = q + "  order by day,seqno limit " + Maxrows;

            #endregion

            TExecuteStatementReq execReq = new TExecuteStatementReq
            {
                SessionHandle = sessHandle,
                Statement = q
            };
            var execResp = client.ExecuteStatement(execReq);

            if (execResp.Status.StatusCode == TStatusCode.SUCCESS_STATUS || execResp.Status.StatusCode == TStatusCode.SUCCESS_WITH_INFO_STATUS)
            {



                #region Process Results
                var stmtHandle = execResp.OperationHandle;

                var fetchReq = new TFetchResultsReq
                {
                    OperationHandle = stmtHandle,
                    Orientation = TFetchOrientation.FETCH_NEXT,
                    MaxRows = Maxrows
                };

                var bMore = false;
                var stshow = true;
                var stfetchshow = true;
                double fetchstart = 0.0;
                do
                {
                    if (_cancellationSource.Token.IsCancellationRequested)
                    {
                        return;
                    }
                    var resultsResp = new TFetchResultsResp();
                    if (stshow)
                    {
                        var stbegin = DateTime.Now.TimeOfDay.TotalMilliseconds;
                        resultsResp = client.FetchResults(fetchReq);
                        var stend = DateTime.Now.TimeOfDay.TotalMilliseconds;
                        Console.WriteLine("Marketdata1 running query time: " + ((stend - stbegin) / 1000.0).ToString());
                        stshow = false;
                    }
                    else
                    {
                        resultsResp = client.FetchResults(fetchReq);
                    }
                    var resultSet = resultsResp.Results;
                    bMore = resultsResp.HasMoreRows;
                    if (stfetchshow)
                    {
                        fetchstart = DateTime.Now.TimeOfDay.TotalMilliseconds;
                        stfetchshow = false;
                    }
                    foreach (var row in resultSet.Rows)
                    {
                        TimeSpan t = TimeSpan.FromMilliseconds(row.ColVals[16].I32Val.Value);

                        var data = new Data
                        {
                            type = Data.DataType.TRADE,
                            trade = new API.Tick
                            {
                                Symbol = row.ColVals[2].StringVal.Value,
                                Date = DateTime.ParseExact(row.ColVals[19].I32Val.Value.ToString(), "yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture),
                                Price = (decimal)row.ColVals[3].DoubleVal.Value,
                                Size = (uint)row.ColVals[7].I32Val.Value,
                                Open = (decimal)row.ColVals[9].DoubleVal.Value,
                                High = (decimal)row.ColVals[10].DoubleVal.Value,
                                Low = (decimal)row.ColVals[11].DoubleVal.Value,
                                Last = (decimal)row.ColVals[12].DoubleVal.Value,
                                TotalVolume = (ulong)row.ColVals[14].I64Val.Value,
                                BestBidSize = (uint)row.ColVals[0].I32Val.Value,
                                Time = t

                            }
                        };



                        Observer.OnNext(data); // notify observer...
                    }

                    fetchReq.Orientation = TFetchOrientation.FETCH_NEXT;
                    Observer.Notify((decimal)resultsResp.Results.Rows.Count); /// send a report of the pct of job completed...
                } while (bMore); // We're fetching 100 at a time.
                Console.WriteLine("Marketdata1 fetch data time: " + ((DateTime.Now.TimeOfDay.TotalMilliseconds - fetchstart) / 1000.0).ToString());
                Observer.OnCompleted(); // we're done



                #endregion
            }
            else
            {
                Observer.OnError(new Exception(string.Format("{0}: {1}", execResp.Status.ErrorCode, execResp.Status.ErrorMessage)));
            }
        }

        private void doFetchTrade2No(TCLIService.Client client, TSessionHandle sessHandle, DateTime StartDate, DateTime EndDate)
        {
            #region Construct Query

            double st = StartTime.TotalMilliseconds;
            double et = EndTime.TotalMilliseconds;

            var q = string.Format("select * from {1}2.trades where day >= {0:yyyyMMdd} and day <= {2:yyyyMMdd} ",
                    StartDate, db,EndDate);

            string symbolArry = "";
            if (Symbols != null)
            {
                foreach (string s in Symbols)
                {
                    symbolArry = symbolArry + "','" + s;
                }
                symbolArry = "(" + symbolArry.Remove(0, 2) + "')";

                q = q + " and symbol in " + symbolArry;

            }


            if (StartTime.TotalMilliseconds != 0)
            {
                q = q + " and time >=" + st.ToString();
            }

            if (EndTime.TotalMilliseconds != 0)
            {
                q = q + " and time <=" + et.ToString();
            }

            q = q + "  order by day,seqno limit " + Maxrows;

            #endregion

            TExecuteStatementReq execReq = new TExecuteStatementReq
            {
                SessionHandle = sessHandle,
                Statement = q
            };
            var execResp = client.ExecuteStatement(execReq);

            if (execResp.Status.StatusCode == TStatusCode.SUCCESS_STATUS || execResp.Status.StatusCode == TStatusCode.SUCCESS_WITH_INFO_STATUS)
            {



                #region Process Results
                var stmtHandle = execResp.OperationHandle;

                var fetchReq = new TFetchResultsReq
                {
                    OperationHandle = stmtHandle,
                    Orientation = TFetchOrientation.FETCH_NEXT,
                    MaxRows = Maxrows
                };

                var bMore = false;
                var stshow = true;
                var stfetchshow = true;
                double fetchstart = 0.0;
                do
                {
                    if (_cancellationSource.Token.IsCancellationRequested)
                    {
                        return;
                    }
                    var resultsResp = new TFetchResultsResp();
                    if (stshow)
                    {
                        var stbegin = DateTime.Now.TimeOfDay.TotalMilliseconds;
                        resultsResp = client.FetchResults(fetchReq);
                        var stend = DateTime.Now.TimeOfDay.TotalMilliseconds;
                        Console.WriteLine("Marketdata2 runing quering time: " + ((stend - stbegin) / 1000.0).ToString());
                        stshow = false;
                    }
                    else
                    {
                        resultsResp = client.FetchResults(fetchReq);
                    }
                    var resultSet = resultsResp.Results;
                    bMore = resultsResp.HasMoreRows;
                    if (stfetchshow)
                    {
                        fetchstart = DateTime.Now.TimeOfDay.TotalMilliseconds;
                        stfetchshow = false;
                    }
                    foreach (var row in resultSet.Rows)
                    {
                        TimeSpan t = TimeSpan.FromMilliseconds(row.ColVals[15].I32Val.Value);

                        var data = new Data
                        {
                            type = Data.DataType.TRADE,
                            trade = new API.Tick
                            {
                                Symbol = row.ColVals[1].StringVal.Value,
                                Date = DateTime.ParseExact(row.ColVals[18].I32Val.Value.ToString(), "yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture),
                                Price = (decimal)row.ColVals[2].DoubleVal.Value,
                                Size = (uint)row.ColVals[6].I32Val.Value,
                                Open = (decimal)row.ColVals[8].DoubleVal.Value,
                                High = (decimal)row.ColVals[9].DoubleVal.Value,
                                Low = (decimal)row.ColVals[10].DoubleVal.Value,
                                Last = (decimal)row.ColVals[11].DoubleVal.Value,
                                TotalVolume = (ulong)row.ColVals[13].I64Val.Value,
                                //BestBidSize = (uint)row.ColVals[0].I32Val.Value,
                                Time = t

                            }
                        };



                        Observer.OnNext(data); // notify observer...
                    }

                    fetchReq.Orientation = TFetchOrientation.FETCH_NEXT;
                    Observer.Notify((decimal)resultsResp.Results.Rows.Count); /// send a report of the pct of job completed...
                } while (bMore); // We're fetching 100 at a time.
                Console.WriteLine("Marketdata2 fetch data time: " + ((DateTime.Now.TimeOfDay.TotalMilliseconds - fetchstart) / 1000.0).ToString());

                Observer.OnCompleted(); // we're done



                #endregion
            }
            else
            {
                Observer.OnError(new Exception(string.Format("{0}: {1}", execResp.Status.ErrorCode, execResp.Status.ErrorMessage)));
            }
        }

        private void doFetchTrade(TCLIService.Client client, TSessionHandle sessHandle, DateTime QueryDate)
        {
            #region Construct Query

            double st = StartTime.TotalMilliseconds;
            double et = EndTime.TotalMilliseconds;

            var q = string.Format("select * from {1}.trades where day={0:yyyyMMdd}",
                    QueryDate, db);

            string symbolArry = "";
            if (Symbols != null)
            {
                foreach (string s in Symbols)
                {
                    symbolArry = symbolArry + "','" + s;
                }
                symbolArry = "(" + symbolArry.Remove(0, 2) + "')";

                q = q + " and symbol in " + symbolArry;

            }


            if (StartTime.TotalMilliseconds != 0)
            {
                q = q + " and time >=" + st.ToString();
            }

            if (EndTime.TotalMilliseconds != 0)
            {
                q = q + " and time <=" + et.ToString();
            }

            q = q + "  order by seqno limit " + Maxrows;

            #endregion

            TExecuteStatementReq execReq = new TExecuteStatementReq
            {
                SessionHandle = sessHandle,
                Statement = q
            };
            var execResp = client.ExecuteStatement(execReq);

            if (execResp.Status.StatusCode == TStatusCode.SUCCESS_STATUS || execResp.Status.StatusCode == TStatusCode.SUCCESS_WITH_INFO_STATUS)
            {

                

                #region Process Results
                var stmtHandle = execResp.OperationHandle;

                var fetchReq = new TFetchResultsReq
                {
                    OperationHandle = stmtHandle,
                    Orientation = TFetchOrientation.FETCH_NEXT,
                    MaxRows = Maxrows
                };

                var bMore = false;
                var stshow = true;
                var stfetchshow = true;
                double fetchstart = 0.0;
                do
                {
                    if (_cancellationSource.Token.IsCancellationRequested)
                    {
                        return;
                    }
                    var resultsResp =new TFetchResultsResp();
                    if (stshow)
                    {
                         var stbegin = DateTime.Now.TimeOfDay.TotalMilliseconds;
                         resultsResp = client.FetchResults(fetchReq);
                         var stend = DateTime.Now.TimeOfDay.TotalMilliseconds;
                         Console.WriteLine("Marketdata1 running query time: "+((stend-stbegin)/1000.0).ToString());
                        stshow = false;
                    }
                    else
                    {
                         resultsResp = client.FetchResults(fetchReq);
                    }
                    var resultSet = resultsResp.Results;
                    bMore = resultsResp.HasMoreRows;
                    if (stfetchshow)
                    {
                        fetchstart = DateTime.Now.TimeOfDay.TotalMilliseconds;
                        stfetchshow = false;
                    }
                    foreach (var row in resultSet.Rows)
                    {
                        TimeSpan t = TimeSpan.FromMilliseconds(row.ColVals[16].I32Val.Value);

                        var data = new Data
                        {
                            type = Data.DataType.TRADE,
                            trade = new API.Tick
                            {
                                Symbol = row.ColVals[2].StringVal.Value,
                                Date = DateTime.ParseExact(row.ColVals[19].I32Val.Value.ToString(), "yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture),
                                Price = (decimal)row.ColVals[3].DoubleVal.Value,
                                Size = (uint)row.ColVals[7].I32Val.Value,
                                Open = (decimal)row.ColVals[9].DoubleVal.Value,
                                High = (decimal)row.ColVals[10].DoubleVal.Value,
                                Low = (decimal)row.ColVals[11].DoubleVal.Value,
                                Last = (decimal)row.ColVals[12].DoubleVal.Value,
                                TotalVolume = (ulong)row.ColVals[14].I64Val.Value,
                                BestBidSize = (uint)row.ColVals[0].I32Val.Value,
                                Time = t

                            }
                        };

                      

                        Observer.OnNext(data); // notify observer...
                    }

                    fetchReq.Orientation = TFetchOrientation.FETCH_NEXT;
                    Observer.Notify((decimal)resultsResp.Results.Rows.Count); /// send a report of the pct of job completed...
                } while (bMore); // We're fetching 100 at a time.
                Console.WriteLine("Marketdata1 fetch data time: " + ((DateTime.Now.TimeOfDay.TotalMilliseconds - fetchstart) / 1000.0).ToString());
                Observer.OnCompleted(); // we're done

             

                #endregion
            }
            else
            {
                Observer.OnError(new Exception(string.Format("{0}: {1}", execResp.Status.ErrorCode, execResp.Status.ErrorMessage)));
            }
        }

        private void doFetchTrade2(TCLIService.Client client, TSessionHandle sessHandle, DateTime QueryDate, bool lastQuery = false)
        {
            #region Construct Query

            double st = StartTime.TotalMilliseconds;
            double et = EndTime.TotalMilliseconds;

            var q = string.Format("select * from {1}2.trades where day={0:yyyyMMdd}",
                    QueryDate, db);

            string symbolArry = "";
            if (Symbols != null && Symbols.Count() < 400)
            {
                foreach (string s in Symbols)
                {
                    symbolArry = symbolArry + "','" + s;
                }
                symbolArry = "(" + symbolArry.Remove(0, 2) + "')";

                q = q + " and symbol in " + symbolArry;

            }


            if (StartTime.TotalMilliseconds != 0)
            {
                q = q + " and time >=" + st.ToString();
            }

            if (EndTime.TotalMilliseconds != 0)
            {
                q = q + " and time <=" + et.ToString();
            }

            q = q + "  order by seqno limit " + Maxrows;

            #endregion

            TExecuteStatementReq execReq = new TExecuteStatementReq
            {
                SessionHandle = sessHandle,
                Statement = q
            };
            var execResp = client.ExecuteStatement(execReq);

            if (execResp.Status.StatusCode == TStatusCode.SUCCESS_STATUS || execResp.Status.StatusCode == TStatusCode.SUCCESS_WITH_INFO_STATUS)
            {

                #region Process Results
                var stmtHandle = execResp.OperationHandle;

                var fetchReq = new TFetchResultsReq
                {
                    OperationHandle = stmtHandle,
                    Orientation = TFetchOrientation.FETCH_NEXT,
                    MaxRows = Maxrows
                };

                var bMore = false;
                var stshow = true;
                var stfetchshow = true;
                double fetchstart = 0.0;
                do
                {
                    if (_cancellationSource.Token.IsCancellationRequested)
                    {
                        return;
                    }
                    var resultsResp = new TFetchResultsResp();
                    if (stshow)
                    {
                        var stbegin = DateTime.Now.TimeOfDay.TotalMilliseconds;
                        resultsResp = client.FetchResults(fetchReq);
                        var stend = DateTime.Now.TimeOfDay.TotalMilliseconds;
                        Console.WriteLine("Marketdata2 runing quering time: " + ((stend - stbegin) / 1000.0).ToString());
                        stshow = false;
                    }
                    else
                    {
                        resultsResp = client.FetchResults(fetchReq);
                    }
                    var resultSet = resultsResp.Results;
                    bMore = resultsResp.HasMoreRows;
                    if (stfetchshow)
                    {
                        fetchstart = DateTime.Now.TimeOfDay.TotalMilliseconds;
                        stfetchshow = false;
                    }
                    foreach (var row in resultSet.Rows)
                    {

                        bool symExi = false;

                        string str = row.ColVals[1].StringVal.Value;
                        foreach (var sym in Symbols)
                        {
                            if (str == sym || Symbols.Count() < 400)
                            {
                                symExi = true;
                                break;
                            }
                        }

                        if (symExi)
                        {
                            TimeSpan t = TimeSpan.FromMilliseconds(row.ColVals[15].I32Val.Value);

                            var data = new Data
                            {
                                type = Data.DataType.TRADE,
                                trade = new API.Tick
                                {
                                    Symbol = row.ColVals[1].StringVal.Value,
                                    Date = DateTime.ParseExact(row.ColVals[18].I32Val.Value.ToString(), "yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture),
                                    Price = (decimal)row.ColVals[2].DoubleVal.Value,
                                    Size = (uint)row.ColVals[6].I32Val.Value,
                                    Open = (decimal)row.ColVals[8].DoubleVal.Value,
                                    High = (decimal)row.ColVals[9].DoubleVal.Value,
                                    Low = (decimal)row.ColVals[10].DoubleVal.Value,
                                    Last = (decimal)row.ColVals[11].DoubleVal.Value,
                                    TotalVolume = (ulong)row.ColVals[13].I32Val.Value,
                                    //BestBidSize = (uint)row.ColVals[0].I32Val.Value,
                                    Time = t

                                }
                            };



                            Observer.OnNext(data); // notify observer...
                        }
                    }

                    fetchReq.Orientation = TFetchOrientation.FETCH_NEXT;
                    Observer.Notify((decimal)resultsResp.Results.Rows.Count); /// send a report of the pct of job completed...
                } while (bMore); // We're fetching 100 at a time.
                Console.WriteLine("Marketdata2 fetch data time: " + ((DateTime.Now.TimeOfDay.TotalMilliseconds - fetchstart) / 1000.0).ToString());
                if (lastQuery)
                {
                    Observer.OnCompleted(); // we're done
                }



                #endregion
            }
            else
            {
                Observer.OnError(new Exception(string.Format("{0}: {1}", execResp.Status.ErrorCode, execResp.Status.ErrorMessage)));
            }
        }

        private void doFetchQuote(TCLIService.Client client, TSessionHandle sessHandle, DateTime QueryDate, bool lastQuery = false)
        {
            #region Construct Query
            double st = StartTime.TotalMilliseconds;
            double et = EndTime.TotalMilliseconds;

            var q = string.Format("select * from {1}.quotes where day={0:yyyyMMdd}",
                   QueryDate, db);

            string symbolArry = "";
            if (Symbols != null && Symbols.Count() < 400)
            {
                foreach (string s in Symbols)
                {
                    symbolArry = symbolArry + "','" + s;
                }
                symbolArry = "(" + symbolArry.Remove(0, 2) + "')";

                q = q + " and symbol in " + symbolArry;

            }


            if (StartTime.TotalMilliseconds != 0)
            {
                q = q + " and time >=" + st.ToString();
            }

            if (EndTime.TotalMilliseconds != 0)
            {
                q = q + " and time <=" + et.ToString();
            }

            #endregion

            TExecuteStatementReq execReq = new TExecuteStatementReq
            {
                SessionHandle = sessHandle,
                Statement = q
            };
            var execResp = client.ExecuteStatement(execReq);
            if (execResp.Status.StatusCode == TStatusCode.SUCCESS_STATUS || execResp.Status.StatusCode == TStatusCode.SUCCESS_WITH_INFO_STATUS)
            {

                #region Fetch Results
                var stmtHandle = execResp.OperationHandle;

                var fetchReq = new TFetchResultsReq
                {
                    OperationHandle = stmtHandle,
                    Orientation = TFetchOrientation.FETCH_NEXT,
                    MaxRows = Maxrows
                };

                var bMore = false;
                do
                {

                    var resultsResp = client.FetchResults(fetchReq);
                    var resultSet = resultsResp.Results;
                    bMore = resultsResp.HasMoreRows;
                    int datet = 0;
                    DateTime dateti = DateTime.Now;
                    foreach (var row in resultSet.Rows)
                    {

                        bool symExi = false;

                        string str = row.ColVals[2].StringVal.Value;
                        foreach (var sym in Symbols)
                        {
                            if (str == sym || Symbols.Count() < 400)
                            {
                                symExi = true;
                                break;
                            }
                        }

                        if (symExi)
                        {
                            TimeSpan t = TimeSpan.FromMilliseconds(row.ColVals[16].I32Val.Value);

                            if (datet != row.ColVals[19].I32Val.Value)
                            {
                                datet = row.ColVals[19].I32Val.Value;
                                dateti = DateTime.ParseExact(row.ColVals[19].I32Val.Value.ToString(), "yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture);
                            }

                            var typej = API.Quote.QuoteType.NONE;
                            if (row.ColVals[5].DoubleVal.Value == row.ColVals[8].DoubleVal.Value)
                            { typej = API.Quote.QuoteType.BEST_ASK; }
                            else if (row.ColVals[6].DoubleVal.Value == row.ColVals[9].DoubleVal.Value)
                            { typej = API.Quote.QuoteType.BEST_BID; }


                            string askexch = row.ColVals[11].StringVal.Value;

                            var data = new Data
                            {
                                type = Data.DataType.QUOTE,
                                quote = new API.Quote
                                {
                                    Symbol = row.ColVals[2].StringVal.Value,
                                    Date = dateti,
                                    AskSize = (uint)row.ColVals[3].I32Val.Value,
                                    BidSize = (uint)row.ColVals[4].I32Val.Value,
                                    Ask = (decimal)row.ColVals[5].DoubleVal.Value,
                                    Bid = (decimal)row.ColVals[6].DoubleVal.Value,
                                    AskExchange = Convert.ToChar(row.ColVals[12].I32Val.Value),
                                    BidExchange = Convert.ToChar(row.ColVals[13].I32Val.Value),
                                    Type = typej,
                                    Time = t

                                }

                            };


                            Observer.OnNext(data); // notify observer...
                        }
                    }

                    fetchReq.Orientation = TFetchOrientation.FETCH_NEXT;
                    Observer.Notify(1.0m); /// TODO: send a report of the pct of job completed...
                } while (bMore);
                if (lastQuery)
                {
                    Observer.OnCompleted(); // we're done
                }


                #endregion
            }
            else
            {
                Observer.OnError(new Exception(string.Format("{0}: {1}", execResp.Status.ErrorCode, execResp.Status.ErrorMessage)));
            }
        }

        private void doFetchDaily(TCLIService.Client client, TSessionHandle sessHandle, DateTime QueryDate, bool lastQuery = false)
        {
            #region Construct Query


            var q = string.Format("select * from {1}.daily where day={0:yyyyMMdd}",
                    QueryDate, db);

            string symbolArry = "";
            if (Symbols != null && Symbols.Count()<400)
            {
                foreach (string s in Symbols)
                {
                    symbolArry = symbolArry + "','" + s;
                }
                symbolArry = "(" + symbolArry.Remove(0, 2) + "')";

                q = q + " and symbol in " + symbolArry;

            }

            #endregion

            TExecuteStatementReq execReq = new TExecuteStatementReq
            {
                SessionHandle = sessHandle,
                Statement = q
            };
            var execResp = client.ExecuteStatement(execReq);
            if (execResp.Status.StatusCode == TStatusCode.SUCCESS_STATUS || execResp.Status.StatusCode == TStatusCode.SUCCESS_WITH_INFO_STATUS)
            {
                #region Fetch Results
                var stmtHandle = execResp.OperationHandle;

                var fetchReq = new TFetchResultsReq
                {
                    OperationHandle = stmtHandle,
                    Orientation = TFetchOrientation.FETCH_NEXT,
                    MaxRows = Maxrows
                };

                var bMore = false;
                do
                {
                    var resultsResp = client.FetchResults(fetchReq);

                    var resultSet = resultsResp.Results;
                    bMore = resultsResp.HasMoreRows;
                    int datet = 0;
                    DateTime dateti = DateTime.Now;
                    foreach (var row in resultSet.Rows)
                    {

                        bool symExi = false;

                        string str = row.ColVals[1].StringVal.Value;
                        foreach (var sym in Symbols)
                        {
                            if (str == sym || Symbols.Count() < 400)
                            {
                                symExi = true;
                                break;
                            }
                        }


                        if (symExi)
                        {

                            if (datet != row.ColVals[7].I32Val.Value)
                            {
                                datet = row.ColVals[7].I32Val.Value;
                                dateti = DateTime.ParseExact(row.ColVals[7].I32Val.Value.ToString(), "yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture);
                            }
                            var data = new Data
                            {
                                type = Data.DataType.DAILY,
                                daily = new API.OHLC
                                {
                                    Symbol = row.ColVals[1].StringVal.Value,
                                    Date = dateti,
                                    Open = (decimal)row.ColVals[2].DoubleVal.Value,
                                    High = (decimal)row.ColVals[3].DoubleVal.Value,
                                    Low = (decimal)row.ColVals[4].DoubleVal.Value,
                                    Close = (decimal)row.ColVals[5].DoubleVal.Value,
                                    Volume = (ulong)row.ColVals[6].I32Val.Value,
                                    Time = TimeSpan.Parse("0")


                                }
                            };
                            Observer.OnNext(data); // notify observer...
                        }
                    }

                    fetchReq.Orientation = TFetchOrientation.FETCH_NEXT;
                    Observer.Notify(1.0m); /// TODO: send a report of the pct of job completed...
                } while (bMore);
                if (lastQuery)
                {
                    Observer.OnCompleted(); // we're done
                }
                #endregion
            }
            else
            {
                Observer.OnError(new Exception(string.Format("{0}: {1}", execResp.Status.ErrorCode, execResp.Status.ErrorMessage)));
            }
        }

        private void doFetchNASDImbalance(TCLIService.Client client, TSessionHandle sessHandle, DateTime QueryDate)
        {
            #region Construct Query
            double st = StartTime.TotalMilliseconds;
            double et = EndTime.TotalMilliseconds;

            var q = string.Format("select * from {1}.imb_nasd where imb_nasd.day={0:yyyyMMdd}",
                    QueryDate, db);

            string symbolArry = "";
            if (Symbols != null)
            {
                foreach (string s in Symbols)
                {
                    symbolArry = symbolArry + "','" + s;
                }
                symbolArry = "(" + symbolArry.Remove(0, 2) + "')";

                q = q + " and symbol in " + symbolArry;

            }

            if (StartTime.TotalMilliseconds != 0)
            {
                q = q + " and time >=" + st.ToString();
            }

            if (EndTime.TotalMilliseconds != 0)
            {
                q = q + " and time <=" + et.ToString();
            }
            #endregion

            TExecuteStatementReq execReq = new TExecuteStatementReq
            {
                SessionHandle = sessHandle,
                Statement = q
            };
            var execResp = client.ExecuteStatement(execReq);
            if (execResp.Status.StatusCode == TStatusCode.SUCCESS_STATUS || execResp.Status.StatusCode == TStatusCode.SUCCESS_WITH_INFO_STATUS)
            {
                #region Fetch Results
                var stmtHandle = execResp.OperationHandle;

                var fetchReq = new TFetchResultsReq
                {
                    OperationHandle = stmtHandle,
                    Orientation = TFetchOrientation.FETCH_NEXT,
                    MaxRows = Maxrows
                };

                var bMore = false;
                do
                {
                    var resultsResp = client.FetchResults(fetchReq);

                    var resultSet = resultsResp.Results;
                    bMore = resultsResp.HasMoreRows;
                    int datet = 0;
                    DateTime dateti = DateTime.Now;
                    foreach (var row in resultSet.Rows)
                    {
                        TimeSpan t = TimeSpan.FromMilliseconds((double)row.ColVals[3].I32Val.Value);

                        if (datet != row.ColVals[0].I32Val.Value)
                        {
                            datet = row.ColVals[0].I32Val.Value;
                            dateti = DateTime.ParseExact(row.ColVals[0].I32Val.Value.ToString(), "yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture);
                        }


                        uint buyv = 0;
                        uint sellv = 0;
                        var sidej = API.Imbalance.ImbSide.NONE;
                        if (row.ColVals[4].I32Val.Value == 0)
                        {
                            sidej = API.Imbalance.ImbSide.NONE;
                            buyv = 0;
                            sellv = 0;
                        }
                        else if (row.ColVals[4].I32Val.Value == 1)
                        {
                            sidej = API.Imbalance.ImbSide.BUY;
                            buyv = (uint)row.ColVals[9].I32Val.Value;
                            sellv = 0;
                        }
                        else if (row.ColVals[4].I32Val.Value == 2)
                        {
                            sidej = API.Imbalance.ImbSide.SELL;
                            sellv = (uint)row.ColVals[9].I32Val.Value;
                            buyv = 0;
                        }

                        var typej = API.Imbalance.ImbType.OTHER;
                        if (row.ColVals[5].I32Val.Value == 3)
                        { typej = API.Imbalance.ImbType.OTHER; }
                        else if (row.ColVals[5].I32Val.Value == 1)
                        { typej = API.Imbalance.ImbType.OPEN; }
                        else if (row.ColVals[5].I32Val.Value == 2)
                        { typej = API.Imbalance.ImbType.CLOSE; }


                        var data = new Data
                        {
                            type = Data.DataType.IMBALANCE,
                            imbalance = new API.Imbalance
                            {
                                Symbol = row.ColVals[2].StringVal.Value,
                                Date = dateti,
                                PairedVolume = (uint)row.ColVals[7].I32Val.Value,
                                Type = typej,
                                Time = t,
                                NetImbalance = (uint)row.ColVals[9].I32Val.Value,
                                BuyVolume = buyv,
                                SellVolume = sellv,
                                ReferencePrice = (decimal)row.ColVals[10].DoubleVal.Value,
                                Side = sidej
                            }
                        };
                        Observer.OnNext(data); // notify observer...
                    }
                    fetchReq.Orientation = TFetchOrientation.FETCH_NEXT;
                    Observer.Notify(1.0m); /// TODO: send a report of the pct of job completed...
                } while (bMore);
                Observer.OnCompleted(); // we're done
                #endregion
            }
            else
            {
                Observer.OnError(new Exception(string.Format("{0}: {1}", execResp.Status.ErrorCode, execResp.Status.ErrorMessage)));
            }
        }

        private void doFetchNYSEImbalance(TCLIService.Client client, TSessionHandle sessHandle, DateTime QueryDate, bool lastQuery = false)
        {
            #region Construct Query

            double st = StartTime.TotalMilliseconds;
            double et = EndTime.TotalMilliseconds;

            var q = string.Format("select * from {1}.imb_nyse where day = {0:yyyyMMdd}",
                    QueryDate, db);

            string symbolArry = "";
            if (Symbols != null && Symbols.Count() < 400)
            {
                foreach (string s in Symbols)
                {
                    symbolArry = symbolArry + "','" + s;
                }
                symbolArry = "(" + symbolArry.Remove(0, 2) + "')";

                q = q + " and symbol in " + symbolArry;

            }

            if (StartTime.TotalMilliseconds != 0)
            {
                q = q + " and msofday >=" + st.ToString();
            }

            if (EndTime.TotalMilliseconds != 0)
            {
                q = q + " and msofday <=" + et.ToString();
            }
            #endregion

            TExecuteStatementReq execReq = new TExecuteStatementReq
            {
                SessionHandle = sessHandle,
                Statement = q
            };
            var execResp = client.ExecuteStatement(execReq);
            if (execResp.Status.StatusCode == TStatusCode.SUCCESS_STATUS || execResp.Status.StatusCode == TStatusCode.SUCCESS_WITH_INFO_STATUS)
            {
                #region Fetch Results
                var stmtHandle = execResp.OperationHandle;

                var fetchReq = new TFetchResultsReq
                {
                    OperationHandle = stmtHandle,
                    Orientation = TFetchOrientation.FETCH_NEXT,
                    MaxRows = Maxrows
                };

                var bMore = false;
                do
                {
                    var resultsResp = client.FetchResults(fetchReq);

                    var resultSet = resultsResp.Results;
                    bMore = resultsResp.HasMoreRows;
                    int datet = 0;
                    DateTime dateti = DateTime.Now;
                    foreach (var row in resultSet.Rows)
                    {

                        bool symExi = false;

                        string str = row.ColVals[0].StringVal.Value;
                        foreach (var sym in Symbols)
                        {
                            if (str == sym || Symbols.Count() < 400)
                            {
                                symExi = true;
                                break;
                            }
                        }



                    if(symExi)
                     {
                        TimeSpan t = TimeSpan.FromMilliseconds((double)row.ColVals[1].I32Val.Value);

                        



                        if (datet != row.ColVals[13].I32Val.Value)
                        {
                            datet = row.ColVals[13].I32Val.Value;
                            dateti = DateTime.ParseExact(row.ColVals[13].I32Val.Value.ToString(), "yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture);
                        }

                       // uint buyv = 0;
                       // uint sellv = 0;
                        var sidej = API.Imbalance.ImbSide.NONE;
                        if (row.ColVals[2].ByteVal.Value == 0)
                        {
                            sidej = API.Imbalance.ImbSide.NONE;
                            //buyv = 0;
                            //sellv = 0;
                        }
                        else if (row.ColVals[2].ByteVal.Value == 1)
                        {
                            sidej = API.Imbalance.ImbSide.BUY;
                            //buyv = (uint)row.ColVals[5].I32Val.Value;
                            //sellv = 0;
                        }
                        else if (row.ColVals[2].ByteVal.Value == 2)
                        {
                            sidej = API.Imbalance.ImbSide.SELL;
                            //sellv = (uint)row.ColVals[5].I32Val.Value;
                           // buyv = 0;
                        }

                        var typej = API.Imbalance.ImbType.OTHER;
                        if (row.ColVals[3].ByteVal.Value == 0)
                        { typej = API.Imbalance.ImbType.OTHER; }
                        else if (row.ColVals[3].ByteVal.Value == 1)
                        { typej = API.Imbalance.ImbType.OPEN; }
                        else if (row.ColVals[3].ByteVal.Value == 4)
                        { typej = API.Imbalance.ImbType.CLOSE; }
                        else if (row.ColVals[3].ByteVal.Value == 5)
                        { typej = API.Imbalance.ImbType.NONE; }

                        var data = new Data
                        {
                            type = Data.DataType.IMBALANCE,
                            imbalance = new API.Imbalance
                            {
                                Symbol = row.ColVals[0].StringVal.Value,
                                Date = dateti,
                                PairedVolume = (uint)row.ColVals[5].I32Val.Value,
                                Type = typej,
                                Time = t,
                                NetImbalance = (uint)row.ColVals[7].I32Val.Value,
                                //BuyVolume = buyv,
                                //SellVolume = sellv,
                                ReferencePrice = (decimal)row.ColVals[8].DoubleVal.Value,
                                Side = sidej,
                                ClearingPrice = (decimal)row.ColVals[11].DoubleVal.Value,
                                Tag=row.ColVals[12].ByteVal.Value
                            }

                        };
                        Observer.OnNext(data); // notify observer...
                       }
                    }

                    fetchReq.Orientation = TFetchOrientation.FETCH_NEXT;
                    Observer.Notify(1.0m); /// send a report of the pct of job completed...
                } while (bMore); // We're fetching 100 at a time.

                if (lastQuery)
                {
                    Observer.OnCompleted(); // we're done
                }
                #endregion
            }
            else
            {
                Observer.OnError(new Exception(string.Format("{0}: {1}", execResp.Status.ErrorCode, execResp.Status.ErrorMessage)));
            }
        }

        private void doFetchAMEXImbalance(TCLIService.Client client, TSessionHandle sessHandle, DateTime QueryDate)
        {
            #region Construct Query
            double st = StartTime.TotalMilliseconds;
            double et = EndTime.TotalMilliseconds;

            var q = string.Format("select * from {1}.imb_amex where imb_amex.day={0:yyyyMMdd}",
                    QueryDate, db);

            string symbolArry = "";
            if (Symbols != null)
            {
                foreach (string s in Symbols)
                {
                    symbolArry = symbolArry + "','" + s;
                }
                symbolArry = "(" + symbolArry.Remove(0, 2) + "')";

                q = q + " and symbol in " + symbolArry;

            }

            if (StartTime.TotalMilliseconds != 0)
            {
                q = q + " and time >=" + st.ToString();
            }

            if (EndTime.TotalMilliseconds != 0)
            {
                q = q + " and time <=" + et.ToString();
            }
            #endregion

            TExecuteStatementReq execReq = new TExecuteStatementReq
            {
                SessionHandle = sessHandle,
                Statement = q
            };
            var execResp = client.ExecuteStatement(execReq);
            if (execResp.Status.StatusCode == TStatusCode.SUCCESS_STATUS || execResp.Status.StatusCode == TStatusCode.SUCCESS_WITH_INFO_STATUS)
            {
                #region Fetch Results
                var stmtHandle = execResp.OperationHandle;

                var fetchReq = new TFetchResultsReq
                {
                    OperationHandle = stmtHandle,
                    Orientation = TFetchOrientation.FETCH_NEXT,
                    MaxRows = Maxrows
                };

                var bMore = false;
                do
                {
                    var resultsResp = client.FetchResults(fetchReq);

                    var resultSet = resultsResp.Results;
                    bMore = resultsResp.HasMoreRows;
                    int datet = 0;
                    DateTime dateti = DateTime.Now;
                    foreach (var row in resultSet.Rows)
                    {
                        TimeSpan t = TimeSpan.FromMilliseconds((double)row.ColVals[3].I32Val.Value);

                        if (datet != row.ColVals[0].I32Val.Value)
                        {
                            datet = row.ColVals[0].I32Val.Value;
                            dateti = DateTime.ParseExact(row.ColVals[0].I32Val.Value.ToString(), "yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture);
                        }

                        uint buyv = 0;
                        uint sellv = 0;
                        var sidej = API.Imbalance.ImbSide.NONE;
                        if (row.ColVals[4].I32Val.Value == 0)
                        {
                            sidej = API.Imbalance.ImbSide.NONE;
                            buyv = 0;
                            sellv = 0;
                        }
                        else if (row.ColVals[4].I32Val.Value == 1)
                        {
                            sidej = API.Imbalance.ImbSide.BUY;
                            buyv = (uint)row.ColVals[9].I32Val.Value;
                            sellv = 0;
                        }
                        else if (row.ColVals[4].I32Val.Value == 2)
                        {
                            sidej = API.Imbalance.ImbSide.SELL;
                            sellv = (uint)row.ColVals[9].I32Val.Value;
                            buyv = 0;
                        }

                        var typej = API.Imbalance.ImbType.OTHER;
                        if (row.ColVals[5].I32Val.Value == 3)
                        { typej = API.Imbalance.ImbType.OTHER; }
                        else if (row.ColVals[5].I32Val.Value == 1)
                        { typej = API.Imbalance.ImbType.OPEN; }
                        else if (row.ColVals[5].I32Val.Value == 2)
                        { typej = API.Imbalance.ImbType.CLOSE; }


                        var data = new Data
                        {
                            type = Data.DataType.IMBALANCE,
                            imbalance = new API.Imbalance
                            {
                                Symbol = row.ColVals[2].StringVal.Value,
                                Date = dateti,
                                PairedVolume = (uint)row.ColVals[7].I32Val.Value,
                                Type = typej,
                                Time = t,
                                NetImbalance = (uint)row.ColVals[9].I32Val.Value,
                                BuyVolume = buyv,
                                SellVolume = sellv,
                                ReferencePrice = (decimal)row.ColVals[10].DoubleVal.Value,
                                Side = sidej,
                                ClearingPrice = (decimal)row.ColVals[13].DoubleVal.Value
                            }

                        };
                        Observer.OnNext(data); // notify observer...
                    }

                    fetchReq.Orientation = TFetchOrientation.FETCH_NEXT;
                    Observer.Notify(1.0m); /// TODO: send a report of the pct of job completed...
                } while (bMore);
                Observer.OnCompleted(); // we're done
                #endregion
            }
            else
            {
                Observer.OnError(new Exception(string.Format("{0}: {1}", execResp.Status.ErrorCode, execResp.Status.ErrorMessage)));
            }
        }

        private void doFetchARCAImbalance(TCLIService.Client client, TSessionHandle sessHandle, DateTime QueryDate)
        {
            #region Construct Query
            double st = StartTime.TotalMilliseconds;
            double et = EndTime.TotalMilliseconds;

            var q = string.Format("select * from {1}.imb_arca where imb_arca.day={0:yyyyMMdd}",
                    QueryDate, db);

            string symbolArry = "";
            if (Symbols != null)
            {
                foreach (string s in Symbols)
                {
                    symbolArry = symbolArry + "','" + s;
                }
                symbolArry = "(" + symbolArry.Remove(0, 2) + "')";

                q = q + " and symbol in " + symbolArry;

            }

            if (StartTime.TotalMilliseconds != 0)
            {
                q = q + " and time >=" + st.ToString();
            }

            if (EndTime.TotalMilliseconds != 0)
            {
                q = q + " and time <=" + et.ToString();
            }
            #endregion

            TExecuteStatementReq execReq = new TExecuteStatementReq
            {
                SessionHandle = sessHandle,
                Statement = q
            };
            var execResp = client.ExecuteStatement(execReq);
            if (execResp.Status.StatusCode == TStatusCode.SUCCESS_STATUS || execResp.Status.StatusCode == TStatusCode.SUCCESS_WITH_INFO_STATUS)
            {
                #region Fetch Results
                var stmtHandle = execResp.OperationHandle;

                var fetchReq = new TFetchResultsReq
                {
                    OperationHandle = stmtHandle,
                    Orientation = TFetchOrientation.FETCH_NEXT,
                    MaxRows = Maxrows
                };

                var bMore = false;
                do
                {
                    var resultsResp = client.FetchResults(fetchReq);

                    var resultSet = resultsResp.Results;
                    bMore = resultsResp.HasMoreRows;
                    int datet = 0;
                    DateTime dateti = DateTime.Now;
                    foreach (var row in resultSet.Rows)
                    {
                        TimeSpan t = TimeSpan.FromMilliseconds((double)row.ColVals[3].I32Val.Value);

                        if (datet != row.ColVals[0].I32Val.Value)
                        {
                            datet = row.ColVals[0].I32Val.Value;
                            dateti = DateTime.ParseExact(row.ColVals[0].I32Val.Value.ToString(), "yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture);
                        }
                        uint buyv = 0;
                        uint sellv = 0;
                        var sidej = API.Imbalance.ImbSide.NONE;
                        if (row.ColVals[4].I32Val.Value == 0)
                        {
                            sidej = API.Imbalance.ImbSide.NONE;
                            buyv = 0;
                            sellv = 0;
                        }
                        else if (row.ColVals[4].I32Val.Value == 1)
                        {
                            sidej = API.Imbalance.ImbSide.BUY;
                            buyv = (uint)row.ColVals[9].I32Val.Value;
                            sellv = 0;
                        }
                        else if (row.ColVals[4].I32Val.Value == 2)
                        {
                            sidej = API.Imbalance.ImbSide.SELL;
                            sellv = (uint)row.ColVals[9].I32Val.Value;
                            buyv = 0;
                        }

                        var typej = API.Imbalance.ImbType.OTHER;
                        if (row.ColVals[5].I32Val.Value == 3)
                        { typej = API.Imbalance.ImbType.OTHER; }
                        else if (row.ColVals[5].I32Val.Value == 1)
                        { typej = API.Imbalance.ImbType.OPEN; }
                        else if (row.ColVals[5].I32Val.Value == 2)
                        { typej = API.Imbalance.ImbType.CLOSE; }


                        var data = new Data
                        {
                            type = Data.DataType.IMBALANCE,
                            imbalance = new API.Imbalance
                            {
                                Symbol = row.ColVals[2].StringVal.Value,
                                Date = dateti,
                                PairedVolume = (uint)row.ColVals[7].I32Val.Value,
                                Type = typej,
                                Time = t,
                                NetImbalance = (uint)row.ColVals[9].I32Val.Value,
                                BuyVolume = buyv,
                                SellVolume = sellv,
                                ReferencePrice = (decimal)row.ColVals[10].DoubleVal.Value,
                                Side = sidej,
                                ClearingPrice = (decimal)row.ColVals[13].DoubleVal.Value
                            }

                        };
                        Observer.OnNext(data); // notify observer...
                    }

                    fetchReq.Orientation = TFetchOrientation.FETCH_NEXT;
                    Observer.Notify(1.0m); /// TODO: send a report of the pct of job completed...
                } while (bMore);
                Observer.OnCompleted(); // we're done
                #endregion
            }
            else
            {
                Observer.OnError(new Exception(string.Format("{0}: {1}", execResp.Status.ErrorCode, execResp.Status.ErrorMessage)));
            }
        }

        private void doFetchNews(TCLIService.Client client, TSessionHandle sessHandle, DateTime QueryDate, bool lastQuery = false)
        {
            #region Construct Query

            double st = StartTime.TotalMilliseconds;
            double et = EndTime.TotalMilliseconds;

            var q = string.Format("select * from {1}.news where day={0:yyyyMMdd}",
                    QueryDate, db);

            string symbolArry = "";
            if (Symbols != null && Symbols.Count() < 400)
            {
                foreach (string s in Symbols)
                {
                    symbolArry = symbolArry + "','" + s;
                }
                symbolArry = "(" + symbolArry.Remove(0, 2) + "')";

                q = q + " and symbol in " + symbolArry;

            }


               if (StartTime.TotalMilliseconds != 0)
               {
                   q = q + " and msofday >= " + st.ToString();
               }

               if (EndTime.TotalMilliseconds != 0)
               {
                   q = q + " and msofday <= " + et.ToString();
               }

            // q = q + "  order by timestamp limit " + Maxrows;

            #endregion

            TExecuteStatementReq execReq = new TExecuteStatementReq
            {
                SessionHandle = sessHandle,
                Statement = q
            };
            var execResp = client.ExecuteStatement(execReq);

            if (execResp.Status.StatusCode == TStatusCode.SUCCESS_STATUS || execResp.Status.StatusCode == TStatusCode.SUCCESS_WITH_INFO_STATUS)
            {
                #region Process Results
                var stmtHandle = execResp.OperationHandle;

                var fetchReq = new TFetchResultsReq
                {
                    OperationHandle = stmtHandle,
                    Orientation = TFetchOrientation.FETCH_NEXT,
                    MaxRows = Maxrows
                };

                var bMore = false;
                var stshow = true;
                var stfetchshow = true;
                double fetchstart = 0.0;
                do
                {
                    if (_cancellationSource.Token.IsCancellationRequested)
                    {
                        return;
                    }
                    var resultsResp = new TFetchResultsResp();
                    if (stshow)
                    {
                        var stbegin = DateTime.Now.TimeOfDay.TotalMilliseconds;
                        resultsResp = client.FetchResults(fetchReq);
                        var stend = DateTime.Now.TimeOfDay.TotalMilliseconds;
                        Console.WriteLine("Marketdata1 running query time: " + ((stend - stbegin) / 1000.0).ToString());
                        stshow = false;
                    }
                    else
                    {
                        resultsResp = client.FetchResults(fetchReq);
                    }
                    var resultSet = resultsResp.Results;
                    bMore = resultsResp.HasMoreRows;
                    if (stfetchshow)
                    {
                        fetchstart = DateTime.Now.TimeOfDay.TotalMilliseconds;
                        stfetchshow = false;
                    }
                    foreach (var row in resultSet.Rows)
                    {
                       

                        bool symExi = false;

                        string str = row.ColVals[1].StringVal.Value;
                        foreach (var sym in Symbols)
                        {
                            if (str == sym || Symbols.Count() < 400)
                            {
                                symExi = true;
                                break;
                            }
                        }
                        if (symExi)
                        {
                            TimeSpan t = TimeSpan.FromMilliseconds(row.ColVals[2].I32Val.Value);

                        var data = new Data
                        {
                            type = Data.DataType.NEWS,
                            news = new API.News
                            {
                                Symbol = row.ColVals[1].StringVal.Value,
                                Day = DateTime.ParseExact(row.ColVals[9].I32Val.Value.ToString(), "yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture),
                                Seqno = (uint)row.ColVals[0].I32Val.Value,
                                Category=row.ColVals[3].StringVal.Value,
                                Source=row.ColVals[4].StringVal.Value,
                                Headline=Regex.Replace(row.ColVals[5].StringVal.Value,"\"",""),
                                Resourceid=row.ColVals[6].StringVal.Value,
                                Story = Regex.Replace(row.ColVals[7].StringVal.Value, "\"", ""),
                                Tags = Regex.Replace(row.ColVals[8].StringVal.Value, "\"", ""),
                                Msofday = t

                            }
                        };
                        Observer.OnNext(data); // notify observer...
                        }
                    }

                    fetchReq.Orientation = TFetchOrientation.FETCH_NEXT;
                    Observer.Notify((decimal)resultsResp.Results.Rows.Count); /// send a report of the pct of job completed...
                } while (bMore); // We're fetching 100 at a time.
                Console.WriteLine("Marketdata1 fetch data time: " + ((DateTime.Now.TimeOfDay.TotalMilliseconds - fetchstart) / 1000.0).ToString());
                if (lastQuery)
                {
                    Observer.OnCompleted(); // we're done
                }
                #endregion
            }
            else
            {
                Observer.OnError(new Exception(string.Format("{0}: {1}", execResp.Status.ErrorCode, execResp.Status.ErrorMessage)));
            }
        }

        private void doFetchExgprints(TCLIService.Client client, TSessionHandle sessHandle, DateTime QueryDate, bool lastQuery = false)
        {
            #region Construct Query

            double st = StartTime.TotalMilliseconds;
            double et = EndTime.TotalMilliseconds;

            var q = string.Format("select * from {1}.exgprints where day={0:yyyyMMdd}",
                    QueryDate, db);

            string symbolArry = "";
            if (Symbols != null && Symbols.Count()<400)
            {
                foreach (string s in Symbols)
                {
                    symbolArry = symbolArry + "','" + s;
                }
                symbolArry = "(" + symbolArry.Remove(0, 2) + "')";

                q = q + " and symbol in " + symbolArry;

            }

            #endregion

            TExecuteStatementReq execReq = new TExecuteStatementReq
            {
                SessionHandle = sessHandle,
                Statement = q
            };
            var execResp = client.ExecuteStatement(execReq);

            if (execResp.Status.StatusCode == TStatusCode.SUCCESS_STATUS || execResp.Status.StatusCode == TStatusCode.SUCCESS_WITH_INFO_STATUS)
            {

                #region test order file
                /*FileStream fs = new FileStream("Exgprints.csv", FileMode.OpenOrCreate);
                StreamWriter m_streamWriter = new StreamWriter(fs);*/
                #endregion

                #region Process Results
                var stmtHandle = execResp.OperationHandle;

                var fetchReq = new TFetchResultsReq
                {
                    OperationHandle = stmtHandle,
                    Orientation = TFetchOrientation.FETCH_NEXT,
                    MaxRows = Maxrows
                };

                var bMore = false;
                var stshow = true;
                var stfetchshow = true;
                double fetchstart = 0.0;
                do
                {
                    if (_cancellationSource.Token.IsCancellationRequested)
                    {
                        return;
                    }
                    var resultsResp =new TFetchResultsResp();
                    if (stshow)
                    {
                         var stbegin = DateTime.Now.TimeOfDay.TotalMilliseconds;
                         resultsResp = client.FetchResults(fetchReq);
                         var stend = DateTime.Now.TimeOfDay.TotalMilliseconds;
                         Console.WriteLine("Marketdata1 running query time: "+((stend-stbegin)/1000.0).ToString());
                        stshow = false;
                    }
                    else
                    {
                         resultsResp = client.FetchResults(fetchReq);
                    }
                    var resultSet = resultsResp.Results;
                    bMore = resultsResp.HasMoreRows;
                    if (stfetchshow)
                    {
                        fetchstart = DateTime.Now.TimeOfDay.TotalMilliseconds;
                        stfetchshow = false;
                    }

                    #region Dic research
                    /*  var DicSym = new Dictionary<string, int>();
                    foreach (var symbol in Symbols)
                    {
                        DicSym.Add(symbol, 0);
                    }*/
                    #endregion

                    foreach (var row in resultSet.Rows)
                    {
                      

                     
                        bool symExi = false;

                        #region Dic Research
                        // symExi = DicSym.ContainsKey(row.ColVals[0].StringVal.Value);
                        #endregion

                        string str = row.ColVals[0].StringVal.Value;
                        foreach (var sym in Symbols)
                        {
                            if (str == sym || Symbols.Count()<400)
                            {
                                symExi = true;
                                break;
                            }
                        }
                      
                        if(symExi)
                        {
                            TimeSpan t = TimeSpan.FromMilliseconds(row.ColVals[1].I32Val.Value);
                          var data = new Data
                          {
                            type = Data.DataType.EXGPRINTS,
                            exgPrints = new API.Exgprints
                            {
                                Symbol = row.ColVals[0].StringVal.Value,
                                Date = DateTime.ParseExact(row.ColVals[7].I32Val.Value.ToString(), "yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture),
                                Price = (decimal)row.ColVals[5].DoubleVal.Value,
                                Size = (uint)row.ColVals[6].I32Val.Value,
                                ListedExchange = (short)row.ColVals[2].I32Val.Value,
                                ReportingExchange = (short)row.ColVals[3].I32Val.Value,
                                printtype = (short)row.ColVals[4].I32Val.Value,
                                Time = t

                            }
                         };

                          #region test oreder file
                     /*   m_streamWriter.WriteLine(data.exgPrints.Symbol + ","
                            + data.exgPrints.Date.ToShortDateString() + ","
                            + data.exgPrints.Time.ToString() + "," 
                            + data.exgPrints.Price.ToString() + ","
                            + data.exgPrints.Size.ToString() + "," 
                            + data.exgPrints.ListedExchange.ToString() + "," 
                            + data.exgPrints.printtype.ToString() + "," 
                            + data.exgPrints.ReportingExchange.ToString());*/
                        #endregion

                          Observer.OnNext(data); // notify observer...
                        }
                    }

                    fetchReq.Orientation = TFetchOrientation.FETCH_NEXT;
                    Observer.Notify((decimal)resultsResp.Results.Rows.Count); /// send a report of the pct of job completed...
                } while (bMore); // We're fetching 100 at a time.
                Console.WriteLine("Marketdata1 fetch data time: " + ((DateTime.Now.TimeOfDay.TotalMilliseconds - fetchstart) / 1000.0).ToString());
                if (lastQuery)
                {
                    Observer.OnCompleted(); // we're done
                }

                #region test oder file
              /*  m_streamWriter.Close();
                m_streamWriter.Dispose();
                fs.Close();
                fs.Dispose();*/
                #endregion

                #endregion
            }
            else
            {
                Observer.OnError(new Exception(string.Format("{0}: {1}", execResp.Status.ErrorCode, execResp.Status.ErrorMessage)));
            }
        }

        private void doFetchShowDate(TCLIService.Client client, TSessionHandle sessHandle)
        {
            #region Construct Query
            string table = "";
            switch (Tablesource)
            {
                case Table.DAILY:
                    {
                        table = db + ".daily";
                    }
                    break;
                case Table.QUOTE:
                    {
                        table = db + ".quotes";
                    }
                    break;
                case Table.TRADE:
                    {
                        table = db + ".trades";
                    }
                    break;
                case Table.AMEX_IMB:
                    {
                        table = db + ".imb_amex";
                    }
                    break;
                case Table.ARCA_IMB:
                    {
                        table = db + ".imb_arca";
                    }
                    break;
                case Table.NASD_IMB:
                    {
                        table = db + ".imb_nasd";
                    }
                    break;
                case Table.NYSE_IMB:
                    {
                        table = db + ".imb_nyse";
                    }
                    break;

            }



            var q = string.Format("show partitions {0}",
                    table);


            #endregion

            TExecuteStatementReq execReq = new TExecuteStatementReq
            {
                SessionHandle = sessHandle,
                Statement = q
            };
            var execResp = client.ExecuteStatement(execReq);
            if (execResp.Status.StatusCode == TStatusCode.SUCCESS_STATUS || execResp.Status.StatusCode == TStatusCode.SUCCESS_WITH_INFO_STATUS)
            {
                #region Process Results
                var stmtHandle = execResp.OperationHandle;

                var fetchReq = new TFetchResultsReq
                {
                    OperationHandle = stmtHandle,
                    Orientation = TFetchOrientation.FETCH_NEXT,
                    MaxRows = Maxrows
                };

                var bMore = false;
                do
                {
                    var resultsResp = client.FetchResults(fetchReq);

                    var resultSet = resultsResp.Results;
                    bMore = resultsResp.HasMoreRows;
                    foreach (var row in resultSet.Rows)
                    {
                        // Construct the object

                        var data = new Data
                        {
                            type = Data.DataType.PARTITIONS,
                            dates = new Data.Partitions
                            {
                                Dates = row.ColVals[0].StringVal.Value
                            }
                        };
                        Observer.OnNext(data); // notify observer...
                    }

                    fetchReq.Orientation = TFetchOrientation.FETCH_NEXT;
                    Observer.Notify(1.0m); /// TODO: send a report of the pct of job completed...
                } while (bMore);
                Observer.OnCompleted(); // we're done
                #endregion
            }
            else
            {
                Observer.OnError(new Exception(string.Format("{0}: {1}", execResp.Status.ErrorCode, execResp.Status.ErrorMessage)));
            }
        }

        private void doFetchUnion(TCLIService.Client client, TSessionHandle sessHandle, string query)
        {
          

            TExecuteStatementReq execReq = new TExecuteStatementReq
            {
                SessionHandle = sessHandle,
                Statement = query
            };
            var execResp = client.ExecuteStatement(execReq);

            if (execResp.Status.StatusCode == TStatusCode.SUCCESS_STATUS || execResp.Status.StatusCode == TStatusCode.SUCCESS_WITH_INFO_STATUS)
            {
                #region test order file
                 FileStream fs = new FileStream("KMB.csv", FileMode.OpenOrCreate);
                 StreamWriter m_streamWriter = new StreamWriter(fs);
                #endregion

                #region Process Results

                var stmtHandle = execResp.OperationHandle;

                var fetchReq = new TFetchResultsReq
                {
                    OperationHandle = stmtHandle,
                    Orientation = TFetchOrientation.FETCH_NEXT,
                    MaxRows = Maxrows
                };

                var bMore = false;
                do
                {
                    if (_cancellationSource.Token.IsCancellationRequested)
                    {
                        return;
                    }
                    var resultsResp = client.FetchResults(fetchReq);
                    var resultSet = resultsResp.Results;
                    bMore = resultsResp.HasMoreRows;
                    int datet = 0;
                    DateTime dateti = DateTime.Now;

                    #region fetch union data

                    foreach (var row in resultSet.Rows)
                    {
                        #region TQI 

                        if (unionType == "TQI")
                        {
                            #region T
                            if (row.ColVals[4].StringVal.Value == "T")
                            {
                                TimeSpan t = TimeSpan.FromMilliseconds(row.ColVals[1].DoubleVal.Value);

                                var data = new Data
                                {
                                    type = Data.DataType.TRADE,
                                    trade = new API.Tick
                                    {
                                        Symbol = row.ColVals[2].StringVal.Value,
                                        Date = DateTime.ParseExact(row.ColVals[0].I32Val.Value.ToString(), "yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture),
                                        Price = (decimal)row.ColVals[6].DoubleVal.Value,
                                        Size = (uint)row.ColVals[11].I32Val.Value,
                                        Open = (decimal)row.ColVals[7].DoubleVal.Value,
                                        High = (decimal)row.ColVals[8].DoubleVal.Value,
                                        Low = (decimal)row.ColVals[9].DoubleVal.Value,
                                        Last = (decimal)row.ColVals[10].DoubleVal.Value,
                                        TotalVolume = (ulong)row.ColVals[18].I64Val.Value,
                                        Time = t
                                    }
                                };
                                Observer.OnNext(data); // notify observer...
                            }
                            #endregion

                            #region Q

                            else if (row.ColVals[4].StringVal.Value == "Q")
                             {
                                 TimeSpan t = TimeSpan.FromMilliseconds(row.ColVals[1].DoubleVal.Value);

                                 if (datet != row.ColVals[0].I32Val.Value)
                                 {
                                  datet = row.ColVals[0].I32Val.Value;
                                  dateti = DateTime.ParseExact(row.ColVals[0].I32Val.Value.ToString(), "yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture);
                                 }

                                  var typej = API.Quote.QuoteType.NONE;
                                  if (row.ColVals[31].DoubleVal.Value == row.ColVals[34].DoubleVal.Value)
                                  { 
                                      typej = API.Quote.QuoteType.BEST_ASK; 
                                  }
                                  else if (row.ColVals[40].DoubleVal.Value == row.ColVals[37].DoubleVal.Value)
                                  { 
                                      typej = API.Quote.QuoteType.BEST_BID; 
                                  }
                                // string askexch = row.ColVals[11].StringVal.Value;

                                 var data = new Data
                                 {
                                  type = Data.DataType.QUOTE,
                                  quote = new API.Quote
                                   {
                                     Symbol = row.ColVals[2].StringVal.Value,
                                     Date = dateti,
                                     AskSize = (uint)row.ColVals[32].I32Val.Value,
                                     BidSize = (uint)row.ColVals[41].I32Val.Value,
                                     Ask = (decimal)row.ColVals[31].DoubleVal.Value,
                                     Bid = (decimal)row.ColVals[40].DoubleVal.Value,
                                     AskExchange = row.ColVals[35].StringVal.Value[0],
                                     BidExchange = row.ColVals[38].StringVal.Value[0],
                                     Type = typej,
                                     Time = t
                                    }
                                  };
                                 Observer.OnNext(data); // notify observer...
                             }

                            #endregion

                            #region I

                            else
                            {
                                TimeSpan t = TimeSpan.FromMilliseconds(row.ColVals[1].DoubleVal.Value);

                                if (datet != row.ColVals[0].I32Val.Value)
                                {
                                    datet = row.ColVals[0].I32Val.Value;
                                    dateti = DateTime.ParseExact(row.ColVals[0].I32Val.Value.ToString(), "yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture);
                                }

                                uint buyv = 0;
                                uint sellv = 0;
                                var sidej = API.Imbalance.ImbSide.NONE;
                                if (row.ColVals[27].I32Val.Value == 0)
                                {
                                    sidej = API.Imbalance.ImbSide.NONE;
                                    buyv = 0;
                                    sellv = 0;
                                }
                                else if (row.ColVals[27].I32Val.Value == 1)
                                {
                                    sidej = API.Imbalance.ImbSide.BUY;
                                    buyv = (uint)row.ColVals[29].I32Val.Value;
                                    sellv = 0;
                                }
                                else if (row.ColVals[27].I32Val.Value == 2)
                                {
                                    sidej = API.Imbalance.ImbSide.SELL;
                                    sellv = (uint)row.ColVals[29].I32Val.Value;
                                    buyv = 0;
                                }

                                var typej = API.Imbalance.ImbType.OTHER;
                                if (row.ColVals[30].I32Val.Value == 3)
                                { typej = API.Imbalance.ImbType.OTHER; }
                                else if (row.ColVals[30].I32Val.Value == 1)
                                { typej = API.Imbalance.ImbType.OPEN; }
                                else if (row.ColVals[30].I32Val.Value == 2)
                                { typej = API.Imbalance.ImbType.CLOSE; }

                                var data = new Data
                                {
                                    type = Data.DataType.IMBALANCE,
                                    imbalance = new API.Imbalance
                                    {
                                        Symbol = row.ColVals[2].StringVal.Value,
                                        Date = dateti,
                                        PairedVolume = (uint)row.ColVals[25].I32Val.Value,
                                        Type = typej,
                                        Time = t,
                                        NetImbalance = (uint)row.ColVals[29].I32Val.Value,
                                        BuyVolume = buyv,
                                        SellVolume = sellv,
                                        ReferencePrice = (decimal)row.ColVals[26].DoubleVal.Value,
                                        Side = sidej,
                                        ClearingPrice = (decimal)row.ColVals[19].DoubleVal.Value
                                    }

                                };
                                Observer.OnNext(data); // notify observer...
                            }
                            #endregion
                        }

                        #endregion

                        #region TI

                        if (unionType == "TI")
                        {
                            #region T

                            if (row.ColVals[4].StringVal.Value == "T")
                            {
                                TimeSpan t = TimeSpan.FromMilliseconds(row.ColVals[1].DoubleVal.Value);

                                var data = new Data
                                {
                                    type = Data.DataType.TRADE,
                                    trade = new API.Tick
                                    {
                                        Symbol = row.ColVals[2].StringVal.Value,
                                        Date = DateTime.ParseExact(row.ColVals[0].I32Val.Value.ToString(), "yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture),
                                        Price = (decimal)row.ColVals[6].DoubleVal.Value,
                                        Size = (uint)row.ColVals[11].I32Val.Value,
                                        Open = (decimal)row.ColVals[7].DoubleVal.Value,
                                        High = (decimal)row.ColVals[8].DoubleVal.Value,
                                        Low = (decimal)row.ColVals[9].DoubleVal.Value,
                                        Last = (decimal)row.ColVals[10].DoubleVal.Value,
                                        TotalVolume = (ulong)row.ColVals[18].I64Val.Value,
                                        Time = t
                                    }
                                };
                                Observer.OnNext(data); // notify observer...
                            }
                            #endregion

                            #region I

                            else
                            {
                                TimeSpan t = TimeSpan.FromMilliseconds(row.ColVals[1].DoubleVal.Value);

                                if (datet != row.ColVals[0].I32Val.Value)
                                {
                                    datet = row.ColVals[0].I32Val.Value;
                                    dateti = DateTime.ParseExact(row.ColVals[0].I32Val.Value.ToString(), "yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture);
                                }

                                uint buyv = 0;
                                uint sellv = 0;
                                var sidej = API.Imbalance.ImbSide.NONE;
                                if (row.ColVals[27].I32Val.Value == 0)
                                {
                                    sidej = API.Imbalance.ImbSide.NONE;
                                    buyv = 0;
                                    sellv = 0;
                                }
                                else if (row.ColVals[27].I32Val.Value == 1)
                                {
                                    sidej = API.Imbalance.ImbSide.BUY;
                                    buyv = (uint)row.ColVals[29].I32Val.Value;
                                    sellv = 0;
                                }
                                else if (row.ColVals[27].I32Val.Value == 2)
                                {
                                    sidej = API.Imbalance.ImbSide.SELL;
                                    sellv = (uint)row.ColVals[29].I32Val.Value;
                                    buyv = 0;
                                }

                                var typej = API.Imbalance.ImbType.OTHER;
                                if (row.ColVals[30].I32Val.Value == 3)
                                { typej = API.Imbalance.ImbType.OTHER; }
                                else if (row.ColVals[30].I32Val.Value == 1)
                                { typej = API.Imbalance.ImbType.OPEN; }
                                else if (row.ColVals[30].I32Val.Value == 2)
                                { typej = API.Imbalance.ImbType.CLOSE; }

                                var data = new Data
                                {
                                    type = Data.DataType.IMBALANCE,
                                    imbalance = new API.Imbalance
                                    {
                                        Symbol = row.ColVals[2].StringVal.Value,
                                        Date = dateti,
                                        PairedVolume = (uint)row.ColVals[25].I32Val.Value,
                                        Type = typej,
                                        Time = t,
                                        NetImbalance = (uint)row.ColVals[29].I32Val.Value,
                                        BuyVolume = buyv,
                                        SellVolume = sellv,
                                        ReferencePrice = (decimal)row.ColVals[26].DoubleVal.Value,
                                        Side = sidej,
                                        ClearingPrice = (decimal)row.ColVals[19].DoubleVal.Value
                                    }

                                };
                                Observer.OnNext(data); // notify observer...
                            }
                            #endregion

                        }

                        #endregion

                        #region QI

                        if (unionType == "QI")
                        {
                            #region Q

                            if (row.ColVals[4].StringVal.Value == "Q")
                            {
                                TimeSpan t = TimeSpan.FromMilliseconds(row.ColVals[1].DoubleVal.Value);

                                if (datet != row.ColVals[0].I32Val.Value)
                                {
                                    datet = row.ColVals[0].I32Val.Value;
                                    dateti = DateTime.ParseExact(row.ColVals[0].I32Val.Value.ToString(), "yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture);
                                }

                                var typej = API.Quote.QuoteType.NONE;
                                if (row.ColVals[16].DoubleVal.Value == row.ColVals[19].DoubleVal.Value)
                                {
                                    typej = API.Quote.QuoteType.BEST_ASK;
                                }
                                else if (row.ColVals[25].DoubleVal.Value == row.ColVals[22].DoubleVal.Value)
                                {
                                    typej = API.Quote.QuoteType.BEST_BID;
                                }
                                // string askexch = row.ColVals[11].StringVal.Value;

                                var data = new Data
                                {
                                    type = Data.DataType.QUOTE,
                                    quote = new API.Quote
                                    {
                                        Symbol = row.ColVals[2].StringVal.Value,
                                        Date = dateti,
                                        AskSize = (uint)row.ColVals[17].I32Val.Value,
                                        BidSize = (uint)row.ColVals[26].I32Val.Value,
                                        Ask = (decimal)row.ColVals[16].DoubleVal.Value,
                                        Bid = (decimal)row.ColVals[25].DoubleVal.Value,
                                        AskExchange = row.ColVals[20].StringVal.Value[0],
                                        BidExchange = row.ColVals[23].StringVal.Value[0],
                                        Type = typej,
                                        Time = t
                                    }
                                };
                                Observer.OnNext(data); // notify observer...
                            }

                            #endregion

                            #region I
                            else
                            {
                                TimeSpan t = TimeSpan.FromMilliseconds(row.ColVals[1].DoubleVal.Value);

                                if (datet != row.ColVals[0].I32Val.Value)
                                {
                                    datet = row.ColVals[0].I32Val.Value;
                                    dateti = DateTime.ParseExact(row.ColVals[0].I32Val.Value.ToString(), "yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture);
                                }

                                uint buyv = 0;
                                uint sellv = 0;
                                var sidej = API.Imbalance.ImbSide.NONE;
                                if (row.ColVals[12].I32Val.Value == 0)
                                {
                                    sidej = API.Imbalance.ImbSide.NONE;
                                    buyv = 0;
                                    sellv = 0;
                                }
                                else if (row.ColVals[12].I32Val.Value == 1)
                                {
                                    sidej = API.Imbalance.ImbSide.BUY;
                                    buyv = (uint)row.ColVals[14].I32Val.Value;
                                    sellv = 0;
                                }
                                else if (row.ColVals[12].I32Val.Value == 2)
                                {
                                    sidej = API.Imbalance.ImbSide.SELL;
                                    sellv = (uint)row.ColVals[14].I32Val.Value;
                                    buyv = 0;
                                }

                                var typej = API.Imbalance.ImbType.OTHER;
                                if (row.ColVals[15].I32Val.Value == 3)
                                { typej = API.Imbalance.ImbType.OTHER; }
                                else if (row.ColVals[15].I32Val.Value == 1)
                                { typej = API.Imbalance.ImbType.OPEN; }
                                else if (row.ColVals[15].I32Val.Value == 2)
                                { typej = API.Imbalance.ImbType.CLOSE; }

                                var data = new Data
                                {
                                    type = Data.DataType.IMBALANCE,
                                    imbalance = new API.Imbalance
                                    {
                                        Symbol = row.ColVals[2].StringVal.Value,
                                        Date = dateti,
                                        PairedVolume = (uint)row.ColVals[10].I32Val.Value,
                                        Type = typej,
                                        Time = t,
                                        NetImbalance = (uint)row.ColVals[14].I32Val.Value,
                                        BuyVolume = buyv,
                                        SellVolume = sellv,
                                        ReferencePrice = (decimal)row.ColVals[11].DoubleVal.Value,
                                        Side = sidej,
                                        ClearingPrice = (decimal)row.ColVals[4].DoubleVal.Value
                                    }

                                };
                                Observer.OnNext(data); // notify observer...
                            }
                            #endregion
                        }

                        #endregion

                        #region TQ

                        if (unionType == "TQ")
                        {
                            #region T
                            if (row.ColVals[4].StringVal.Value == "T")
                            {
                                TimeSpan t = TimeSpan.FromMilliseconds(row.ColVals[2].I32Val.Value);

                                var data = new Data
                                {
                                    type = Data.DataType.TRADE,
                                    trade = new API.Tick
                                    {
                                        Symbol = row.ColVals[3].StringVal.Value,
                                        Date = DateTime.ParseExact(row.ColVals[1].I32Val.Value.ToString(), "yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture),
                                        Price = (decimal)row.ColVals[7].DoubleVal.Value,
                                        Size = (uint)row.ColVals[12].I32Val.Value,
                                        Open = (decimal)row.ColVals[8].DoubleVal.Value,
                                        High = (decimal)row.ColVals[9].DoubleVal.Value,
                                        Low = (decimal)row.ColVals[10].DoubleVal.Value,
                                        Last = (decimal)row.ColVals[11].DoubleVal.Value,
                                        TotalVolume = (ulong)row.ColVals[19].I64Val.Value,
                                        Time = t
                                    }
                                };

                          #region test oreder file
                                m_streamWriter.WriteLine("OnTrade: " + data.trade.Symbol + ","  + data.trade.Date.ToShortDateString()+","+ data.trade.Time.ToString() + "," + data.trade.Price.ToString() + "," + data.trade.Size.ToString() + "," + data.trade.Open.ToString() + "," + data.trade.High.ToString() + "," + data.trade.Low.ToString() + "," + data.trade.Last.ToString() + "," + data.trade.TotalVolume.ToString());
                          #endregion

                                Observer.OnNext(data); // notify observer...
                            }
                            #endregion

                            #region Q

                            else if (row.ColVals[4].StringVal.Value == "Q")
                            {
                                TimeSpan t = TimeSpan.FromMilliseconds(row.ColVals[2].I32Val.Value);

                                if (datet != row.ColVals[1].I32Val.Value)
                                {
                                    datet = row.ColVals[1].I32Val.Value;
                                    dateti = DateTime.ParseExact(row.ColVals[1].I32Val.Value.ToString(), "yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture);
                                }

                                var typej = API.Quote.QuoteType.NONE;
                                if (row.ColVals[20].DoubleVal.Value == row.ColVals[23].DoubleVal.Value)
                                {
                                    typej = API.Quote.QuoteType.BEST_ASK;
                                }
                                else if (row.ColVals[29].DoubleVal.Value == row.ColVals[26].DoubleVal.Value)
                                {
                                    typej = API.Quote.QuoteType.BEST_BID;
                                }
                                // string askexch = row.ColVals[11].StringVal.Value;

                                var data = new Data
                                {
                                    type = Data.DataType.QUOTE,
                                    quote = new API.Quote
                                    {
                                        Symbol = row.ColVals[3].StringVal.Value,
                                        Date = dateti,
                                        AskSize = (uint)row.ColVals[21].I32Val.Value,
                                        BidSize = (uint)row.ColVals[30].I32Val.Value,
                                        Ask = (decimal)row.ColVals[20].DoubleVal.Value,
                                        Bid = (decimal)row.ColVals[29].DoubleVal.Value,
                                        AskExchange = (char)row.ColVals[24].I32Val.Value,
                                        BidExchange = (char)row.ColVals[27].I32Val.Value,
                                        Type = typej,
                                        Time = t
                                    }
                                };

                                #region test oreder file
                        //        m_streamWriter.WriteLine("OnQuote: " + data.quote.Symbol + "," + data.quote.Time.ToString() + "," + data.quote.Ask.ToString() + "," + data.quote.AskSize.ToString() + "," + data.quote.Bid.ToString() + "," + data.quote.BidSize.ToString() + "," + data.quote.AskExchange.ToString() + "," + data.quote.BidExchange.ToString() + "," + data.quote.Type.ToString());
                                #endregion

                                Observer.OnNext(data); // notify observer...
                            }

                            #endregion
                        }

                        #endregion

                        #region I

                        if (unionType == "I")
                        {
                                TimeSpan t = TimeSpan.FromMilliseconds(row.ColVals[1].DoubleVal.Value);

                                if (datet != row.ColVals[0].I32Val.Value)
                                {
                                    datet = row.ColVals[0].I32Val.Value;
                                    dateti = DateTime.ParseExact(row.ColVals[0].I32Val.Value.ToString(), "yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture);
                                }

                                uint buyv = 0;
                                uint sellv = 0;
                                var sidej = API.Imbalance.ImbSide.NONE;
                                if (row.ColVals[12].I32Val.Value == 0)
                                {
                                    sidej = API.Imbalance.ImbSide.NONE;
                                    buyv = 0;
                                    sellv = 0;
                                }
                                else if (row.ColVals[12].I32Val.Value == 1)
                                {
                                    sidej = API.Imbalance.ImbSide.BUY;
                                    buyv = (uint)row.ColVals[14].I32Val.Value;
                                    sellv = 0;
                                }
                                else if (row.ColVals[12].I32Val.Value == 2)
                                {
                                    sidej = API.Imbalance.ImbSide.SELL;
                                    sellv = (uint)row.ColVals[14].I32Val.Value;
                                    buyv = 0;
                                }

                                var typej = API.Imbalance.ImbType.OTHER;
                                if (row.ColVals[15].I32Val.Value == 3)
                                { typej = API.Imbalance.ImbType.OTHER; }
                                else if (row.ColVals[15].I32Val.Value == 1)
                                { typej = API.Imbalance.ImbType.OPEN; }
                                else if (row.ColVals[15].I32Val.Value == 2)
                                { typej = API.Imbalance.ImbType.CLOSE; }

                                var data = new Data
                                {
                                    type = Data.DataType.IMBALANCE,
                                    imbalance = new API.Imbalance
                                    {
                                        Symbol = row.ColVals[2].StringVal.Value,
                                        Date = dateti,
                                        PairedVolume = (uint)row.ColVals[10].I32Val.Value,
                                        Type = typej,
                                        Time = t,
                                        NetImbalance = (uint)row.ColVals[14].I32Val.Value,
                                        BuyVolume = buyv,
                                        SellVolume = sellv,
                                        ReferencePrice = (decimal)row.ColVals[11].DoubleVal.Value,
                                        Side = sidej,
                                        ClearingPrice = (decimal)row.ColVals[4].DoubleVal.Value
                                    }

                                };
                                Observer.OnNext(data); // notify observer...

                        }
                        #endregion

                    }
                    #endregion

                    fetchReq.Orientation = TFetchOrientation.FETCH_NEXT;
                    Observer.Notify((decimal)resultsResp.Results.Rows.Count); /// send a report of the pct of job completed...
                } while (bMore); // We're fetching 100 at a time.
                Observer.OnCompleted(); // we're done

               #region test oder file
                m_streamWriter.Close();
                m_streamWriter.Dispose();
                fs.Close();
                fs.Dispose();
               #endregion

                #endregion
            }
            else
            {
                Observer.OnError(new Exception(string.Format("{0}: {1}", execResp.Status.ErrorCode, execResp.Status.ErrorMessage)));
            }
        }

        #endregion
    }

    /// <summary>
    /// Used to specify the imbalance table for a query.
    /// </summary>
    public enum ImbalanceSource
    {
        NYSE,
        NASD,
        ARCA,
        AMEX
    }

    public enum Table
    {
        DAILY,
        QUOTE,
        TRADE,
        NYSE_IMB,
        NASD_IMB,
        ARCA_IMB,
        AMEX_IMB
    }
}

