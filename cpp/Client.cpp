#include "Client.h"
#include "DataTypes.h"

// thrift headers
#include <thrift\protocol\TBinaryProtocol.h>
#include <thrift\transport\TSocket.h>
#include <thrift\transport\TBufferTransports.h>
#include <thrift\protocol\TProtocol.h>
#include "TCLIService.h"
#include "TCLIService_types.h"

// boost headers
#include <boost\make_shared.hpp>
#include <boost\date_time\posix_time\posix_time_io.hpp>
#include <boost\date_time\gregorian\gregorian_io.hpp>
#include <boost\foreach.hpp>

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::hive::service::cli::thrift;

//std::string Maxrow="4294967295";
unsigned int Maxrow=4294967295;

namespace Coastal
{
	namespace DB
	{
		// private implementation 
		class Impl : private boost::noncopyable
		{
		public:

			#pragma region fecthdata
			
			#pragma region daily request handler
			void doFetchDaily(Client *pHandler,
				const boost::shared_ptr<TCLIServiceClient> &pCli,
				const TSessionHandle &session,
				const boost::gregorian::date &dt, 
				const boost::posix_time::time_duration &start,
				const boost::posix_time::time_duration &end,
				const std::vector<std::string> &symbols)
			{
				// date format
				boost::gregorian::date_facet *dfacet = new boost::gregorian::date_facet("%Y%m%d");
				std::ostringstream oss;
				oss.imbue(std::locale(oss.getloc(), dfacet));

				// query

				oss << "select * from marketdata.daily as D where day="
					<< dt;

				if (symbols.size() != 0 && symbols.size() < 400)
				{
					std::string symbolsArray;
					BOOST_FOREACH (std::string s, symbols)
					{
						symbolsArray = symbolsArray + "','" + s;
					}
					symbolsArray = &symbolsArray[2];
					symbolsArray = "(" + symbolsArray + "')";
					oss	<< " and symbol in " 
						<< symbolsArray;
				}

				const std::string query = oss.str();

#if TEST
				std::cout << query << std::endl;
#endif

				TExecuteStatementReq execReq;
				execReq.sessionHandle = session;
				execReq.statement = query;

				// execute query
				TExecuteStatementResp execResp;
				pCli->ExecuteStatement(execResp,execReq);
				TOperationHandle stmtHandle = execResp.operationHandle;

				if(execResp.status.statusCode == TStatusCode::SUCCESS_STATUS || execResp.status.statusCode == TStatusCode::SUCCESS_WITH_INFO_STATUS)
				{
					// fetch results
					TFetchResultsReq fetchReq;
					fetchReq.operationHandle = stmtHandle;
					fetchReq.maxRows =  Maxrow;
					fetchReq.orientation = TFetchOrientation::FETCH_NEXT;

					bool bmore = false;
					do
					{

						TFetchResultsResp resultsResp;
						pCli->FetchResults(resultsResp,fetchReq);
						TRowSet resultSet = resultsResp.results;
						bmore=resultsResp.hasMoreRows;


						BOOST_FOREACH (TRow row, resultSet.rows)
						{
							bool symExi = false;
							std::string str = row.colVals[1].stringVal.value;
							BOOST_FOREACH (std::string sym, symbols)
							{
								if(str==sym || symbols.size()<400)
								{
									symExi = true;
                                      break;
								}

							}

							if(symExi)
							{
							Daily dai;

							// construct daily
							dai.date = boost::gregorian::from_undelimited_string(std::to_string((_Longlong)row.colVals[0].i32Val.value));
							dai.symbol = row.colVals[1].stringVal.value;
							dai.high = row.colVals[3].doubleVal.value;
							dai.close = row.colVals[5].doubleVal.value;
							dai.low = row.colVals[4].doubleVal.value;
							dai.open = row.colVals[2].doubleVal.value;
							dai.volume = row.colVals[6].i64Val.value;
							// notify handler
							pHandler->OnDaily(dai);
							}
						}
						fetchReq.orientation = TFetchOrientation::FETCH_NEXT;
					}
					while(bmore);
				}
			}
			#pragma endregion

            #pragma region trade request handler
			void doFetchTrades(Client *pHandler,
				const boost::shared_ptr<TCLIServiceClient> &pCli,
				const TSessionHandle &session,
				const boost::gregorian::date &dt, 
				const boost::posix_time::time_duration &start,
				const boost::posix_time::time_duration &end,
				const std::vector<std::string> &symbols)
			{
				// date format
				boost::gregorian::date_facet *dfacet = new boost::gregorian::date_facet("%Y%m%d");
				std::ostringstream oss;
				oss.imbue(std::locale(oss.getloc(), dfacet));

				// query
				oss << "select * from marketdata.trades as T where day="
					<< dt;
				if(start.total_milliseconds() != 0)
				{
					oss	<< " and T.time >= " 
						<< start.total_milliseconds();
				}
				if(end.total_milliseconds() != 0)
				{
					oss	<< " and T.time <= " 
						<< end.total_milliseconds();
				}
				if (symbols.size() != 0 && symbols.size() < 400)
				{
					std::string symbolsArray;
					BOOST_FOREACH (std::string s, symbols)
					{
						symbolsArray = symbolsArray + "','" + s;
					}
					symbolsArray = &symbolsArray[2];
					symbolsArray = "(" + symbolsArray + "')";
					oss	<< " and symbol in " 
						<< symbolsArray;
				}
				//oss << " order by T.time limit " + Maxrow;
				const std::string query = oss.str();

#if TEST
				std::cout << query << std::endl;
#endif

				TExecuteStatementReq execReq;
				execReq.sessionHandle = session;
				execReq.statement = query;

				// execute query
				TExecuteStatementResp execResp;
				pCli->ExecuteStatement(execResp,execReq);
				TOperationHandle stmtHandle = execResp.operationHandle;

				if(execResp.status.statusCode == TStatusCode::SUCCESS_STATUS || execResp.status.statusCode == TStatusCode::SUCCESS_WITH_INFO_STATUS)
				{
					// fetch results
					TFetchResultsReq fetchReq;
					fetchReq.operationHandle = stmtHandle;
					fetchReq.maxRows =  Maxrow;
					fetchReq.orientation = TFetchOrientation::FETCH_NEXT;

					bool bmore=false;
					do
					{

						TFetchResultsResp resultsResp;
						pCli->FetchResults(resultsResp,fetchReq);
						TRowSet resultSet = resultsResp.results;
						bmore = resultsResp.hasMoreRows;


						BOOST_FOREACH (TRow row, resultSet.rows)
						{
							bool symExi = false;
							std::string str = row.colVals[2].stringVal.value;
							BOOST_FOREACH (std::string sym, symbols)
							{
								if(str==sym || symbols.size()<400)
								{
									symExi = true;
                                      break;
								}

							}

							if(symExi)
							{
							Trade trd;

							// construct trade
							trd.date = boost::gregorian::from_undelimited_string(std::to_string((_Longlong)row.colVals[19].i32Val.value));
							trd.symbol = row.colVals[2].stringVal.value;
							trd.time = boost::posix_time::millisec(row.colVals[16].i32Val.value);
							trd.exg = row.colVals[18].i32Val.value;
							trd.high = row.colVals[10].doubleVal.value;
							trd.last = row.colVals[12].doubleVal.value;
							trd.low = row.colVals[11].doubleVal.value;
							trd.open = row.colVals[9].doubleVal.value;
							trd.price = row.colVals[3].doubleVal.value;
							trd.size = row.colVals[7].i32Val.value;
							trd.volume = row.colVals[14].i64Val.value;
							// notify handler
							pHandler->OnTrade(trd);
							}
						}
						fetchReq.orientation = TFetchOrientation::FETCH_NEXT;
					}
					while(bmore);
				}
			}
			#pragma endregion

		    #pragma region trade marketdata2 request handler
			void doFetchTrades2(Client *pHandler,
				const boost::shared_ptr<TCLIServiceClient> &pCli,
				const TSessionHandle &session,
				const boost::gregorian::date &dt, 
				const boost::posix_time::time_duration &start,
				const boost::posix_time::time_duration &end,
				const std::vector<std::string> &symbols)
			{
				// date format
				boost::gregorian::date_facet *dfacet = new boost::gregorian::date_facet("%Y%m%d");
				std::ostringstream oss;
				oss.imbue(std::locale(oss.getloc(), dfacet));

				// query
				oss << "select * from marketdata2.trades as T where day="
					<< dt;
				if(start.total_milliseconds() != 0)
				{
					oss	<< " and T.time >= " 
						<< start.total_milliseconds();
				}
				if(end.total_milliseconds() != 0)
				{
					oss	<< " and T.time <= " 
						<< end.total_milliseconds();
				}
				if (symbols.size() != 0 && symbols.size() < 400)
				{
					std::string symbolsArray;
					BOOST_FOREACH (std::string s, symbols)
					{
						symbolsArray = symbolsArray + "','" + s;
					}
					symbolsArray = &symbolsArray[2];
					symbolsArray = "(" + symbolsArray + "')";
					oss	<< " and symbol in " 
						<< symbolsArray;
				}
				oss << " order by seqno limit " << Maxrow;
				const std::string query = oss.str();

#if TEST
				std::cout << query << std::endl;
#endif

				TExecuteStatementReq execReq;
				execReq.sessionHandle = session;
				execReq.statement = query;

				// execute query
				TExecuteStatementResp execResp;
				pCli->ExecuteStatement(execResp,execReq);
				TOperationHandle stmtHandle = execResp.operationHandle;

				if(execResp.status.statusCode == TStatusCode::SUCCESS_STATUS || execResp.status.statusCode == TStatusCode::SUCCESS_WITH_INFO_STATUS)
				{
					// fetch results
					TFetchResultsReq fetchReq;
					fetchReq.operationHandle = stmtHandle;
					fetchReq.maxRows =  Maxrow;
					fetchReq.orientation = TFetchOrientation::FETCH_NEXT;

					bool bmore=false;
					do
					{

						TFetchResultsResp resultsResp;
						pCli->FetchResults(resultsResp,fetchReq);
						TRowSet resultSet = resultsResp.results;
						bmore = resultsResp.hasMoreRows;


						BOOST_FOREACH (TRow row, resultSet.rows)
						{
							bool symExi = false;
							std::string str = row.colVals[1].stringVal.value;
							BOOST_FOREACH (std::string sym, symbols)
							{
								if(str==sym || symbols.size()<400)
								{
									symExi = true;
                                      break;
								}

							}

							if(symExi)
							{
							Trade trd;

							// construct trade
							trd.date = boost::gregorian::from_undelimited_string(std::to_string((_Longlong)row.colVals[18].i32Val.value));
							trd.symbol = row.colVals[1].stringVal.value;
							trd.time = boost::posix_time::millisec(row.colVals[15].i32Val.value);
							trd.exg = row.colVals[17].i32Val.value;
							trd.high = row.colVals[9].doubleVal.value;
							trd.last = row.colVals[11].doubleVal.value;
							trd.low = row.colVals[10].doubleVal.value;
							trd.open = row.colVals[8].doubleVal.value;
							trd.price = row.colVals[2].doubleVal.value;
							trd.size = row.colVals[6].i32Val.value;
							trd.volume = row.colVals[13].i32Val.value;
							// notify handler
							pHandler->OnTrade(trd);
							}
						}
						fetchReq.orientation = TFetchOrientation::FETCH_NEXT;
					}
					while(bmore);
				}
			}
			#pragma endregion

			#pragma region NYSE request handler
			void doFetchNYSE(Client *pHandler,
				const boost::shared_ptr<TCLIServiceClient> &pCli,
				const TSessionHandle &session,
				const boost::gregorian::date &dt, 
				const boost::posix_time::time_duration &start,
				const boost::posix_time::time_duration &end,
				const std::vector<std::string> &symbols)
			{
				// date format
				boost::gregorian::date_facet *dfacet = new boost::gregorian::date_facet("%Y%m%d");
				std::ostringstream oss;
				oss.imbue(std::locale(oss.getloc(), dfacet));

				// query
				oss << "select * from marketdata.imb_nyse as I where I.day="
					<< dt;
				if(start.total_milliseconds() != 0)
				{
					oss	<< " and I.msofday >= " 
						<< start.total_milliseconds();
				}
				if(end.total_milliseconds() != 0)
				{
					oss	<< " and I.msofday <= " 
						<< end.total_milliseconds();
				}
				if (symbols.size() != 0 && symbols.size() < 400)
				{
					std::string symbolsArray;
					BOOST_FOREACH (std::string s, symbols)
					{
						symbolsArray = symbolsArray + "','" + s;
					}
					symbolsArray = &symbolsArray[2];
					symbolsArray = "(" + symbolsArray + "')";
					oss	<< " and symbol in " 
						<< symbolsArray;
				}
				//oss << " order by I.time limit " + Maxrow;
				const std::string query = oss.str();

#if TEST
				std::cout << query << std::endl;
#endif

				TExecuteStatementReq execReq;
				execReq.sessionHandle = session;
				execReq.statement = query;

				// execute query
				TExecuteStatementResp execResp;
				pCli->ExecuteStatement(execResp,execReq);
				TOperationHandle stmtHandle = execResp.operationHandle;

				if(execResp.status.statusCode == TStatusCode::SUCCESS_STATUS || execResp.status.statusCode == TStatusCode::SUCCESS_WITH_INFO_STATUS)
				{
					// fetch results
					TFetchResultsReq fetchReq;
					fetchReq.operationHandle = stmtHandle;
					fetchReq.maxRows =  Maxrow;
					fetchReq.orientation = TFetchOrientation::FETCH_NEXT;

					bool bmore=false;
					do
					{

						TFetchResultsResp resultsResp;
						pCli->FetchResults(resultsResp,fetchReq);
						TRowSet resultSet = resultsResp.results;
						bmore=resultsResp.hasMoreRows;


						BOOST_FOREACH (TRow row, resultSet.rows)
						{
							bool symExi = false;
							std::string str = row.colVals[0].stringVal.value;
							BOOST_FOREACH (std::string sym, symbols)
							{
								if(str==sym || symbols.size()<400)
								{
									symExi = true;
                                      break;
								}

							}

							if(symExi)
							{
							Imbalance imb;

							Imbalance::Side sidej = Imbalance::NONE;
							switch(row.colVals[2].byteVal.value)
							{
							case 1:
								{
									sidej = Imbalance::BUY;
								}
								break;
							case 2:
								{
									sidej = Imbalance::SELL;
								}
								break;
							default:
								{
									sidej = Imbalance::NONE;
								}
								break;
							}

							Imbalance::ImbType typej = Imbalance::UNKNOWN;
							switch(row.colVals[3].byteVal.value)
							{
							case 0:
								typej = Imbalance::UNKNOWN;
								break;
							case 1:
								typej = Imbalance::OPEN;
								break;
							case 2:
								typej = Imbalance::MARKET;
								break;
							case 3:
								typej = Imbalance::HALT;
								break;
							case 4:
								typej = Imbalance::CLOSE;
								break;
							case 5:
								typej = Imbalance::NO_IMBALANCE;
								break;
							}

							// construct imbalance
							imb.date = boost::gregorian::from_undelimited_string(std::to_string((_Longlong)row.colVals[13].i32Val.value));
							imb.symbol = row.colVals[0].stringVal.value;
							imb.time = boost::posix_time::millisec(row.colVals[1].i32Val.value);
							imb.clearing_price = row.colVals[11].doubleVal.value;
							imb.far_price = row.colVals[9].doubleVal.value;
							imb.imbalance_type = typej;
							imb.market_imbalance = row.colVals[6].i32Val.value;
							imb.near_price = row.colVals[10].doubleVal.value;
							imb.paired_shares = row.colVals[5].i32Val.value;
							imb.ref_price = row.colVals[8].doubleVal.value;
							imb.side = sidej;
							imb.total_imbalance = row.colVals[7].i32Val.value;
							imb.tag=row.colVals[12].byteVal.value;
							// notify handler
							pHandler->OnImbalance(imb);
							}
						}
						fetchReq.orientation = TFetchOrientation::FETCH_NEXT;
					}
					while(bmore);
				}
			}
			#pragma endregion

			#pragma region AMEX request handler
			void doFetchAMEX(Client *pHandler,
				const boost::shared_ptr<TCLIServiceClient> &pCli,
				const TSessionHandle &session,
				const boost::gregorian::date &dt, 
				const boost::posix_time::time_duration &start,
				const boost::posix_time::time_duration &end,
				const std::vector<std::string> &symbols)
			{
				// date format
				boost::gregorian::date_facet *dfacet = new boost::gregorian::date_facet("%Y%m%d");
				std::ostringstream oss;
				oss.imbue(std::locale(oss.getloc(), dfacet));

				// query
				oss << "select * from marketdata.imb_amex as I where I.day="
					<< dt;
				if(start.total_milliseconds() != 0)
				{
					oss	<< " and I.time >= " 
						<< start.total_milliseconds();
				}
				if(end.total_milliseconds() != 0)
				{
					oss	<< " and I.time <= " 
						<< end.total_milliseconds();
				}
				if (symbols.size() != 0)
				{
					std::string symbolsArray;
					BOOST_FOREACH (std::string s, symbols)
					{
						symbolsArray = symbolsArray + "','" + s;
					}
					symbolsArray = &symbolsArray[2];
					symbolsArray = "(" + symbolsArray + "')";
					oss	<< " and symbol in " 
						<< symbolsArray;
				}
				oss << " order by I.time limit " + Maxrow;
				const std::string query = oss.str();

#if TEST
				std::cout << query << std::endl;
#endif

				TExecuteStatementReq execReq;
				execReq.sessionHandle = session;
				execReq.statement = query;

				// execute query
				TExecuteStatementResp execResp;
				pCli->ExecuteStatement(execResp,execReq);
				TOperationHandle stmtHandle = execResp.operationHandle;

				if(execResp.status.statusCode == TStatusCode::SUCCESS_STATUS || execResp.status.statusCode == TStatusCode::SUCCESS_WITH_INFO_STATUS)
				{
					// fetch results
					TFetchResultsReq fetchReq;
					fetchReq.operationHandle = stmtHandle;
					fetchReq.maxRows =  Maxrow;
					fetchReq.orientation = TFetchOrientation::FETCH_NEXT;

					bool bmore = false;
					do
					{

						TFetchResultsResp resultsResp;
						pCli->FetchResults(resultsResp,fetchReq);
						TRowSet resultSet = resultsResp.results;
						bmore = resultsResp.hasMoreRows;


						BOOST_FOREACH (TRow row, resultSet.rows)
						{
							Imbalance imb;

							Imbalance::Side sidej = Imbalance::NONE;
							switch(row.colVals[4].i32Val.value)
							{
							case 1:
								{
									sidej = Imbalance::BUY;
								}
								break;
							case 2:
								{
									sidej = Imbalance::SELL;
								}
								break;
							default:
								{
									sidej = Imbalance::NONE;
								}
								break;
							}
							

							Imbalance::ImbType typej = Imbalance::UNKNOWN;
							switch(row.colVals[5].i32Val.value)
							{
							case 1:
								typej = Imbalance::OPEN;
								break;
							case 2:
								typej = Imbalance::MARKET;
								break;
							case 3:
								typej = Imbalance::HALT;
								break;
							case 4:
								typej = Imbalance::CLOSE;
								break;
							case 5:
								typej = Imbalance::NO_IMBALANCE;
								break;
							}

							// construct imbalance
							imb.date = boost::gregorian::from_undelimited_string(std::to_string((_Longlong)row.colVals[0].i32Val.value));
							imb.symbol = row.colVals[2].stringVal.value;
							imb.time = boost::posix_time::millisec(row.colVals[1].doubleVal.value);
							imb.clearing_price = row.colVals[13].doubleVal.value;
							imb.far_price = row.colVals[11].doubleVal.value;
							imb.imbalance_type = typej;
							imb.market_imbalance = row.colVals[8].i32Val.value;
							imb.near_price = row.colVals[12].doubleVal.value;
							imb.paired_shares = row.colVals[7].i32Val.value;
							imb.ref_price = row.colVals[10].doubleVal.value;
							imb.side = sidej;
							imb.total_imbalance = row.colVals[9].i32Val.value;
							//imb.tag=row.colVals[14].i32Val.value;
							// notify handler
							pHandler->OnImbalance(imb);
						}
						fetchReq.orientation = TFetchOrientation::FETCH_NEXT;
					}
					while(bmore);
				}
			}
			#pragma endregion

			#pragma region ARCA request handler
			void doFetchARCA(Client *pHandler,
				const boost::shared_ptr<TCLIServiceClient> &pCli,
				const TSessionHandle &session,
				const boost::gregorian::date &dt, 
				const boost::posix_time::time_duration &start,
				const boost::posix_time::time_duration &end,
				const std::vector<std::string> &symbols)
			{
				// date format
				boost::gregorian::date_facet *dfacet = new boost::gregorian::date_facet("%Y%m%d");
				std::ostringstream oss;
				oss.imbue(std::locale(oss.getloc(), dfacet));

				// query
				oss << "select * from marketdata.imb_arca as I where I.day="
					<< dt;
				if(start.total_milliseconds() != 0)
				{
					oss	<< " and I.time >= " 
						<< start.total_milliseconds();
				}
				if(end.total_milliseconds() != 0)
				{
					oss	<< " and I.time <= " 
						<< end.total_milliseconds();
				}
				if (symbols.size() != 0)
				{
					std::string symbolsArray;
					BOOST_FOREACH (std::string s, symbols)
					{
						symbolsArray = symbolsArray + "','" + s;
					}
					symbolsArray = &symbolsArray[2];
					symbolsArray = "(" + symbolsArray + "')";
					oss	<< " and symbol in " 
						<< symbolsArray;
				}
				oss << " order by I.time limit " + Maxrow;
				const std::string query = oss.str();

#if TEST
				std::cout << query << std::endl;
#endif

				TExecuteStatementReq execReq;
				execReq.sessionHandle = session;
				execReq.statement = query;

				// execute query
				TExecuteStatementResp execResp;
				pCli->ExecuteStatement(execResp,execReq);
				TOperationHandle stmtHandle = execResp.operationHandle;

				if(execResp.status.statusCode == TStatusCode::SUCCESS_STATUS || execResp.status.statusCode == TStatusCode::SUCCESS_WITH_INFO_STATUS)
				{
					// fetch results
					TFetchResultsReq fetchReq;
					fetchReq.operationHandle = stmtHandle;
					fetchReq.maxRows =  Maxrow;
					fetchReq.orientation = TFetchOrientation::FETCH_NEXT;

					bool bmore=false;
					do
					{

						TFetchResultsResp resultsResp;
						pCli->FetchResults(resultsResp,fetchReq);
						TRowSet resultSet = resultsResp.results;
						bmore = resultsResp.hasMoreRows;


						BOOST_FOREACH (TRow row, resultSet.rows)
						{
							Imbalance imb;

							Imbalance::Side sidej = Imbalance::NONE;
							if (row.colVals[4].i32Val.value == 0)
							{
								sidej = Imbalance::NONE;
							}
							else if (row.colVals[4].i32Val.value == 1)
							{
								sidej = Imbalance::BUY;
							}
							else if (row.colVals[4].i32Val.value == 2)
							{
								sidej = Imbalance::SELL;
							}
							

							Imbalance::ImbType typej = Imbalance::UNKNOWN;
							switch(row.colVals[5].i32Val.value)
							{
							case 1:
								typej = Imbalance::OPEN;
								break;
							case 2:
								typej = Imbalance::MARKET;
								break;
							case 3:
								typej = Imbalance::HALT;
								break;
							case 4:
								typej = Imbalance::CLOSE;
								break;
							case 5:
								typej = Imbalance::NO_IMBALANCE;
								break;
							}

							// construct imbalance
							imb.date = boost::gregorian::from_undelimited_string(std::to_string((_Longlong)row.colVals[0].i32Val.value));
							imb.symbol = row.colVals[2].stringVal.value;
							imb.time = boost::posix_time::millisec(row.colVals[1].doubleVal.value);
							imb.clearing_price = row.colVals[13].doubleVal.value;
							imb.far_price = row.colVals[11].doubleVal.value;
							imb.imbalance_type = typej;
							imb.market_imbalance = row.colVals[8].i32Val.value;
							imb.near_price = row.colVals[12].doubleVal.value;
							imb.paired_shares = row.colVals[7].i32Val.value;
							imb.ref_price = row.colVals[10].doubleVal.value;
							imb.side = sidej;
							imb.total_imbalance = row.colVals[9].i32Val.value;
							//imb.tag=row.colVals[14].i32Val.value;
							// notify handler
							pHandler->OnImbalance(imb);
						}
						fetchReq.orientation=TFetchOrientation::FETCH_NEXT;
					}
					while(bmore);
				}
			}
			#pragma endregion

			#pragma region ARCA request handler
			void doFetchNASD(Client *pHandler,
				const boost::shared_ptr<TCLIServiceClient> &pCli,
				const TSessionHandle &session,
				const boost::gregorian::date &dt, 
				const boost::posix_time::time_duration &start,
				const boost::posix_time::time_duration &end,
				const std::vector<std::string> &symbols)
			{
				// date format
				boost::gregorian::date_facet *dfacet = new boost::gregorian::date_facet("%Y%m%d");
				std::ostringstream oss;
				oss.imbue(std::locale(oss.getloc(), dfacet));

				// query
				oss << "select * from marketdata.imb_nasd as I where I.day="
					<< dt;
				if(start.total_milliseconds() !=0)
				{
					oss	<< " and I.time >= " 
						<< start.total_milliseconds();
				}
				if(end.total_milliseconds() != 0)
				{
					oss	<< " and I.time <= " 
						<< end.total_milliseconds();
				}
				if (symbols.size() != 0)
				{
					std::string symbolsArray;
					BOOST_FOREACH (std::string s, symbols)
					{
						symbolsArray = symbolsArray + "','" + s;
					}
					symbolsArray = &symbolsArray[2];
					symbolsArray = "(" + symbolsArray + "')";
					oss	<< " and symbol in " 
						<< symbolsArray;
				}
				oss << " order by I.time limit " + Maxrow;
				const std::string query = oss.str();

#if TEST
				std::cout << query << std::endl;
#endif

				TExecuteStatementReq execReq;
				execReq.sessionHandle = session;
				execReq.statement = query;

				// execute query
				TExecuteStatementResp execResp;
				pCli->ExecuteStatement(execResp,execReq);
				TOperationHandle stmtHandle = execResp.operationHandle;

				if(execResp.status.statusCode == TStatusCode::SUCCESS_STATUS || execResp.status.statusCode == TStatusCode::SUCCESS_WITH_INFO_STATUS)
				{
					// fetch results
					TFetchResultsReq fetchReq;
					fetchReq.operationHandle = stmtHandle;
					fetchReq.maxRows =  Maxrow;
					fetchReq.orientation = TFetchOrientation::FETCH_NEXT;

					bool bmore = false;
					do
					{

						TFetchResultsResp resultsResp;
						pCli->FetchResults(resultsResp,fetchReq);
						TRowSet resultSet = resultsResp.results;
						bmore=resultsResp.hasMoreRows;


						BOOST_FOREACH (TRow row, resultSet.rows)
						{
							Imbalance imb;

							Imbalance::Side sidej = Imbalance::NONE;
							switch(row.colVals[4].i32Val.value)
							{
							case 1:
								{
									sidej = Imbalance::BUY;
								}
								break;
							case 2:
								{
									sidej = Imbalance::SELL;
								}
								break;
							default:
								{
									sidej = Imbalance::NONE;
								}
								break;
							}
							

							Imbalance::ImbType typej = Imbalance::UNKNOWN;
							switch(row.colVals[5].i32Val.value)
							{
							case 1:
								typej = Imbalance::OPEN;
								break;
							case 2:
								typej = Imbalance::MARKET;
								break;
							case 3:
								typej = Imbalance::HALT;
								break;
							case 4:
								typej = Imbalance::CLOSE;
								break;
							case 5:
								typej = Imbalance::NO_IMBALANCE;
								break;
							}

							// construct imbalance
							imb.date = boost::gregorian::from_undelimited_string(std::to_string((_Longlong)row.colVals[0].i32Val.value));
							imb.symbol = row.colVals[2].stringVal.value;
							imb.time = boost::posix_time::millisec(row.colVals[1].doubleVal.value);
							//imb.clearing_price=row.colVals[13].doubleVal.value;
							imb.far_price = row.colVals[11].doubleVal.value;
							imb.imbalance_type = typej;
							imb.market_imbalance = row.colVals[8].i32Val.value;
							imb.near_price = row.colVals[12].doubleVal.value;
							imb.paired_shares = row.colVals[7].i32Val.value;
							imb.ref_price = row.colVals[10].doubleVal.value;
							imb.side = sidej;
							imb.total_imbalance = row.colVals[9].i32Val.value;
							//imb.tag=row.colVals[13].i32Val.value;

							// notify handler
							pHandler->OnImbalance(imb);
						}
						fetchReq.orientation = TFetchOrientation::FETCH_NEXT;
					}
					while(bmore);
				}
			}
			#pragma endregion

			#pragma region quote request handler
			void doFetchQuotes(Client *pHandler,
				const boost::shared_ptr<TCLIServiceClient> &pCli,
				const TSessionHandle &session,
				const boost::gregorian::date &dt, 
				const boost::posix_time::time_duration &start,
				const boost::posix_time::time_duration &end,
				const std::vector<std::string> &symbols)
			{
				// date format
				boost::gregorian::date_facet *dfacet = new boost::gregorian::date_facet("%Y%m%d");
				std::ostringstream oss;
				oss.imbue(std::locale(oss.getloc(), dfacet));

				// query
				oss << "select * from marketdata.quotes as Q where day="
					<< dt;
				if(start.total_milliseconds() != 0)
				{
					oss	<< " and Q.time >= " 
						<< start.total_milliseconds();
				}
				if(end.total_milliseconds() != 0)
				{
					oss	<< " and Q.time <= " 
						<< end.total_milliseconds();
				}
				if (symbols.size() != 0 && symbols.size() < 400)
				{
					std::string symbolsArray;
					BOOST_FOREACH (std::string s, symbols)
					{
						symbolsArray = symbolsArray + "','" + s;
					}
					symbolsArray = &symbolsArray[2];
					symbolsArray = "(" + symbolsArray + "')";
					oss	<< " and symbol in " 
						<< symbolsArray;
				}
	//			oss << " order by Q.time limit " << Maxrow;
				const std::string query = oss.str();

#if TEST
				std::cout << query << std::endl;
#endif

				TExecuteStatementReq execReq;
				execReq.sessionHandle = session;
				execReq.statement = query;

				// execute query
				TExecuteStatementResp execResp;
				pCli->ExecuteStatement(execResp,execReq);
				TOperationHandle stmtHandle = execResp.operationHandle;

				if(execResp.status.statusCode == TStatusCode::SUCCESS_STATUS || execResp.status.statusCode == TStatusCode::SUCCESS_WITH_INFO_STATUS)
				{
					// fetch results
					TFetchResultsReq fetchReq;
					fetchReq.operationHandle = stmtHandle;
					fetchReq.maxRows = Maxrow;
					fetchReq.orientation = TFetchOrientation::FETCH_NEXT;

					bool bmore=false;
					do
					{

						TFetchResultsResp resultsResp;
						pCli->FetchResults(resultsResp,fetchReq);
						TRowSet resultSet = resultsResp.results;
						bmore = resultsResp.hasMoreRows;


						BOOST_FOREACH (TRow row, resultSet.rows)
						{
							bool symExi = false;
							std::string str = row.colVals[2].stringVal.value;
							BOOST_FOREACH (std::string sym, symbols)
							{
								if(str==sym || symbols.size()<400)
								{
									symExi = true;
                                      break;
								}

							}

							if(symExi)
							{
							Quote quo;
							//std::istringstream buffer(row.colVals[11].stringVal.value);
							//std::istringstream buffer2(row.colVals[12].stringVal.value);
							// construct quote
							quo.date = boost::gregorian::from_undelimited_string(std::to_string((_Longlong)row.colVals[19].i32Val.value));
							quo.symbol = row.colVals[2].stringVal.value;
							quo.time = boost::posix_time::millisec(row.colVals[16].i32Val.value);
							quo.ask = row.colVals[5].doubleVal.value;
							quo.ask_size = row.colVals[3].i32Val.value;
							quo.best_ask = row.colVals[8].doubleVal.value;
							quo.best_ask_exg=row.colVals[12].i32Val.value;
							quo.best_ask_size = row.colVals[11].i32Val.value;
							quo.best_bid = row.colVals[9].doubleVal.value;
							quo.best_bid_exg=row.colVals[13].i32Val.value;
							quo.best_bid_size = row.colVals[12].i32Val.value;
							quo.bid = row.colVals[6].doubleVal.value;
							quo.bid_size = row.colVals[4].i32Val.value;

							// notify handler
							pHandler->OnQuote(quo);
							}
						}
						fetchReq.orientation = TFetchOrientation::FETCH_NEXT;
					}
					while(bmore);
				}
			}
			#pragma endregion

		    #pragma region Exgprints request handler
			void doFetchExgprints(Client *pHandler,
				const boost::shared_ptr<TCLIServiceClient> &pCli,
				const TSessionHandle &session,
				const boost::gregorian::date &dt, 
				const boost::posix_time::time_duration &start,
				const boost::posix_time::time_duration &end,
				const std::vector<std::string> &symbols)
			{
				// date format
				boost::gregorian::date_facet *dfacet = new boost::gregorian::date_facet("%Y%m%d");
				std::ostringstream oss;
				oss.imbue(std::locale(oss.getloc(), dfacet));

				// query
				oss << "select * from marketdata.exgprints where day="
					<< dt;
			
				if (symbols.size() != 0 && symbols.size() < 400)
				{
					std::string symbolsArray;
					BOOST_FOREACH (std::string s, symbols)
					{
						symbolsArray = symbolsArray + "','" + s;
					}
					symbolsArray = &symbolsArray[2];
					symbolsArray = "(" + symbolsArray + "')";
					oss	<< " and symbol in " 
						<< symbolsArray;
				}
				//oss << " order by T.time limit " + Maxrow;
				const std::string query = oss.str();

#if TEST
				std::cout << query << std::endl;
#endif

				TExecuteStatementReq execReq;
				execReq.sessionHandle = session;
				execReq.statement = query;

				// execute query
				TExecuteStatementResp execResp;
				pCli->ExecuteStatement(execResp,execReq);
				TOperationHandle stmtHandle = execResp.operationHandle;

				if(execResp.status.statusCode == TStatusCode::SUCCESS_STATUS || execResp.status.statusCode == TStatusCode::SUCCESS_WITH_INFO_STATUS)
				{
					// fetch results
					TFetchResultsReq fetchReq;
					fetchReq.operationHandle = stmtHandle;
					fetchReq.maxRows =  Maxrow;
					fetchReq.orientation = TFetchOrientation::FETCH_NEXT;

					bool bmore=false;
					do
					{

						TFetchResultsResp resultsResp;
						pCli->FetchResults(resultsResp,fetchReq);
						TRowSet resultSet = resultsResp.results;
						bmore = resultsResp.hasMoreRows;


						BOOST_FOREACH (TRow row, resultSet.rows)
						{
							bool symExi = false;
							std::string str = row.colVals[0].stringVal.value;
							BOOST_FOREACH (std::string sym, symbols)
							{
								if(str==sym || symbols.size()<400)
								{
									symExi = true;
                                      break;
								}

							}

							if(symExi)
							{
							Exgprints exgP;

							// construct trade
							exgP.date = boost::gregorian::from_undelimited_string(std::to_string((_Longlong)row.colVals[7].i32Val.value));
							exgP.symbol = row.colVals[0].stringVal.value;
							exgP.time = boost::posix_time::millisec(row.colVals[1].i32Val.value);
							exgP.size = row.colVals[6].i32Val.value;
							exgP.price = row.colVals[5].doubleVal.value;
							exgP.listedexchange=row.colVals[2].i32Val.value;
							exgP.reportingexchange=row.colVals[3].i32Val.value;
							exgP.printtype=row.colVals[4].i32Val.value;
							
							// notify handler
							pHandler->OnExgprints(exgP);
							}
						}
						fetchReq.orientation = TFetchOrientation::FETCH_NEXT;
					}
					while(bmore);
				}
			}
			#pragma endregion

		    #pragma region News request handler
			void doFetchNews(Client *pHandler,
				const boost::shared_ptr<TCLIServiceClient> &pCli,
				const TSessionHandle &session,
				const boost::gregorian::date &dt, 
				const boost::posix_time::time_duration &start,
				const boost::posix_time::time_duration &end,
				const std::vector<std::string> &symbols)
			{
				// date format
				boost::gregorian::date_facet *dfacet = new boost::gregorian::date_facet("%Y%m%d");
				std::ostringstream oss;
				oss.imbue(std::locale(oss.getloc(), dfacet));

				// query
				oss << "select * from marketdata.news where day="
					<< dt;
			   if(start.total_milliseconds() != 0)
				{
					oss	<< " and msofday >= " 
						<< start.total_milliseconds();
				}
				if(end.total_milliseconds() != 0)
				{
					oss	<< " and msofday <= " 
						<< end.total_milliseconds();
				}
				if (symbols.size() != 0 && symbols.size() < 400)
				{
					std::string symbolsArray;
					BOOST_FOREACH (std::string s, symbols)
					{
						symbolsArray = symbolsArray + "','" + s;
					}
					symbolsArray = &symbolsArray[2];
					symbolsArray = "(" + symbolsArray + "')";
					oss	<< " and symbol in " 
						<< symbolsArray;
				}
				//oss << " order by T.time limit " + Maxrow;
				const std::string query = oss.str();

#if TEST
				std::cout << query << std::endl;
#endif

				TExecuteStatementReq execReq;
				execReq.sessionHandle = session;
				execReq.statement = query;

				// execute query
				TExecuteStatementResp execResp;
				pCli->ExecuteStatement(execResp,execReq);
				TOperationHandle stmtHandle = execResp.operationHandle;

				if(execResp.status.statusCode == TStatusCode::SUCCESS_STATUS || execResp.status.statusCode == TStatusCode::SUCCESS_WITH_INFO_STATUS)
				{
					// fetch results
					TFetchResultsReq fetchReq;
					fetchReq.operationHandle = stmtHandle;
					fetchReq.maxRows =  Maxrow;
					fetchReq.orientation = TFetchOrientation::FETCH_NEXT;

					bool bmore=false;
					do
					{

						TFetchResultsResp resultsResp;
						pCli->FetchResults(resultsResp,fetchReq);
						TRowSet resultSet = resultsResp.results;
						bmore = resultsResp.hasMoreRows;


						BOOST_FOREACH (TRow row, resultSet.rows)
						{
							bool symExi = false;
							std::string str = row.colVals[1].stringVal.value;
							BOOST_FOREACH (std::string sym, symbols)
							{
								if(str==sym || symbols.size()<400)
								{
									symExi = true;
                                      break;
								}

							}

							std::string newHeadline = row.colVals[5].stringVal.value;
							newHeadline = newHeadline.substr(1).substr(0, newHeadline.size()-2);

							std::string newStory = row.colVals[7].stringVal.value;
							newStory = newStory.substr(1).substr(0, newStory.size()-2);

							std::string newTag = row.colVals[8].stringVal.value;
							newTag = newTag.substr(1).substr(0, newTag.size()-2);

							if(symExi)
							{
							News news;
							// construct trade
							news.date = boost::gregorian::from_undelimited_string(std::to_string((_Longlong)row.colVals[9].i32Val.value));
							news.symbol = row.colVals[1].stringVal.value;
							news.time = boost::posix_time::millisec(row.colVals[2].i32Val.value);
							news.seqno = row.colVals[0].i32Val.value;
							news.category=row.colVals[3].stringVal.value;
							news.source=row.colVals[4].stringVal.value;
							news.headline=newHeadline;
							news.resourceid=row.colVals[6].stringVal.value;
							news.story=newStory;
							news.tags=newTag;
							// notify handler
							pHandler->OnNews(news);
							}

						}
						fetchReq.orientation = TFetchOrientation::FETCH_NEXT;
					}
					while(bmore);
				}
			}
			#pragma endregion

            #pragma endregion 
			// union request handler
            #pragma region union fetchdata

			void doFetchUnion(Client *pHandler,
				const boost::shared_ptr<TCLIServiceClient> &pCli,
				const TSessionHandle &session,
				std::string query,
				std::string unionType)
			 {
				TExecuteStatementReq execReq;
				execReq.sessionHandle = session;
				execReq.statement = query;

				// execute query
				TExecuteStatementResp execResp;
				pCli->ExecuteStatement(execResp,execReq);
				TOperationHandle stmtHandle=execResp.operationHandle;

				if(execResp.status.statusCode == TStatusCode::SUCCESS_STATUS || execResp.status.statusCode == TStatusCode::SUCCESS_WITH_INFO_STATUS)
				{
					// fetch results
					TFetchResultsReq fetchReq;
					fetchReq.operationHandle = stmtHandle;
					fetchReq.maxRows = Maxrow;
					fetchReq.orientation = TFetchOrientation::FETCH_NEXT;
					bool bmore=false;

                    #pragma region fetch the union data

					do
					{
						TFetchResultsResp resultsResp;
						pCli->FetchResults(resultsResp,fetchReq);
						TRowSet resultSet = resultsResp.results;
						bmore=resultsResp.hasMoreRows;
						BOOST_FOREACH (TRow row, resultSet.rows)
						{

							#pragma region union type TQI
							if (unionType == "TQI")
							{
								if (row.colVals[4].stringVal.value == "T")
								{
									Trade trd;
									// construct trade
									trd.date = boost::gregorian::from_undelimited_string(std::to_string((_Longlong)row.colVals[0].i32Val.value));
									trd.symbol = row.colVals[2].stringVal.value;
									trd.time = boost::posix_time::millisec(row.colVals[1].doubleVal.value);
									trd.exg = row.colVals[16].i32Val.value;
									trd.high = row.colVals[8].doubleVal.value;
									trd.last = row.colVals[10].doubleVal.value;
									trd.low = row.colVals[9].doubleVal.value;
									trd.open = row.colVals[7].doubleVal.value;
									trd.price = row.colVals[6].doubleVal.value;
									trd.size = row.colVals[11].i32Val.value;
									trd.volume = row.colVals[18].i64Val.value;
									// notify handler
									pHandler->OnTrade(trd);
								}

								else if (row.colVals[4].stringVal.value == "Q")
								{
									Quote quo;
									std::istringstream buffer(row.colVals[35].stringVal.value);
									std::istringstream buffer2(row.colVals[38].stringVal.value);
									// construct quote
									quo.date = boost::gregorian::from_undelimited_string(std::to_string((_Longlong)row.colVals[0].i32Val.value));
									quo.symbol = row.colVals[2].stringVal.value;
									quo.time = boost::posix_time::millisec(row.colVals[1].doubleVal.value);
									quo.ask = row.colVals[31].doubleVal.value;
									quo.ask_size = row.colVals[32].i32Val.value;
									quo.best_ask = row.colVals[34].doubleVal.value;
									buffer >> quo.best_ask_exg;
									quo.best_ask_size = row.colVals[36].i32Val.value;
									quo.best_bid = row.colVals[37].doubleVal.value;
									buffer2 >> quo.best_bid_exg;
									quo.best_bid_size = row.colVals[39].i32Val.value;
									quo.bid = row.colVals[40].doubleVal.value;
									quo.bid_size = row.colVals[41].i32Val.value;
									// notify handler
									pHandler->OnQuote(quo);
								}
								else
								{
									Imbalance imb;
									Imbalance::Side sidej = Imbalance::NONE;
									switch(row.colVals[27].i32Val.value)
									{
									case 1:
										{
											sidej = Imbalance::BUY;
										}
										break;
									case 2:
										{
											sidej = Imbalance::SELL;
										}
										break;
									default:
										{
											sidej = Imbalance::NONE;
										}
										break;
									}

									Imbalance::ImbType typej = Imbalance::UNKNOWN;
									switch(row.colVals[30].i32Val.value)
									{
									case 1:
										typej = Imbalance::OPEN;
										break;
									case 2:
										typej = Imbalance::MARKET;
										break;
									case 3:
										typej = Imbalance::HALT;
										break;
									case 4:
										typej = Imbalance::CLOSE;
										break;
									case 5:
										typej = Imbalance::NO_IMBALANCE;
										break;
									}
									// construct imbalance
									imb.date = boost::gregorian::from_undelimited_string(std::to_string((_Longlong)row.colVals[0].i32Val.value));
									imb.symbol = row.colVals[2].stringVal.value;
									imb.time = boost::posix_time::millisec(row.colVals[1].doubleVal.value);
									imb.clearing_price = row.colVals[19].doubleVal.value;
									imb.far_price = row.colVals[22].doubleVal.value;
									imb.imbalance_type = typej;
									imb.market_imbalance = row.colVals[23].i32Val.value;
									imb.near_price = row.colVals[24].doubleVal.value;
									imb.paired_shares = row.colVals[25].i32Val.value;
									imb.ref_price = row.colVals[26].doubleVal.value;
									imb.side = sidej;
									imb.total_imbalance = row.colVals[29].i32Val.value;
									imb.flag = row.colVals[3].stringVal.value;
									//imb.tag=row.colVals[14].i32Val.value;
									// notify handler
									pHandler->OnImbalance(imb);
								}
							}
							#pragma endregion

                            #pragma region union type TI

							if (unionType == "TI")
							{

								if (row.colVals[4].stringVal.value == "T")
								{
									Trade trd;
									// construct trade
									trd.date = boost::gregorian::from_undelimited_string(std::to_string((_Longlong)row.colVals[0].i32Val.value));
									trd.symbol = row.colVals[2].stringVal.value;
									trd.time = boost::posix_time::millisec(row.colVals[1].doubleVal.value);
									trd.exg = row.colVals[16].i32Val.value;
									trd.high = row.colVals[8].doubleVal.value;
									trd.last = row.colVals[10].doubleVal.value;
									trd.low = row.colVals[9].doubleVal.value;
									trd.open = row.colVals[7].doubleVal.value;
									trd.price = row.colVals[6].doubleVal.value;
									trd.size = row.colVals[11].i32Val.value;
									trd.volume = row.colVals[18].i64Val.value;
									// notify handler
									pHandler->OnTrade(trd);
								}
								else
								{
									Imbalance imb;
									Imbalance::Side sidej = Imbalance::NONE;
									switch(row.colVals[27].i32Val.value)
									{
									case 1:
										{
											sidej = Imbalance::BUY;
										}
										break;
									case 2:
										{
											sidej = Imbalance::SELL;
										}
										break;
									default:
										{
											sidej = Imbalance::NONE;
										}
										break;
									}

									Imbalance::ImbType typej = Imbalance::UNKNOWN;
									switch(row.colVals[30].i32Val.value)
									{
									case 1:
										typej = Imbalance::OPEN;
										break;
									case 2:
										typej = Imbalance::MARKET;
										break;
									case 3:
										typej = Imbalance::HALT;
										break;
									case 4:
										typej = Imbalance::CLOSE;
										break;
									case 5:
										typej = Imbalance::NO_IMBALANCE;
										break;
									}
									// construct imbalance
									imb.date = boost::gregorian::from_undelimited_string(std::to_string((_Longlong)row.colVals[0].i32Val.value));
									imb.symbol = row.colVals[2].stringVal.value;
									imb.time = boost::posix_time::millisec(row.colVals[1].doubleVal.value);
									imb.clearing_price = row.colVals[19].doubleVal.value;
									imb.far_price = row.colVals[22].doubleVal.value;
									imb.imbalance_type = typej;
									imb.market_imbalance = row.colVals[23].i32Val.value;
									imb.near_price = row.colVals[24].doubleVal.value;
									imb.paired_shares = row.colVals[25].i32Val.value;
									imb.ref_price = row.colVals[26].doubleVal.value;
									imb.side = sidej;
									imb.total_imbalance = row.colVals[29].i32Val.value;
									imb.flag = row.colVals[3].stringVal.value;
									//imb.tag=row.colVals[14].i32Val.value;
									// notify handler
									pHandler->OnImbalance(imb);
								}
							}
							#pragma endregion

                            #pragma region union type QI

							if ( unionType == "QI")
							{
								if (row.colVals[4].stringVal.value == "Q" )
								{
									Quote quo;
									std::istringstream buffer(row.colVals[20].stringVal.value);
									std::istringstream buffer2(row.colVals[23].stringVal.value);
									// construct quote
									quo.date = boost::gregorian::from_undelimited_string(std::to_string((_Longlong)row.colVals[0].i32Val.value));
									quo.symbol = row.colVals[2].stringVal.value;
									quo.time = boost::posix_time::millisec(row.colVals[1].doubleVal.value);
									quo.ask = row.colVals[16].doubleVal.value;
									quo.ask_size = row.colVals[17].i32Val.value;
									quo.best_ask = row.colVals[19].doubleVal.value;
									buffer >> quo.best_ask_exg;
									quo.best_ask_size = row.colVals[21].i32Val.value;
									quo.best_bid = row.colVals[22].doubleVal.value;
									buffer2 >> quo.best_bid_exg;
									quo.best_bid_size = row.colVals[24].i32Val.value;
									quo.bid = row.colVals[25].doubleVal.value;
									quo.bid_size = row.colVals[26].i32Val.value;
									// notify handler
									pHandler->OnQuote(quo);
								}
								else 
								{
									Imbalance imb;
									Imbalance::Side sidej = Imbalance::NONE;
									switch(row.colVals[12].i32Val.value)
									{
									case 1:
										{
											sidej = Imbalance::BUY;
										}
										break;
									case 2:
										{
											sidej = Imbalance::SELL;
										}
										break;
									default:
										{
											sidej = Imbalance::NONE;
										}
										break;
									}

									Imbalance::ImbType typej = Imbalance::UNKNOWN;
									switch(row.colVals[15].i32Val.value)
									{
									case 1:
										typej = Imbalance::OPEN;
										break;
									case 2:
										typej = Imbalance::MARKET;
										break;
									case 3:
										typej = Imbalance::HALT;
										break;
									case 4:
										typej = Imbalance::CLOSE;
										break;
									case 5:
										typej = Imbalance::NO_IMBALANCE;
										break;
									}
									// construct imbalance
									imb.date = boost::gregorian::from_undelimited_string(std::to_string((_Longlong)row.colVals[0].i32Val.value));
									imb.symbol = row.colVals[2].stringVal.value;
									imb.time = boost::posix_time::millisec(row.colVals[1].doubleVal.value);
									imb.clearing_price = row.colVals[4].doubleVal.value;
									imb.far_price = row.colVals[7].doubleVal.value;
									imb.imbalance_type = typej;
									imb.market_imbalance = row.colVals[8].i32Val.value;
									imb.near_price = row.colVals[9].doubleVal.value;
									imb.paired_shares = row.colVals[10].i32Val.value;
									imb.ref_price = row.colVals[11].doubleVal.value;
									imb.side = sidej;
									imb.total_imbalance = row.colVals[14].i32Val.value;
									imb.flag = row.colVals[3].stringVal.value;
									//imb.tag=row.colVals[14].i32Val.value;
									// notify handler
									pHandler->OnImbalance(imb);
								}
							}
                             #pragma endregion

							#pragma region union type TQ

							if (unionType == "TQ")
							{
								if (row.colVals[4].stringVal.value == "T")
								{
									Trade trd;
									// construct trade
									trd.date = boost::gregorian::from_undelimited_string(std::to_string((_Longlong)row.colVals[1].i32Val.value));
									trd.symbol = row.colVals[3].stringVal.value;
									trd.time = boost::posix_time::millisec(row.colVals[2].i32Val.value);
									trd.exg = row.colVals[17].i32Val.value;
									trd.high = row.colVals[9].doubleVal.value;
									trd.last = row.colVals[11].doubleVal.value;
									trd.low = row.colVals[10].doubleVal.value;
									trd.open = row.colVals[8].doubleVal.value;
									trd.price = row.colVals[7].doubleVal.value;
									trd.size = row.colVals[12].i32Val.value;
									trd.volume = row.colVals[19].i64Val.value;
									// notify handler
									pHandler->OnTrade(trd);
								}
								else
								{
									Quote quo;
									//std::istringstream buffer(row.colVals[23].stringVal.value);
									//std::istringstream buffer2(row.colVals[26].stringVal.value);
									// construct quote
									quo.date = boost::gregorian::from_undelimited_string(std::to_string((_Longlong)row.colVals[1].i32Val.value));
									quo.symbol = row.colVals[3].stringVal.value;
									quo.time = boost::posix_time::millisec(row.colVals[2].i32Val.value);
									quo.ask = row.colVals[20].doubleVal.value;
									quo.ask_size = row.colVals[21].i32Val.value;
									quo.best_ask = row.colVals[23].doubleVal.value;
									//buffer >> quo.best_ask_exg;
									quo.best_ask_exg=row.colVals[24].i32Val.value;
									quo.best_ask_size = row.colVals[25].i32Val.value;
									quo.best_bid = row.colVals[26].doubleVal.value;
									//buffer2 >> quo.best_bid_exg;
									quo.best_bid_exg=row.colVals[27].i32Val.value;
									quo.best_bid_size = row.colVals[28].i32Val.value;
									quo.bid = row.colVals[29].doubleVal.value;
									quo.bid_size = row.colVals[30].i32Val.value;
									// notify handler
									pHandler->OnQuote(quo);
								}
							}

							#pragma endregion

                            #pragma region union type I

							if (unionType == "I")
							{
								Imbalance imb;
								Imbalance::Side sidej = Imbalance::NONE;
								switch(row.colVals[12].i32Val.value)
								{
								case 1:
									{
										sidej = Imbalance::BUY;
									}
									break;
								case 2:
									{
										sidej = Imbalance::SELL;
									}
									break;
								default:
									{
										sidej = Imbalance::NONE;
									}
									break;
								}

								Imbalance::ImbType typej = Imbalance::UNKNOWN;
								switch(row.colVals[15].i32Val.value)
								{
								case 1:
									typej = Imbalance::OPEN;
									break;
								case 2:
									typej = Imbalance::MARKET;
									break;
								case 3:
									typej = Imbalance::HALT;
									break;
								case 4:
									typej = Imbalance::CLOSE;
									break;
								case 5:
									typej = Imbalance::NO_IMBALANCE;
									break;
								}
								// construct imbalance
								imb.date = boost::gregorian::from_undelimited_string(std::to_string((_Longlong)row.colVals[0].i32Val.value));
								imb.symbol = row.colVals[2].stringVal.value;
								imb.time = boost::posix_time::millisec(row.colVals[1].doubleVal.value);
								imb.clearing_price = row.colVals[4].doubleVal.value;
								imb.far_price = row.colVals[7].doubleVal.value;
								imb.imbalance_type = typej;
								imb.market_imbalance = row.colVals[8].i32Val.value;
								imb.near_price = row.colVals[9].doubleVal.value;
								imb.paired_shares = row.colVals[10].i32Val.value;
								imb.ref_price = row.colVals[11].doubleVal.value;
								imb.side = sidej;
								imb.total_imbalance = row.colVals[14].i32Val.value;
								imb.flag = row.colVals[3].stringVal.value;
								//imb.tag=row.colVals[14].i32Val.value;
								// notify handler
								pHandler->OnImbalance(imb);
							}

                            #pragma endregion
						}
						fetchReq.orientation = TFetchOrientation::FETCH_NEXT;
					}
					while(bmore);

                   #pragma endregion
				}
			 }
            #pragma endregion

            #pragma region generate union query

			 #pragma region union trade query
			 std::string createUnionTradeQuery(const boost::gregorian::date &dt, 
				const boost::posix_time::time_duration &start,
				const boost::posix_time::time_duration &end,
				const std::vector<std::string> &symbols,
				std::string unionType)
			  {
				// date format
				boost::gregorian::date_facet *dfacet = new boost::gregorian::date_facet("%Y%m%d");
				std::ostringstream oss;
				oss.imbue(std::locale(oss.getloc(), dfacet));

				// query
                #pragma region generate query

				oss << "select seqno,day,time, symbol ,'T' as flag,conditionflags,exchangesequence,"
					<< "price,open,high,low,last,size,exchangetime,listedexchange,netchange,priceflags,reportingexchange,tradecondition,totalvolume,";
				if (unionType == "TI")
				{
					oss	<< "0.0 as clearing_price,0 as exg_specific_info,0 as exg_time,0.0 as far_price,0 as market_imbalance,"
						<< "0.0 as near_price,0 as paired_shares,0.0 as ref_price,0 as side,'' as source,0 as total_imbalance,0 as type";
				}
				if (unionType == "TQI")
				{
					oss	<< "0.0 as clearing_price,0 as exg_specific_info,0 as exg_time,0.0 as far_price,0 as market_imbalance,"
						<< "0.0 as near_price,0 as paired_shares,0.0 as ref_price,0 as side,'' as source,0 as total_imbalance,0 as type,"
						<< "0.0 as ask,0 as asksize,0 as bbochangeflags,0.0 as bestask,"
						<< "0 as bestaskexchange,0 as bestasksize,0.0 as bestbid,0 as bestbidexchange,0 as bestbidsize,0.0 as bid,0 as bidsize,0 as quotecondition";
				}
				if (unionType == "TQ")
				{
					oss << "0.0 as ask,0 as asksize,0 as bbochangeflags,0.0 as bestask,"
						<< "0 as bestaskexchange,0 as bestasksize,0.0 as bestbid,0 as bestbidexchange,0 as bestbidsize,0.0 as bid,0 as bidsize,0 as quotecondition";
				}
				oss	<<" from marketdata.trades as T where day = "
					<< dt;
				if(start.total_milliseconds() != 0)
				{
					oss	<< " and T.time >= " 
						<< start.total_milliseconds();
				}
				if(end.total_milliseconds() != 0)
				{
					oss	<< " and T.time <= " 
						<< end.total_milliseconds();
				}
				if (symbols.size() != 0)
				{
					std::string symbolsArray;
					BOOST_FOREACH (std::string s, symbols)
					{
						symbolsArray = symbolsArray + "','" + s;
					}
					symbolsArray = &symbolsArray[2];
					symbolsArray = "(" + symbolsArray + "')";
					oss	<< " and symbol in " 
						<< symbolsArray;
				}

				const std::string query = oss.str(); 

                #pragma endregion

#if TEST
				std::cout << query << std::endl;
#endif
				return query;
			  }
            #pragma endregion

			 #pragma region union NYSE query

			std::string createUnionNyseQuery(const boost::gregorian::date &dt, 
				const boost::posix_time::time_duration &start,
				const boost::posix_time::time_duration &end,
				const std::vector<std::string> &symbols,
				std::string unionType)
			{
				// date format
				boost::gregorian::date_facet *dfacet = new boost::gregorian::date_facet("%Y%m%d");
				std::ostringstream oss;
				oss.imbue(std::locale(oss.getloc(), dfacet));

				// query
				 #pragma region generate query
				oss << "select dt,time, symbol ,'NYSE' as flag,";
				if (unionType == "TI")
				{
					oss	<< "'' as condition_flags,0 as exg_sequence,0.0 as price,0.0 as open,0.0 as high,0.0 as low,0.0 as last,"
						<< "0 as size,0 as exg_timestamp,0 as listed_exg,0.0 as net_change,'' as price_flag,0 as reporting_exg,0 as trade_condition,0 as volume,"
						<< "clearing_price,exg_specific_info,exg_time,far_price,market_imbalance,near_price,paired_shares,ref_price,side,source,total_imbalance,type ";
				}
				if (unionType == "TQI")
				{
					oss	<< "'' as condition_flags,0 as exg_sequence,0.0 as price,0.0 as open,0.0 as high,0.0 as low,0.0 as last,"
						<< "0 as size,0 as exg_timestamp,0 as listed_exg,0.0 as net_change,'' as price_flag,0 as reporting_exg,0 as trade_condition,0 as volume,"
						<< "clearing_price,exg_specific_info,exg_time,far_price,market_imbalance,near_price,paired_shares,ref_price,side,source,total_imbalance,type,"
						<< "0.0 as ask,0 as ask_size,'' as bbo_change_flages,0.0 as best_ask,'' as best_ask_exg,0 as best_ask_size,0.0 as best_bid,"
						<< "'' as best_bid_exg,0 as best_bid_size,0.0 as bid,0 as bid_size,'' as quote_condition ";
				}
				if (unionType == "QI")
				{
					oss << "clearing_price,exg_specific_info,exg_time,far_price,market_imbalance,near_price,paired_shares,ref_price,side,source,total_imbalance,type,"
						<< "0.0 as ask,0 as ask_size,'' as bbo_change_flages,0.0 as best_ask,'' as best_ask_exg,0 as best_ask_size,0.0 as best_bid,'' as best_bid_exg,0 as best_bid_size,"
						<< "0.0 as bid,0 as bid_size,0 as exg_timestamp,0 as listed_exg,0 as reporting_exg,'' as quote_condition ";
				}
				if (unionType == "I")
				{
					oss << "clearing_price,exg_specific_info,exg_time,far_price,market_imbalance,near_price,paired_shares,ref_price,side,source,total_imbalance,type ";
				}
				oss	<<" from marketdata.imb_nyse as I where I.day="
					<< dt;
				if(start.total_milliseconds() != 0)
				{
					oss	<< " and I.time >= " 
						<< start.total_milliseconds();
				}
				if(end.total_milliseconds() != 0)
				{
					oss	<< " and I.time <= " 
						<< end.total_milliseconds();
				}
				if (symbols.size() != 0)
				{
					std::string symbolsArray;
					BOOST_FOREACH (std::string s, symbols)
					{
						symbolsArray = symbolsArray + "','" + s;
					}
					symbolsArray = &symbolsArray[2];
					symbolsArray = "(" + symbolsArray + "')";
					oss	<< " and symbol in " 
						<< symbolsArray;
				}

				const std::string query = oss.str();
                #pragma endregion
#if TEST
				std::cout << query << std::endl;
#endif
				return query;


			}
			#pragma endregion

			 #pragma region union NASD query
			std::string createUnionNasdQuery(const boost::gregorian::date &dt, 
				const boost::posix_time::time_duration &start,
				const boost::posix_time::time_duration &end,
				const std::vector<std::string> &symbols,
				std::string unionType)
			{
				// date format
				boost::gregorian::date_facet *dfacet = new boost::gregorian::date_facet("%Y%m%d");
				std::ostringstream oss;
				oss.imbue(std::locale(oss.getloc(), dfacet));

				// query
				 #pragma region generate query
				oss << "select dt,time, symbol ,'NASD' as flag,";
				if (unionType == "TI")
				{
					oss	<< "'' as condition_flags,0 as exg_sequence,0.0 as price,0.0 as open,0.0 as high,0.0 as low,0.0 as last,"
						<< "0 as size,0 as exg_timestamp,0 as listed_exg,0.0 as net_change,'' as price_flag,0 as reporting_exg,0 as trade_condition,0 as volume,"
						<< "0.0 as clearing_price,exg_specific_info,exg_time,far_price,market_imbalance,near_price,paired_shares,ref_price,side,source,total_imbalance,type ";
				}
				if (unionType == "TQI")
				{
					oss	<< "'' as condition_flags,0 as exg_sequence,0.0 as price,0.0 as open,0.0 as high,0.0 as low,0.0 as last,"
						<< "0 as size,0 as exg_timestamp,0 as listed_exg,0.0 as net_change,'' as price_flag,0 as reporting_exg,0 as trade_condition,0 as volume,"
						<< "0.0 as clearing_price,exg_specific_info,exg_time,far_price,market_imbalance,near_price,paired_shares,ref_price,side,source,total_imbalance,type,"
						<< "0.0 as ask,0 as ask_size,'' as bbo_change_flages,0.0 as best_ask,'' as best_ask_exg,0 as best_ask_size,0.0 as best_bid,"
						<< "'' as best_bid_exg,0 as best_bid_size,0.0 as bid,0 as bid_size,'' as quote_condition ";
				}
				if (unionType == "QI")
				{
					oss << "0.0 as clearing_price,exg_specific_info,exg_time,far_price,market_imbalance,near_price,paired_shares,ref_price,side,source,total_imbalance,type,"
						<< "0.0 as ask,0 as ask_size,'' as bbo_change_flages,0.0 as best_ask,'' as best_ask_exg,0 as best_ask_size,0.0 as best_bid,'' as best_bid_exg,0 as best_bid_size,"
						<< "0.0 as bid,0 as bid_size,0 as exg_timestamp,0 as listed_exg,0 as reporting_exg,'' as quote_condition ";
				}
				if (unionType == "I")
				{
					oss << "0.0 as clearing_price,exg_specific_info,exg_time,far_price,market_imbalance,near_price,paired_shares,ref_price,side,source,total_imbalance,type ";
				}
				oss	<<" from marketdata.imb_nasd as I where I.day="
					<< dt;
				if(start.total_milliseconds() != 0)
				{
					oss	<< " and I.time >= " 
						<< start.total_milliseconds();
				}
				if(end.total_milliseconds() != 0)
				{
					oss	<< " and I.time <= " 
						<< end.total_milliseconds();
				}
				if (symbols.size() != 0)
				{
					std::string symbolsArray;
					BOOST_FOREACH (std::string s, symbols)
					{
						symbolsArray = symbolsArray + "','" + s;
					}
					symbolsArray = &symbolsArray[2];
					symbolsArray = "(" + symbolsArray + "')";
					oss	<< " and symbol in " 
						<< symbolsArray;
				}

				const std::string query = oss.str();
                #pragma endregion
#if TEST
				std::cout << query << std::endl;
#endif
				return query;


			}
			#pragma endregion
			 
			 #pragma region union AMEX query
			std::string createUnionAmexQuery(const boost::gregorian::date &dt, 
				const boost::posix_time::time_duration &start,
				const boost::posix_time::time_duration &end,
				const std::vector<std::string> &symbols,
				std::string unionType)
			{
				// date format
				boost::gregorian::date_facet *dfacet = new boost::gregorian::date_facet("%Y%m%d");
				std::ostringstream oss;
				oss.imbue(std::locale(oss.getloc(), dfacet));

				// query
				 #pragma region generate query
				oss << "select dt,time, symbol ,'AMEX' as flag,";
				if (unionType == "TI")
				{
					oss	<< "'' as condition_flags,0 as exg_sequence,0.0 as price,0.0 as open,0.0 as high,0.0 as low,0.0 as last,"
						<< "0 as size,0 as exg_timestamp,0 as listed_exg,0.0 as net_change,'' as price_flag,0 as reporting_exg,0 as trade_condition,0 as volume,"
						<< "clearing_price,exg_specific_info,exg_time,far_price,market_imbalance,near_price,paired_shares,ref_price,side,source,total_imbalance,type ";
				}
				if (unionType == "TQI")
				{
					oss	<< "'' as condition_flags,0 as exg_sequence,0.0 as price,0.0 as open,0.0 as high,0.0 as low,0.0 as last,"
						<< "0 as size,0 as exg_timestamp,0 as listed_exg,0.0 as net_change,'' as price_flag,0 as reporting_exg,0 as trade_condition,0 as volume,"
						<< "clearing_price,exg_specific_info,exg_time,far_price,market_imbalance,near_price,paired_shares,ref_price,side,source,total_imbalance,type,"
						<< "0.0 as ask,0 as ask_size,'' as bbo_change_flages,0.0 as best_ask,'' as best_ask_exg,0 as best_ask_size,0.0 as best_bid,"
						<< "'' as best_bid_exg,0 as best_bid_size,0.0 as bid,0 as bid_size,'' as quote_condition ";
				}
				if (unionType == "QI")
				{
					oss << "clearing_price,exg_specific_info,exg_time,far_price,market_imbalance,near_price,paired_shares,ref_price,side,source,total_imbalance,type,"
						<< "0.0 as ask,0 as ask_size,'' as bbo_change_flages,0.0 as best_ask,'' as best_ask_exg,0 as best_ask_size,0.0 as best_bid,'' as best_bid_exg,0 as best_bid_size,"
						<< "0.0 as bid,0 as bid_size,0 as exg_timestamp,0 as listed_exg,0 as reporting_exg,'' as quote_condition ";
				}
				if (unionType == "I")
				{
					oss << "clearing_price,exg_specific_info,exg_time,far_price,market_imbalance,near_price,paired_shares,ref_price,side,source,total_imbalance,type ";
				}
				oss	<<" from marketdata.imb_amex as I where I.day="
					<< dt;
				if(start.total_milliseconds() != 0)
				{
					oss	<< " and I.time >= " 
						<< start.total_milliseconds();
				}
				if(end.total_milliseconds() != 0)
				{
					oss	<< " and I.time <= " 
						<< end.total_milliseconds();
				}
				if (symbols.size() != 0)
				{
					std::string symbolsArray;
					BOOST_FOREACH (std::string s, symbols)
					{
						symbolsArray = symbolsArray + "','" + s;
					}
					symbolsArray = &symbolsArray[2];
					symbolsArray = "(" + symbolsArray + "')";
					oss	<< " and symbol in " 
						<< symbolsArray;
				}

				const std::string query = oss.str();
                #pragma endregion
#if TEST
				std::cout << query << std::endl;
#endif
				return query;


			}
			#pragma endregion

			 #pragma region union ARCA query
			std::string createUnionArcaQuery(const boost::gregorian::date &dt, 
				const boost::posix_time::time_duration &start,
				const boost::posix_time::time_duration &end,
				const std::vector<std::string> &symbols,
				std::string unionType)
			{
				// date format
				boost::gregorian::date_facet *dfacet = new boost::gregorian::date_facet("%Y%m%d");
				std::ostringstream oss;
				oss.imbue(std::locale(oss.getloc(), dfacet));

				// query
				 #pragma region generate query
				oss << "select dt,time, symbol ,'ARCA' as flag,";
				if (unionType == "TI")
				{
					oss	<< "'' as condition_flags,0 as exg_sequence,0.0 as price,0.0 as open,0.0 as high,0.0 as low,0.0 as last,"
						<< "0 as size,0 as exg_timestamp,0 as listed_exg,0.0 as net_change,'' as price_flag,0 as reporting_exg,0 as trade_condition,0 as volume,"
						<< "clearing_price,exg_specific_info,exg_time,far_price,market_imbalance,near_price,paired_shares,ref_price,side,source,total_imbalance,type ";
				}
				if (unionType == "TQI")
				{
					oss	<< "'' as condition_flags,0 as exg_sequence,0.0 as price,0.0 as open,0.0 as high,0.0 as low,0.0 as last,"
						<< "0 as size,0 as exg_timestamp,0 as listed_exg,0.0 as net_change,'' as price_flag,0 as reporting_exg,0 as trade_condition,0 as volume,"
						<< "clearing_price,exg_specific_info,exg_time,far_price,market_imbalance,near_price,paired_shares,ref_price,side,source,total_imbalance,type,"
						<< "0.0 as ask,0 as ask_size,'' as bbo_change_flages,0.0 as best_ask,'' as best_ask_exg,0 as best_ask_size,0.0 as best_bid,"
						<< "'' as best_bid_exg,0 as best_bid_size,0.0 as bid,0 as bid_size,'' as quote_condition ";
				}
				if (unionType == "QI")
				{
					oss << "clearing_price,exg_specific_info,exg_time,far_price,market_imbalance,near_price,paired_shares,ref_price,side,source,total_imbalance,type,"
						<< "0.0 as ask,0 as ask_size,'' as bbo_change_flages,0.0 as best_ask,'' as best_ask_exg,0 as best_ask_size,0.0 as best_bid,'' as best_bid_exg,0 as best_bid_size,"
						<< "0.0 as bid,0 as bid_size,0 as exg_timestamp,0 as listed_exg,0 as reporting_exg,'' as quote_condition ";
				}
				if (unionType == "I")
				{
					oss << "clearing_price,exg_specific_info,exg_time,far_price,market_imbalance,near_price,paired_shares,ref_price,side,source,total_imbalance,type ";
				}
				oss	<<" from marketdata.imb_arca as I where I.day="
					<< dt;
				if(start.total_milliseconds() != 0)
				{
					oss	<< " and I.time >= " 
						<< start.total_milliseconds();
				}
				if(end.total_milliseconds() != 0)
				{
					oss	<< " and I.time <= " 
						<< end.total_milliseconds();
				}
				if (symbols.size() != 0)
				{
					std::string symbolsArray;
					BOOST_FOREACH (std::string s, symbols)
					{
						symbolsArray = symbolsArray + "','" + s;
					}
					symbolsArray = &symbolsArray[2];
					symbolsArray = "(" + symbolsArray + "')";
					oss	<< " and symbol in " 
						<< symbolsArray;
				}

				const std::string query = oss.str();
                #pragma endregion
#if TEST
				std::cout << query << std::endl;
#endif
				return query;


			}
			#pragma endregion

             #pragma region union Quote query
			std::string createUnionQuoteQuery(const boost::gregorian::date &dt, 
				const boost::posix_time::time_duration &start,
				const boost::posix_time::time_duration &end,
				const std::vector<std::string> &symbols,
				std::string unionType)
			{
				// date format
				boost::gregorian::date_facet *dfacet = new boost::gregorian::date_facet("%Y%m%d");
				std::ostringstream oss;
				oss.imbue(std::locale(oss.getloc(), dfacet));

				// query
				 #pragma region generate query

				oss << "select seqno,day,time, symbol ,'Q' as flag,";
				if (unionType == "QI")
				{
					oss	<< "0.0 as clearing_price,0 as exg_specific_info,0 as exg_time,0.0 as far_price,0 as market_imbalance,0.0 as near_price,"
						<< "0 as paired_shares,0.0 as ref_price,0 as side,'' as source,0 as total_imbalance,0 as type,ask,asksize,bbochangeflags,"
						<< "bestask,bestaskexchange,bestasksize,bestbid,bestbidexchange,bestbidsize,bid,bidsize,exchangetime,listedexchange,reportingexchange,quotecondition";
				}
				if (unionType == "TQI")
				{
					oss	<< "0 as conditionflags,0 as exchangesequence,0.0 as price,0.0 as open,0.0 as high,0.0 as low,0.0 as last,0 as size,exchangetime,listedexchange,"
						<< "0.0 as net_change,'' as price_flag,reporting_exg,0 as trade_condition,0 as volume,0.0 as clearing_price,0 as exg_specific_info,"
						<< "0 as exg_time,0.0 as far_price,0 as market_imbalance,0.0 as near_price,0 as paired_shares,0.0 as ref_price,0 as side,'' as source,"
						<< "0 as total_imbalance,0 as type,ask,asksize,bbochangeflags,bestask,bestaskexchange,bestasksize,bestbid,bestbidexchange,bestbidsize,bid,bidsize,quotecondition";
				}
				if (unionType == "TQ")
				{
					oss << "0 as conditionflags,0 as exchangesequence,0.0 as price,0.0 as open,0.0 as high,0.0 as low,0.0 as last,0 as size,exchangetime,listedexchange,"
						<< "0.0 as netchange,0 as priceflags,reportingexchange,0 as tradecondition,0 as totalvolume,ask,asksize,bbochangeflags,bestask,bestaskexchange,"
						<< "bestasksize,bestbid,bestbidexchange,bestbidsize,bid,bidsize,quotecondition";
				}
				oss	<<" from marketdata.quotes as Q where day = "
					<< dt;
				if(start.total_milliseconds() != 0)
				{
					oss	<< " and Q.time >= " 
						<< start.total_milliseconds();
				}
				if(end.total_milliseconds() != 0)
				{
					oss	<< " and Q.time <= " 
						<< end.total_milliseconds();
				}
				if (symbols.size() != 0)
				{
					std::string symbolsArray;
					BOOST_FOREACH (std::string s, symbols)
					{
						symbolsArray = symbolsArray + "','" + s;
					}
					symbolsArray = &symbolsArray[2];
					symbolsArray = "(" + symbolsArray + "')";
					oss	<< " and symbol in " 
						<< symbolsArray;
				}

				const std::string query = oss.str();
                #pragma endregion
#if TEST
				std::cout << query << std::endl;
#endif
				return query;


			}
			#pragma endregion

             #pragma endregion
		};

		Client::Client(const std::string &user, const std::string &password, const std::string &server)
			: user_(user)
			, password_(password)
			, server_(server)
		{
		}

		Client::~Client()
		{
		}

        #pragma region Subscribe

		void Client::Subscribe(const SubscriptionFlags flags,
			const std::vector<std::string> &symbols,
			const boost::gregorian::date &start, 
			const boost::gregorian::date &end,
			const boost::posix_time::time_duration &begin,
			const boost::posix_time::time_duration &finish)
		{
			// convert int to string
				std::stringstream ss;
				ss<<Maxrow;
			    std::string max;
			    ss>>max;

            //grantee some condition  
			assert(start <= end);
			assert(begin <= finish);

           #pragma region get the union type
#if TEST
			std::ostringstream oss;
#endif
			std::vector<SubscriptionFlags> tickTypeSet;
			std::string unionType;

			bool bFirst = false;
			bool imbExist = false;
			if(flags & TRADE)
			{
#if TEST
				bFirst = true;
				oss << "TRADE";
#endif
				tickTypeSet.push_back(TRADE);
				unionType = "T";
			}
			if(flags & QUOTE)
			{
#if TEST
				oss <<  ((bFirst) ? "|" : "") << "QUOTE";
				bFirst = true;
#endif
				tickTypeSet.push_back(QUOTE);
				unionType = unionType + "Q";
			}
			if(flags & NYSE_IMBALANCE)
			{
#if TEST
				oss <<  ((bFirst) ? "|" : "") << "NYSE_IMBALANCE";
				bFirst = true;
#endif
				tickTypeSet.push_back(NYSE_IMBALANCE);
				unionType = unionType + "I";
				imbExist = true;
			}
			if(flags & NASD_IMBALANCE)
			{
#if TEST
				oss <<  ((bFirst) ? "|" : "") << "NASD_IMBALANCE";
				bFirst = true;
#endif
				tickTypeSet.push_back(NASD_IMBALANCE);
				if (imbExist == false)
				{
					unionType = unionType + "I";
					imbExist = true;
				}

			}
			if(flags & AMEX_IMBALANCE)
			{
#if TEST
				oss <<  ((bFirst) ? "|" : "") << "AMEX_IMBALANCE";
				bFirst = true;
#endif
				tickTypeSet.push_back(AMEX_IMBALANCE);
				if (imbExist == false)
				{
					unionType = unionType + "I";
					imbExist = true;
				}
			}
			if(flags & ARCA_IMBALANCE)
			{
#if TEST
				oss <<  ((bFirst) ? "|" : "") << "ARCA_IMBALANCE";
				bFirst = true;
#endif
				tickTypeSet.push_back(ARCA_IMBALANCE);
				if (imbExist == false)
				{
					unionType = unionType + "I";
					imbExist = true;
				}
			}
			if(flags & DAILY)
			{
#if TEST
				oss <<  ((bFirst) ? "|" : "") << "DAILY";
#endif
				tickTypeSet.push_back(DAILY);
			}
			if(flags & EXGPRINTS)
			{
#if TEST
				oss <<  ((bFirst) ? "|" : "") << "Exgprints";
#endif
				tickTypeSet.push_back(EXGPRINTS);
			}
				if(flags & NEWS)
			{
#if TEST
				oss <<  ((bFirst) ? "|" : "") << "News";
#endif
				tickTypeSet.push_back(NEWS);
			}


#if TEST
			std::cout << "Querying: " << start << " to " << end
				<< " between " << begin << " and " << finish
				<< " for " << oss.str()
				<< std::endl;
#endif	
            #pragma endregion

			try
			{
				// create connectors
				boost::shared_ptr<TTransport> pSocket = boost::make_shared<TSocket>(server_, 5049);
				boost::shared_ptr<TTransport> pTransport = boost::make_shared<TBufferedTransport>(pSocket);
				boost::shared_ptr<TProtocol> pProtocol = boost::make_shared<TBinaryProtocol>(pTransport);
				boost::shared_ptr<TCLIServiceClient> pCli = boost::make_shared<TCLIServiceClient>(pProtocol);

				// open transport
				pTransport->open();
                #pragma region open session
				try
				{
					// open session
					TOpenSessionReq req;
					req.client_protocol = TProtocolVersion::HIVE_CLI_SERVICE_PROTOCOL_V1;
					req.username = user_;
					req.password = password_;

					TOpenSessionResp openResp;
					pCli->OpenSession(openResp, req);

					if (openResp.status.statusCode == TStatusCode::SUCCESS_STATUS || openResp.status.statusCode == TStatusCode::SUCCESS_WITH_INFO_STATUS)
					{
						TSessionHandle sessHandle = openResp.sessionHandle;

#if TEST
						std::cout << "Connected..." << std::endl;
#endif
						static boost::scoped_ptr<Impl> pImpl(new Impl());

						if(pImpl)
						{

                          #pragma region only one type of tick data

							if(tickTypeSet.size()==1)
							{
								switch (flags)
								{
								case TRADE:
									{
										// loop through dates
										boost::gregorian::day_iterator dt(start);
										for(; dt <= end; ++dt)
										{
											pImpl->doFetchTrades2(
												this,
												pCli, 
												sessHandle,
												*dt,
												begin,
												finish,
												symbols);
										}
										break;
									}

								case DAILY:
									{
										// loop through dates
										boost::gregorian::day_iterator dt(start);
										for(; dt <= end; ++dt)
										{
											pImpl->doFetchDaily(
												this,
												pCli, 
												sessHandle,
												*dt,
												begin,
												finish,
												symbols);
										}
										break;
									}
									case EXGPRINTS:
									{
										// loop through dates
										boost::gregorian::day_iterator dt(start);
										for(; dt <= end; ++dt)
										{
											pImpl->doFetchExgprints(
												this,
												pCli, 
												sessHandle,
												*dt,
												begin,
												finish,
												symbols);
										}
										break;
									}
									case NEWS:
									{
										// loop through dates
										boost::gregorian::day_iterator dt(start);
										for(; dt <= end; ++dt)
										{
											pImpl->doFetchNews(
												this,
												pCli, 
												sessHandle,
												*dt,
												begin,
												finish,
												symbols);
										}
										break;
									}
								case NYSE_IMBALANCE:
									{
										// loop through dates
										boost::gregorian::day_iterator dt(start);
										for(; dt <= end; ++dt)
										{
											pImpl->doFetchNYSE(
												this,
												pCli, 
												sessHandle,
												*dt,
												begin,
												finish,
												symbols);
										}
										break;
									}
								case NASD_IMBALANCE:
									{
										// loop through dates
										boost::gregorian::day_iterator dt(start);
										for(; dt <= end; ++dt)
										{
											pImpl->doFetchNASD(
												this,
												pCli, 
												sessHandle,
												*dt,
												begin,
												finish,
												symbols);
										}
										break;
									}
								case AMEX_IMBALANCE:
									{
										// loop through dates
										boost::gregorian::day_iterator dt(start);
										for(; dt <= end; ++dt)
										{
											pImpl->doFetchAMEX(
												this,
												pCli, 
												sessHandle,
												*dt,
												begin,
												finish,
												symbols);
										}
										break;
									}
								case ARCA_IMBALANCE:
									{
										// loop through dates
										boost::gregorian::day_iterator dt(start);
										for(; dt <= end; ++dt)
										{
											pImpl->doFetchARCA(
												this,
												pCli, 
												sessHandle,
												*dt,
												begin,
												finish,
												symbols);
										}
										break;
									}
								case QUOTE:
									{
										// loop through dates
										boost::gregorian::day_iterator dt(start);
										for(; dt <= end; ++dt)
										{
											pImpl->doFetchQuotes(
												this,
												pCli, 
												sessHandle,
												*dt,
												begin,
												finish,
												symbols);
										}
										break;
									} 

								}
							}
                            #pragma endregion

                          #pragma region union type tick data

							else if (tickTypeSet.size()!=0)
							{ 

								boost::gregorian::day_iterator dt(start);
								for(; dt <= end; ++dt)
								{
									std::string query;
									bool firstUnion = false; 
									BOOST_FOREACH (SubscriptionFlags tickType, tickTypeSet)
									{
										switch (tickType)
										{
										case TRADE:
											{ 
												if (firstUnion==true)
												{
													query = query + " union all " + pImpl->createUnionTradeQuery(
														*dt,
														begin,
														finish,
														symbols,
														unionType);
												}
												else
												{
													query = pImpl->createUnionTradeQuery(
														*dt,
														begin,
														finish,
														symbols,
														unionType);
												}
												firstUnion = true;
												break;
											}
										case DAILY:
											{
												pImpl->doFetchDaily(
													this,
													pCli, 
													sessHandle,
													*dt,
													begin,
													finish,
													symbols);
												break;
											}
										case NYSE_IMBALANCE:
											{
												if (firstUnion==true)
												{
													query = query + " union all " + pImpl->createUnionNyseQuery(
														*dt,
														begin,
														finish,
														symbols,
														unionType);
												}
												else
												{
													query = pImpl->createUnionNyseQuery(
														*dt,
														begin,
														finish,
														symbols,
														unionType);
												}
												firstUnion = true;
												break;
											}
										case NASD_IMBALANCE:
											{
												if (firstUnion==true)
												{
													query = query + " union all " + pImpl->createUnionNasdQuery(
														*dt,
														begin,
														finish,
														symbols,
														unionType);
												}
												else
												{
													query = pImpl->createUnionNasdQuery(
														*dt,
														begin,
														finish,
														symbols,
														unionType);
												}
												firstUnion = true;
												break;
											}
										case AMEX_IMBALANCE:
											{
												if (firstUnion==true)
												{
													query = query + " union all " + pImpl->createUnionAmexQuery(
														*dt,
														begin,
														finish,
														symbols,
														unionType);
												}
												else
												{
													query = pImpl->createUnionAmexQuery(
														*dt,
														begin,
														finish,
														symbols,
														unionType);
												}
												firstUnion = true;
												break;
											}
										case ARCA_IMBALANCE:
											{
												if (firstUnion==true)
												{
													query = query + " union all " + pImpl->createUnionArcaQuery(
														*dt,
														begin,
														finish,
														symbols,
														unionType);
												}
												else
												{
													query = pImpl->createUnionArcaQuery(
														*dt,
														begin,
														finish,
														symbols,
														unionType);
												}
												firstUnion = true;
												break;
											}
										case QUOTE:
											{
												if (firstUnion==true)
												{
													query = query + " union all " + pImpl->createUnionQuoteQuery(
														*dt,
														begin,
														finish,
														symbols,
														unionType);
												}
												else
												{
													query = pImpl->createUnionQuoteQuery(
														*dt,
														begin,
														finish,
														symbols,
														unionType);
												}
												firstUnion = true;
												break;
											} 
										}
									}
								
									query = "select * from ( " + query + " ) log order by log.seqno limit " + max;
#if TEST
									std::cout << query << std::endl;
#endif
									pImpl->doFetchUnion(
										this,
										pCli, 
										sessHandle,
										query,
										unionType);

								}
							}
                            #pragma endregion
						}

						// close session
						TCloseSessionReq closeReq;
						closeReq.sessionHandle = sessHandle;

						TCloseSessionResp closeResp;
						pCli->CloseSession(closeResp, closeReq);
					}

					// close transport
					pTransport->close();
				}
                #pragma endregion
				catch(const std::exception &ex)
				{
					if(pTransport)
						pTransport->close();

					throw;
				}
			}
			catch(const std::exception &ex)
			{
			}
			catch(...)
			{
			}
		}
        #pragma endregion

        #pragma region SubscribeALL

		void Client::SubscribeAll(const SubscriptionFlags flags,
			const boost::gregorian::date &start, 
			const boost::gregorian::date &end,
			const boost::posix_time::time_duration &begin,
			const boost::posix_time::time_duration &finish)
		{
			static std::vector<std::string> emptyv;
			Subscribe(flags, emptyv, start, end, begin, finish);
		}
        #pragma endregion
	}
}