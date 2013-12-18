#!/usr/bin/python


class SubscriptionFlags:
    DAILY = 1
    TRADE = 2 
    QUOTE = 4 
    NYSE_IMBALANCE = 8
    NASD_IMBALANCE = 16 
    AMEX_IMBALANCE = 32 
    ARCA_IMBALANCE = 64 
    EXGPRINTS = 128   
    NEWS = 256   
    ENUM = [1,2,4,8,16,32,64,128,256]

class Side:
    NONE = 0
    BUY = 1
    SELL = 2

class ImbType:
    UNKNOWN = 0
    OPEN = 1
    MARKET = 2
    HALT = 3
    CLOSE = 4
    NO_IMBALANCE = 5

class Trade:
    '''Represents a trade.'''
    count = 0
    Open = 0.0
    High = 0.0
    Low = 0.0
    Last = 0.0
    Price = 0.0
    Size = 0
    Volume = 0
    Exg = 0
    Symbol = ""
    Time = ""
    Date =  0

    def __init__(self):
        '''Initializes the Quote's data.'''
        #self.name = name
        Trade.count += 1

    def howMany(self):
        '''Prints the current population.'''
        if Trade.count == 1:
            print 'only one trade here.'
        else:
            print 'We have %d trades here.' % Trade.count

    def Open(self,da):
     	self.Open=da

    def High(self,da):
     	self.High=da

    def Low(self,da):
     	self.Low=da

    def Last(self,da):
     	self.Last=da

    def Price(self,da):
     	self.Price=da

    def Size(self,da):
     	self.Size=da

    def Volume(self,da):
     	self.Volume=da

    def Exg(self,da):
     	self.Exg=da

    def Symbol(self,da):
     	self.Symbol=da

    def Time(self,da):
     	self.Time=da

    def Date(self,da):
     	self.Date=da

class Quote:
    '''Represents a Quote.'''
    count = 0
    Bid = 0.0
    Bid_size = 0
    Bid_exg = 0
    Ask = 0.0
    Ask_size = 0
    Ask_exg = 0
    Best_bid = 0.0
    Best_bid_size = 0
    Best_bid_exg = 0
    Best_ask = 0.0
    Best_ask_size = 0
    Best_ask_exg = 0
    Symbol = ""
    Time = ""
    Date =  0

    def __init__(self):
        '''Initializes the Quote's data.'''
        #self.name = name
        Quote.count += 1

    def howMany(self):
        '''Prints the current population.'''
        if Quote.count == 1:
            print 'only one Quote here.'
        else:
            print 'We have %d Quotes here.' % Quote.count

    def Bid(self,da):
     	self.Bid=da

    def Bid_size(self,da):
     	self.Bid_size=da

    def Bid_exg(self,da):
     	self.Bid_exg=da

    def Ask(self,da):
     	self.Ask=da

    def Ask_size(self,da):
     	self.Ask_size=da

    def Ask_exg(self,da):
     	self.Ask_exg=da

    def Best_bid(self,da):
     	self.Best_bid=da

    def Best_bid_size(self,da):
     	self.Best_bid_size=da

    def Best_bid_exg(self,da):
     	self.Best_bid_exg=da

    def Best_ask(self,da):
     	self.Best_ask=da

    def Best_ask_size(self,da):
     	self.Best_ask_size=da

    def Best_ask_exg(self,da):
     	self.Best_ask_exg=da

    def Symbol(self,da):
     	self.Symbol=da

    def Time(self,da):
     	self.Time=da

    def Date(self,da):
     	self.Date=da

class Daily:
    '''Represents a Daily.'''
    count = 0
    Open = 0.0
    High = 0.0
    Low = 0.0
    Close = 0.0
    Volume = 0
    Symbol = ""
    Date =  0

    def __init__(self):
        '''Initializes the Daily's data.'''
        #self.name = name
        Daily.count += 1

    def howMany(self):
        '''Prints the current population.'''
        if Daily.count == 1:
            print 'only one Daily data here.'
        else:
            print 'We have %d dailys here.' % Daily.count

    def Open(self,da):
     	self.Open=da

    def High(self,da):
     	self.High=da

    def Low(self,da):
     	self.Low=da

    def Close(self,da):
     	self.Close=da

    def Volume(self,da):
     	self.Volume=da

    def Symbol(self,da):
     	self.Symbol=da

    def Date(self,da):
     	self.Date=da
  
class Imbalance:
    '''Represents a Imbalance.'''
    count = 0
    Imbalance_type = ImbType.UNKNOWN
    Side = Side.NONE
    Paired_shares = 0
    Market_imbalance = 0
    Total_imbalance = 0
    Ref_price = 0.0
    Far_price = 0.0
    Near_price = 0.0
    Clearing_price=0.0
    Tag = 0
    Symbol = ""
    Date =  0
    Time =""

    def __init__(self):
        '''Initializes the Daily's data.'''
        #self.name = name
        Imbalance.count += 1

    def howMany(self):
        '''Prints the current population.'''
        if Daily.count == 1:
            print 'only one Daily data here.'
        else:
            print 'We have %d dailys here.' % Imbalance.count

    def Imbalance_type(self,da):
        self.Imbalance_type=da

    def Side(self,da):
        self.Side=da

    def Paired_shares(self,da):
        self.Paired_shares=da

    def Market_imbalance(self,da):
        self.Market_imbalance=da

    def Total_imbalance(self,da):
        self.Total_imbalance=da

    def Ref_price(self,da):
        self.Ref_price=da

    def Near_price(self,da):
        self.Near_price=da

    def Paired_shares(self,da):
        self.Paired_shares=da

    def Far_price(self,da):
        self.Far_price=da

    def Clearing_price(self,da):
        self.Clearing_price=da

    def Tag(self,da):
        self.Tag=da

    def Time(self,da):
        self.Time=da

    def Symbol(self,da):
        self.Symbol=da

    def Date(self,da):
        self.Date=da

class Client:
    username = ""
    password = ""
    server = ""

    def __init__(self):
    	'''Initializes the Daily's data.'''
        #self.name = name

    def Client(self,user,password,server):
        self.username=user
        self.password=password
        self.server=server

class Exgprints:
    '''Represents a Exgprints.'''
    count = 0
    Price = 0.0
    Size = 0
    Listedexchange = 0
    Reportingexchange = 0
    Printtype = 0
    Symbol = ""
    Time = ""
    Date =  0

    def __init__(self):
        '''Initializes the Exgprints's data.'''
        #self.name = name
        Exgprints.count += 1

    def howMany(self):
        '''Prints the current population.'''
        if Exgprints.count == 1:
            print 'only one Exgprints here.'
        else:
            print 'We have %d Exgprints here.' % Exgprints.count

    def Price(self,da):
        self.Price=da

    def Size(self,da):
        self.Size=da

    def Reportingexchange(self,da):
        self.Reportingexchange=da

    def Printtype(self,da):
        self.Printtype=da

    def Listedexchange(self,da):
        self.Listedexchange=da

    def Symbol(self,da):
        self.Symbol=da

    def Time(self,da):
        self.Time=da

    def Date(self,da):
        self.Date=da

class News:
    '''Represents a News.'''
    count = 0
    Symbol = ""
    Time = ""
    Date =  0
    Seqno = 0
    Category = ""
    Source = ""
    Headline = ""
    Resourceid = ""
    Story = ""
    Tags = ""

    def __init__(self):
        '''Initializes the News's data.'''
        #self.name = name
        News.count += 1

    def howMany(self):
        '''Prints the current population.'''
        if News.count == 1:
            print 'only one News here.'
        else:
            print 'We have %d News here.' % News.count

    def Seqno(self,da):
        self.Seqno=da

    def Category(self,da):
        self.Category=da

    def Source(self,da):
        self.Source=da

    def Headline(self,da):
        self.Headline=da

    def Resourceid(self,da):
        self.Resourceid=da

    def Story(self,da):
         self.Story=da

    def Tags(self,da):
         self.Tags=da

    def Symbol(self,da):
        self.Symbol=da

    def Time(self,da):
        self.Time=da

    def Date(self,da):
        self.Date=da


def OnQuote(quote):
    print "OnQuote: " + quote.Symbol + "|" + str(quote.Date) + "|" + quote.Time + "|" + str(quote.Bid) + "|" + str(quote.Bid_size)  + "|" + str(quote.Ask) + "|" + str(quote.Ask_size)  + "|" + str(quote.Best_bid) + "|" + str(quote.Best_bid_size) + "|" + str(quote.Best_bid_exg)+ "|" + str(quote.Best_ask) + "|" + str(quote.Best_ask_size) + "|" + str(quote.Best_ask_exg)

def OnDaily(daily):
    print "OnDaily: " + daily.Symbol + "|" + str(daily.Date) + "|" + str(daily.Open) + "|" + str(daily.High) + "|" + str(daily.Low) + "|" + str(daily.Close) + "|" +  str(daily.Volume)

def OnTrade(trade):
	print "OnTrade: " + trade.Symbol + "|" + str(trade.Date) + "|" + trade.Time + "|" + str(trade.Price) + "|" + str(trade.Open) + "|" + str(trade.High) + "|" + str(trade.Low) + "|" + str(trade.Last) + "|" + str(trade.Size) + "|" + str(trade.Exg) + "|" + str(trade.Volume)
    
def OnImbalance(imb):
    print "OnImbalance: " + imb.Symbol + "|" + str(imb.Date) + "|" + imb.Time + "|" + str(imb.Clearing_price) + "|" + str(imb.Far_price) + "|" + str(imb.Imbalance_type) + "|" + str(imb.Market_imbalance) + "|" + str(imb.Near_price) + "|" + str(imb.Paired_shares) + "|" + str(imb.Ref_price) + "|" + str(imb.Side)+ "|" + str(imb.Tag)+ "|" + str(imb.Total_imbalance)
   
def OnExgprints(exgP):
    print "OnExgprints: " + exgP.Symbol + "|" + str(exgP.Date) + "|" + exgP.Time + "|" + str(exgP.Price) + "|" + str(exgP.Size) + "|" + str(exgP.Listedexchange) + "|" + str(exgP.Reportingexchange) + "|" + str(exgP.Printtype) 
  
def OnNews(news):
    print "OnNews: " + news.Symbol + "|" + str(news.Date) + "|" + news.Time + "|" + news.Category + "|" + news.Source + "|" + news.Headline + "|" + news.Resourceid + "|" + news.Tags + "|" + news.Story 
  

