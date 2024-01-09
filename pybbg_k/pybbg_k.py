# -*- coding: utf-8 -*-
"""
Edited on 18/01/2017
"""

from __future__ import print_function
import blpapi
from collections import defaultdict
from pandas import DataFrame
from datetime import datetime, date, time
import pandas as pd
import numpy as np
import sys
from pprint import pprint
import warnings
import six
from dateutil.relativedelta import relativedelta
from time import sleep


class Pybbg():
    def __init__(self, host='localhost', port=8194):
        """
        Starting bloomberg API session
        close with session.close()
        """
        # Fill SessionOptions
        sessionOptions = blpapi.SessionOptions()
        sessionOptions.setServerHost(host)
        sessionOptions.setServerPort(port)

        self.initialized_services = set()

        # Create a Session
        self.session = blpapi.Session(sessionOptions)

        # Start a Session
        if not self.session.start():
            print("Failed to start session.")

        self.session.nextEvent()
    
    def close(self):
        self.session.stop()    
        
    def service_refData(self):
        """
        init service for refData
        """
        if '//blp/refdata' in self.initialized_services:
            return

        if not self.session.openService("//blp/refdata"):
            print("Failed to open //blp/refdata")

        
#        self.session.nextEvent()

        # Obtain previously opened service
        self.refDataService = self.session.getService("//blp/refdata")
        
#        self.session.nextEvent()

        self.initialized_services.add('//blp/refdata')

    def bdh(self, ticker_list, fld_list, start_date, end_date=date.today().strftime('%Y%m%d'), periodselection='DAILY', overrides=None, other_request_parameters=None, move_dates_to_period_end=False):
        """
        Get ticker_list and field_list
        return pandas multi level columns dataframe
        """
        # Create and fill the request for the historical data
        self.service_refData()

        if isstring(ticker_list):
            ticker_list = [ticker_list]
        if isstring(fld_list):
            fld_list = [fld_list]

        if hasattr(start_date, 'strftime'):
            start_date = start_date.strftime('%Y%m%d')
        if hasattr(end_date, 'strftime'):
            end_date = end_date.strftime('%Y%m%d')

        request = self.refDataService.createRequest("HistoricalDataRequest")
        for t in ticker_list:
            request.getElement("securities").appendValue(t)
        for f in fld_list:
            request.getElement("fields").appendValue(f)
        request.set("periodicitySelection", periodselection)
        request.set("startDate", start_date)
        request.set("endDate", end_date)


        if overrides is not None:
            overrideOuter = request.getElement('overrides')
            for k in overrides:
                override1 = overrideOuter.appendElement()
                override1.setElement('fieldId', k)
                override1.setElement('value', overrides[k])

        if other_request_parameters is not None:
            for k,v in six.iteritems(other_request_parameters):
                request.set(k, v)

        def adjust_date(to_adjust):
            if periodselection == 'MONTHLY':
                # always make the date the last day of the month
                return date(to_adjust.year, to_adjust.month, 1) + relativedelta(months=1) - relativedelta(days=1)
            if periodselection == 'WEEKLY':
                return to_adjust + relativedelta(weekday=4)

            return to_adjust

        # print("Sending Request:", request)
        # Send the request
        self.session.sendRequest(request)
        # defaultdict - later convert to pandas
        data = defaultdict(dict)
        warnings.warn(str(data))
        # Process received events
        while (True):
            # We provide timeout to give the chance for Ctrl+C handling:
            ev = self.session.nextEvent()
            if ev.eventType() == blpapi.Event.RESPONSE or ev.eventType() == blpapi.Event.PARTIAL_RESPONSE:

                for msg in ev:
                    ticker = msg.getElement('securityData').getElement('security').getValue()
                    fieldData = msg.getElement('securityData').getElement('fieldData')
                    for i in range(fieldData.numValues()):
                        for j in range(1, fieldData.getValue(i).numElements()):
                            dt = fieldData.getValue(i).getElement(0).getValue()
                            if move_dates_to_period_end:
                                dt = adjust_date(dt)
                            # fld_list
                            fld = str(fieldData.getValue(i).getElement(j).name())
                            data[(ticker, fld)][dt] = fieldData.getValue(i).getElement(j).getValue()

            if ev.eventType() == blpapi.Event.RESPONSE:
                # Response completly received, so we could exit
                break
            sleep(1)
            print("Waiting for bdh")


        if len(fld_list) == 1:
            data = {k[0]: v for k, v in data.items()}
            data = DataFrame(data)
            data = data[ticker_list]
            data.index = pd.to_datetime(data.index)
            return data

        if len(data) == 0:
            # security error case
            return DataFrame()

        data = DataFrame(data)
        data = data[ticker_list]
        data.columns = pd.MultiIndex.from_tuples(data, names=['ticker', 'field'])
        data.index = pd.to_datetime(data.index)
        return data

    def bdib(self, ticker, fld_list, startDateTime, endDateTime, eventType='TRADE', interval=1):
        """
        Get one ticker (Only one ticker available per call); eventType (TRADE, BID, ASK,..etc); interval (in minutes)
                ; fld_list (Only [open, high, low, close, volumne, numEvents] availalbe)
        return pandas dataframe with return Data
        """
        self.service_refData()
        # Create and fill the request for the historical data
        request = self.refDataService.createRequest("IntradayBarRequest")
        request.set("security", ticker)
        request.set("eventType", eventType)
        request.set("interval", interval)  # bar interval in minutes
        request.set("startDateTime", startDateTime)
        request.set("endDateTime", endDateTime)

        # print "Sending Request:", request
        # Send the request
        self.session.sendRequest(request)
        # defaultdict - later convert to pandas
        data = defaultdict(dict)
        # Process received events
        while (True):
            # We provide timeout to give the chance for Ctrl+C handling:
            ev = self.session.nextEvent(500)
            if ev.eventType() == blpapi.Event.RESPONSE or ev.eventType() == blpapi.Event.PARTIAL_RESPONSE:
                for msg in ev:
                    barTickData = msg.getElement('barData').getElement('barTickData')
                    for i in range(barTickData.numValues()):
                        for j in range(len(fld_list)):
                            data[(fld_list[j])][barTickData.getValue(i).getElement(0).getValue()] = barTickData.getValue(
                                i).getElement(fld_list[j]).getValue()

            if ev.eventType() == blpapi.Event.RESPONSE:
                # Response completly received, so we could exit
                break
        data = DataFrame(data)
        data.index = pd.to_datetime(data.index)
        return data

    def bdp(self, ticker, fld_list, overrides=None):
#        print(ticker, fld_list, overrides)
#        print("bdp: self.service_refData()")
        self.service_refData()
#        print("bdp: self.service_refData() exit")

        request = self.refDataService.createRequest("ReferenceDataRequest")
        if isstring(ticker):
            ticker = [ticker]

        securities = request.getElement("securities")
        for t in ticker:
            securities.appendValue(t)

        if isstring(fld_list):
            fld_list = [fld_list]

        fields = request.getElement("fields")
        for f in fld_list:
            fields.appendValue(f)

        if overrides is not None:
            overrideOuter = request.getElement('overrides')
            for k in overrides:
                override1 = overrideOuter.appendElement()
                override1.setElement('fieldId', k)
                override1.setElement('value', overrides[k])

        self.session.sendRequest(request)
        data = dict()

        while (True):
            # We provide timeout to give the chance for Ctrl+C handling:
            ev = self.session.nextEvent()
            
#            print("bdp: ev.eventType() = ", ev.eventType() )
            
            if ev.eventType() == blpapi.Event.RESPONSE or ev.eventType() == blpapi.Event.PARTIAL_RESPONSE:
                
                for msg in ev:
#                    print(msg)
                    securityData = msg.getElement("securityData")
    
                    for i in range(securityData.numValues()):
                        fieldData = securityData.getValue(i).getElement("fieldData")
                        secId = securityData.getValue(i).getElement("security").getValue()
                        data[secId] = dict()
                        for field in fld_list:
                            if fieldData.hasElement(field):
                                data[secId][field] = fieldData.getElement(field).getValue()
                            else:
                                data[secId][field] = np.NaN

            if ev.eventType() == blpapi.Event.RESPONSE:
                # Response completly received, so we could exit
                break
            sleep(1)
            print("Waiting for bdp")

        return pd.DataFrame.from_dict(data)

    # def bdp(self, ticker, fld_list):

    #     self.service_refData()

    #     request = self.refDataService.createRequest("ReferenceDataRequest")
    #     if isstring(ticker):
    #         ticker = [ ticker ]

    #     securities = request.getElement("securities")
    #     for t in ticker:
    #         securities.appendValue(t)

    #     if isstring(fld_list):
    #         fld_list = [ fld_list ]

    #     fields = request.getElement("fields")
    #     for f in fld_list:
    #         fields.appendValue(f)


    #     self.session.sendRequest(request)
    #     data = dict()

    #     while(True):
    #         # We provide timeout to give the chance for Ctrl+C handling:
    #         ev = self.session.nextEvent(500)
    #         for msg in ev:
    #             securityData = msg.getElement("securityData")

    #             for i in range(securityData.numValues()):
    #                 fieldData = securityData.getValue(i).getElement("fieldData")
    #                 secId = securityData.getValue(i).getElement("security").getValue()
    #                 data[secId] = dict()
    #                 for field in fld_list:
    #                     if fieldData.hasElement(field):
    #                         data[secId][field] = fieldData.getElement(field).getValue()
    #                     else:
    #                         data[secId][field] = np.NaN



    #         if ev.eventType() == blpapi.Event.RESPONSE:
    #             # Response completly received, so we could exit
    #             break

    #     return pd.DataFrame.from_dict(data)


    def bds(self, security, field, overrides=None):

        self.service_refData()

        request = self.refDataService.createRequest("ReferenceDataRequest")
        assert isstring(security)
        assert isstring(field)

        securities = request.getElement("securities")
        securities.appendValue(security)

        fields = request.getElement("fields")
        fields.appendValue(field)

        if overrides is not None:
            overrideOuter = request.getElement('overrides')
            for k in overrides:
                override1 = overrideOuter.appendElement()
                override1.setElement('fieldId', k)
                override1.setElement('value', overrides[k])

        # print(request)
        self.session.sendRequest(request)
        data = dict()

        while (True):
            # We provide timeout to give the chance for Ctrl+C handling:
            ev = self.session.nextEvent()
            if ev.eventType() == blpapi.Event.RESPONSE or ev.eventType() == blpapi.Event.PARTIAL_RESPONSE:
                for msg in ev:
                    # processMessage(msg)
                    securityData = msg.getElement("securityData")
                    for i in range(securityData.numValues()):
                        fieldData = securityData.getValue(i).getElement("fieldData").getElement(field)
                        for i, row in enumerate(fieldData.values()):
                            for j in range(row.numElements()):
                                e = row.getElement(j)
                                k = str(e.name())
                                v = e.getValue()
                                if k not in data:
                                    data[k] = list()
    
                                data[k].append(v)

            if ev.eventType() == blpapi.Event.RESPONSE:
                # Response completly received, so we could exit
                break
            sleep(1)
            print("Waiting for bds")

        return pd.DataFrame.from_dict(data)
    
    
    def service_exrsvc(self):
        """
        init service for exrsvc
        """
#        print("service_exrsvc")

        if '//blp/exrsvc' in self.initialized_services:
            return

        if not self.session.openService("//blp/exrsvc"):
            print("Failed to open //blp/exrsvc")

#        self.session.nextEvent()

        # Obtain previously opened service
        self.exrsvcService = self.session.getService("//blp/exrsvc")

#        self.session.nextEvent()
        
#        print("""self.initialized_services.add('//blp/exrsvc')""")
        self.initialized_services.add('//blp/exrsvc')
#        print("exiting service_exrsvc")
        
    def bsrch(self, ticker_busqueda='G1C0'):#G1O2

        print("self.service_exrsvc()")
        self.service_exrsvc()
#        print("Llego acá -3")

        request = self.exrsvcService.createRequest("ExcelGetGridRequest")
        
        assert isstring(ticker_busqueda)

#        session.openService("//blp/exrsvc")
        
#        exrService = session.getService("//blp/exrsvc")
        
                
#        request = exrService.createRequest("ExcelGetGridRequest")          
        request.set("Domain", "FI:"+ticker_busqueda)
        # dict_overrides = {"BIKEY" : "RBBK1QFRHFKT"}
        # if dict_overrides is not None:
        #     ors=request.getElement('Overrides')
        
        #     for nm, value in dict_overrides.items():
        #         ovrd=ors.appendElement()
        #         ovrd.setElement('name',nm)
        #         ovrd.setElement('value',value)
            
            
        response = self.session.sendRequest(request)

        bonds = []
#        print("Llego acá -2")
#        continuar_loop = True
        while True:
            
#            print("En loop")
            ev = self.session.nextEvent()
#            print("Llego acá 0")    
#            print(ev.eventType(),blpapi.Event.RESPONSE)
#            print("ev.eventType() == blpapi.Event.RESPONSE", ev.eventType() == blpapi.Event.RESPONSE)    
            if ev.eventType() == blpapi.Event.RESPONSE or ev.eventType() == blpapi.Event.PARTIAL_RESPONSE:
#                print("Llego acá 1")
                for msg in ev:
#                    print(msg)
#                    print("Llego acá 2")
                    for i in range(msg.getElement("DataRecords").numValues()):
#                        i = 0
#                        print("Llego acá 3 i=" +str(i) )
                        bonds.append(msg.getElement("DataRecords").getValue(i).getElement("DataFields").getValue(0).getElement("StringValue").getValue())
#            print("Llego acá 4")    
            if ev.eventType() == blpapi.Event.RESPONSE: 
#                print("Llego acá 5")
                break
            sleep(1)
            print("Waiting for bsrch")
#        print("Saliendo loop")
#        print(bonds)
        return bonds
    
    # domain = "TPD:DEX"
    # dict_overrides = {"BIKEY" : "RBBK1QFRHFKT"}
    def bsrch_general(self, domain, dict_overrides=None):#G1O2

        print("self.service_exrsvc()")
        self.service_exrsvc()
#        print("Llego acá -3")

        request = self.exrsvcService.createRequest("ExcelGetGridRequest")
        
        request.set("Domain", domain)
        
        if dict_overrides is not None:
            ors=request.getElement('Overrides')
        
            for nm, value in dict_overrides.items():
                ovrd=ors.appendElement()
                ovrd.setElement('name',nm)
                ovrd.setElement('value',value)
            
            
        response = self.session.sendRequest(request)

        results = []
#        print("Llego acá -2")
#        continuar_loop = True
        fields = None
        while True:
            
#            print("En loop")
            ev = self.session.nextEvent()
#            print("Llego acá 0")    
#            print(ev.eventType(),blpapi.Event.RESPONSE)
#            print("ev.eventType() == blpapi.Event.RESPONSE", ev.eventType() == blpapi.Event.RESPONSE)    
            if ev.eventType() == blpapi.Event.RESPONSE or ev.eventType() == blpapi.Event.PARTIAL_RESPONSE:
#                print("Llego acá 1")
                for msg in ev:
#                    print(msg)
#                    print("Llego acá 2")
                    if fields is None:
                        fields = list(msg.getElement("ColumnTitles"))
                    for i in range(msg.getElement("DataRecords").numValues()):
#                        i = 0
#                        print("Llego acá 3 i=" +str(i) )
                        values_i = msg.getElement("DataRecords").getValue(i).getElement("DataFields")
                        item_dict = zip(fields, [list(item.values())[0] for item in values_i.toPy()])
                        
                        results.append({field : value for field, value in item_dict})
                        
#            print("Llego acá 4")    
            if ev.eventType() == blpapi.Event.RESPONSE: 
#                print("Llego acá 5")
                break
            sleep(1)
            print("Waiting for bsrch")
#        print("Saliendo loop")
#        print(bonds)
        df_resultado = pd.DataFrame(results)
        return df_resultado
    

    def stop(self):
        self.session.stop()


def isstring(s):
    # if we use Python 3
    if (sys.version_info[0] == 3):
        return isinstance(s, str)
    # we use Python 2
    return isinstance(s, basestring)


def processMessage(msg):
    SECURITY_DATA = blpapi.Name("securityData")
    SECURITY = blpapi.Name("security")
    FIELD_DATA = blpapi.Name("fieldData")
    FIELD_EXCEPTIONS = blpapi.Name("fieldExceptions")
    FIELD_ID = blpapi.Name("fieldId")
    ERROR_INFO = blpapi.Name("errorInfo")

    securityDataArray = msg.getElement(SECURITY_DATA)
    for securityData in securityDataArray.values():
        print(securityData.getElementAsString(SECURITY))
        fieldData = securityData.getElement(FIELD_DATA)
        for field in fieldData.elements():
            for i, row in enumerate(field.values()):
                for j in range(row.numElements()):
                    e = row.getElement(j)
                    print("Row %d col %d: %s %s" % (i, j, e.name(), e.getValue()))