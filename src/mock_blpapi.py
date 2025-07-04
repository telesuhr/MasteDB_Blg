"""
Mock Bloomberg API for testing when real blpapi is not available
"""
import pandas as pd
from datetime import datetime, date, timedelta
from typing import Any, Optional
import random


class DataType:
    FLOAT64 = "FLOAT64"
    INT32 = "INT32"
    INT64 = "INT64"
    DATE = "DATE"
    STRING = "STRING"


class Event:
    RESPONSE = "RESPONSE"


class Element:
    def __init__(self, name: str, value: Any, datatype: str = DataType.STRING):
        self._name = name
        self._value = value
        self._datatype = datatype
        
    def name(self):
        return self._name
        
    def datatype(self):
        return self._datatype
        
    def isNull(self):
        return self._value is None
        
    def getValueAsFloat(self):
        return float(self._value)
        
    def getValueAsInteger(self):
        return int(self._value)
        
    def getValueAsInt64(self):
        return int(self._value)
        
    def getValueAsString(self):
        return str(self._value)
        
    def getValueAsDatetime(self):
        if isinstance(self._value, (date, datetime)):
            return self._value
        return datetime.strptime(str(self._value), '%Y%m%d')


class MockFieldData:
    def __init__(self, date_val: date, data: dict):
        self.date_val = date_val
        self.data = data
        
    def hasElement(self, name: str):
        return name == "date" or name in self.data
        
    def getElement(self, name: str):
        if name == "date":
            return Element("date", self.date_val, DataType.DATE)
        return Element(name, self.data.get(name), DataType.FLOAT64)
        
    def getElementAsDatetime(self, name: str):
        return self.date_val
        
    def numElements(self):
        return len(self.data) + 1  # +1 for date
        
    def getElement(self, index):
        if index == 0:
            return Element("date", self.date_val, DataType.DATE)
        
        keys = list(self.data.keys())
        if index - 1 < len(keys):
            key = keys[index - 1]
            return Element(key, self.data[key], DataType.FLOAT64)
        
        return Element("unknown", None, DataType.STRING)


class MockSecurityData:
    def __init__(self, security: str, field_data_list: list):
        self.security = security
        self.field_data_list = field_data_list
        
    def getElementAsString(self, name: str):
        if name == "security":
            return self.security
        return None
        
    def hasElement(self, name: str):
        return name in ["security", "fieldData"]
        
    def getElement(self, name: str):
        if name == "fieldData":
            return MockFieldDataArray(self.field_data_list)
        return None


class MockFieldDataArray:
    def __init__(self, field_data_list: list):
        self.field_data_list = field_data_list
        
    def numValues(self):
        return len(self.field_data_list)
        
    def getValueAsElement(self, index):
        return self.field_data_list[index]


class MockMessage:
    def __init__(self, message_type: str, security_data: MockSecurityData):
        self._message_type = message_type
        self.security_data = security_data
        
    def messageType(self):
        return self._message_type
        
    def hasElement(self, name: str):
        return name == "securityData"
        
    def getElement(self, name: str):
        if name == "securityData":
            return self.security_data
        return None


class MockEvent:
    def __init__(self, event_type: str, messages: list):
        self._event_type = event_type
        self.messages = messages
        
    def eventType(self):
        return self._event_type
        
    def __iter__(self):
        return iter(self.messages)


class MockRequest:
    def __init__(self):
        self.elements = {}
        self.arrays = {}
        
    def getElement(self, name: str):
        if name not in self.arrays:
            self.arrays[name] = MockElementArray()
        return self.arrays[name]
        
    def set(self, name: str, value: Any):
        self.elements[name] = value


class MockElementArray:
    def __init__(self):
        self.values = []
        
    def appendValue(self, value: Any):
        self.values.append(value)


class MockService:
    def createRequest(self, request_type: str):
        return MockRequest()


class MockSession:
    def __init__(self):
        self.service = MockService()
        
    def start(self):
        return True
        
    def openService(self, service_name: str):
        return True
        
    def getService(self, service_name: str):
        return self.service
        
    def sendRequest(self, request: MockRequest):
        pass
        
    def nextEvent(self, timeout: int):
        # Generate mock data
        securities = request.arrays.get("securities", MockElementArray()).values
        fields = request.arrays.get("fields", MockElementArray()).values
        
        if not securities:
            securities = ["LMCADY03 Comdty"]
        if not fields:
            fields = ["PX_LAST"]
            
        # Generate mock historical data
        messages = []
        for security in securities:
            field_data_list = []
            
            # Generate 5 days of mock data
            for i in range(5):
                date_val = date.today() - timedelta(days=i)
                data = {}
                
                for field in fields:
                    if field == "PX_LAST":
                        # All LME copper prices should be in USD/MT for consistency
                        if 'LP' in security and 'Comdty' in security:
                            # LP1-LP12 LME Generic futures in USD/MT (corrected from previous pound-based pricing)
                            # Generate slight variations from cash price to simulate contango/backwardation
                            base_price = random.uniform(8000, 9000)
                            # Add slight forward curve variations for different months
                            if 'LP1' in security:
                                data[field] = round(base_price + random.uniform(-50, 50), 2)
                            elif 'LP2' in security:
                                data[field] = round(base_price + random.uniform(-30, 70), 2)
                            elif 'LP3' in security:
                                data[field] = round(base_price + random.uniform(-10, 90), 2)
                            else:
                                # LP4-LP12
                                month_num = int(security.replace('LP', '').replace(' Comdty', ''))
                                forward_premium = month_num * random.uniform(5, 15)
                                data[field] = round(base_price + forward_premium, 2)
                        elif 'Index' in security:
                            # Index prices (like LMCADY Index) in USD/MT
                            data[field] = round(random.uniform(8000, 9000), 2)
                        else:
                            # Default copper futures prices in USD/MT
                            data[field] = round(random.uniform(8000, 9000), 2)
                    else:
                        data[field] = round(random.uniform(100, 1000), 2)
                        
                field_data_list.append(MockFieldData(date_val, data))
                
            security_data = MockSecurityData(security, field_data_list)
            message = MockMessage("HistoricalDataResponse", security_data)
            messages.append(message)
            
        return MockEvent(Event.RESPONSE, messages)
        
    def stop(self):
        pass


class SessionOptions:
    def setServerHost(self, host: str):
        pass
        
    def setServerPort(self, port: int):
        pass


class Session:
    def __init__(self, options: SessionOptions):
        self.mock_session = MockSession()
        
    def start(self):
        return self.mock_session.start()
        
    def openService(self, service_name: str):
        return self.mock_session.openService(service_name)
        
    def getService(self, service_name: str):
        return self.mock_session.getService(service_name)
        
    def sendRequest(self, request):
        return self.mock_session.sendRequest(request)
        
    def nextEvent(self, timeout: int):
        return self.mock_session.nextEvent(timeout)
        
    def stop(self):
        return self.mock_session.stop()


# Mock constants
class Names:
    HISTORICAL_DATA_RESPONSE = "HistoricalDataResponse"
    REFERENCE_DATA_RESPONSE = "ReferenceDataResponse"


# Type aliases for compatibility
Message = MockMessage