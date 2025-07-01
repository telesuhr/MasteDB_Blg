"""
Bloomberg APIへの接続とデータ取得モジュール
"""
try:
    import blpapi
    MOCK_MODE = False
except ImportError:
    print("Warning: Real blpapi not available, using mock Bloomberg API for testing")
    import mock_blpapi as blpapi
    MOCK_MODE = True

import pandas as pd
from typing import Optional, Any, Union
from datetime import datetime, date
import time
import sys
import os

# プロジェクトルートとsrcディレクトリをPythonパスに追加
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
src_dir = os.path.join(project_root, 'src')
sys.path.insert(0, project_root)
sys.path.insert(0, src_dir)

from config.bloomberg_config import BLOOMBERG_HOST, BLOOMBERG_PORT
from config.logging_config import logger


class BloombergDataFetcher:
    """Bloomberg APIからデータを取得するクラス"""
    
    def __init__(self):
        self.session = None
        self.service = None
        
    def connect(self) -> bool:
        """Bloomberg APIに接続"""
        try:
            # セッションオプションの設定
            sessionOptions = blpapi.SessionOptions()
            sessionOptions.setServerHost(BLOOMBERG_HOST)
            sessionOptions.setServerPort(BLOOMBERG_PORT)
            
            # セッションの作成と開始
            self.session = blpapi.Session(sessionOptions)
            
            if not self.session.start():
                logger.error("Failed to start Bloomberg session")
                return False
                
            if not self.session.openService("//blp/refdata"):
                logger.error("Failed to open Bloomberg service")
                return False
                
            self.service = self.session.getService("//blp/refdata")
            logger.info("Successfully connected to Bloomberg API")
            return True
            
        except Exception as e:
            logger.error(f"Bloomberg connection error: {e}")
            return False
            
    def disconnect(self):
        """Bloomberg APIから切断"""
        if self.session:
            self.session.stop()
            logger.info("Disconnected from Bloomberg API")
            
    def get_historical_data(self, securities: list[str], fields: list[str],
                           start_date: str, end_date: str,
                           overrides: Optional[dict[str, Any]] = None) -> pd.DataFrame:
        """
        ヒストリカルデータを取得
        
        Args:
            securities: 証券リスト（Bloombergティッカー）
            fields: フィールドリスト
            start_date: 開始日（YYYYMMDD形式）
            end_date: 終了日（YYYYMMDD形式）
            overrides: オーバーライド設定
            
        Returns:
            pd.DataFrame: 取得したデータ
        """
        if not self.service:
            logger.error("Bloomberg service not initialized")
            return pd.DataFrame()
            
        try:
            # リクエストの作成
            request = self.service.createRequest("HistoricalDataRequest")
            
            # 証券の追加（最大100件まで）
            for security in securities[:100]:
                request.getElement("securities").appendValue(security)
                
            # フィールドの追加
            for field in fields:
                request.getElement("fields").appendValue(field)
                
            # 日付範囲の設定
            request.set("startDate", start_date)
            request.set("endDate", end_date)
            
            # オプション設定
            request.set("periodicitySelection", "DAILY")
            request.set("overrideOption", "OVERRIDE_OPTION_GPA")
            request.set("adjustmentFollowDPDF", True)
            
            # 全データソースからデータを取得（一時的にコメントアウト - 無効なオーバーライドのため）
            # overrides_element = request.getElement("overrides")
            # override_element = overrides_element.appendElement()
            # override_element.setElement("fieldId", "ALL_AVAILABLE_PRICING_SOURCE")
            # override_element.setElement("value", "Y")
            
            # カスタムオーバーライドの適用
            if overrides:
                overrides_element = request.getElement("overrides")
                for field_id, value in overrides.items():
                    override_element = overrides_element.appendElement()
                    override_element.setElement("fieldId", field_id)
                    override_element.setElement("value", value)
                    
            # リクエストの送信
            self.session.sendRequest(request)
            
            # レスポンスの処理
            data_list = []
            max_iterations = 1000  # 無限ループ防止
            iteration_count = 0
            
            while True:
                iteration_count += 1
                if iteration_count > max_iterations:
                    logger.warning(f"Max iterations ({max_iterations}) reached, breaking loop")
                    break
                
                try:
                    event = self.session.nextEvent(5000)  # タイムアウトを5秒に延長
                except Exception as e:
                    logger.error(f"Error getting next event: {e}")
                    break
                
                for msg in event:
                    if msg.hasElement("responseError"):
                        error = msg.getElement("responseError")
                        logger.error(f"Bloomberg response error: {error}")
                        continue
                        
                    # 新しいバージョンでは文字列で指定
                    if str(msg.messageType()) == 'HistoricalDataResponse':
                        self._process_historical_response(msg, data_list)
                        
                if event.eventType() == blpapi.Event.RESPONSE:
                    break
                    
            # DataFrameに変換
            if data_list:
                df = pd.DataFrame(data_list)
                logger.info(f"Retrieved {len(df)} historical data records")
                return df
            else:
                logger.warning("No historical data retrieved")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Error retrieving historical data: {e}")
            return pd.DataFrame()
            
    def get_reference_data(self, securities: list[str], fields: list[str],
                          overrides: Optional[dict[str, Any]] = None) -> pd.DataFrame:
        """
        リファレンスデータ（直近値）を取得
        
        Args:
            securities: 証券リスト
            fields: フィールドリスト
            overrides: オーバーライド設定
            
        Returns:
            pd.DataFrame: 取得したデータ
        """
        if not self.service:
            logger.error("Bloomberg service not initialized")
            return pd.DataFrame()
            
        try:
            # リクエストの作成
            request = self.service.createRequest("ReferenceDataRequest")
            
            # 証券の追加（最大100件まで）
            for security in securities[:100]:
                request.getElement("securities").appendValue(security)
                
            # フィールドの追加
            for field in fields:
                request.getElement("fields").appendValue(field)
                
            # オーバーライドの適用
            if overrides:
                overrides_element = request.getElement("overrides")
                for field_id, value in overrides.items():
                    override_element = overrides_element.appendElement()
                    override_element.setElement("fieldId", field_id)
                    override_element.setElement("value", value)
                    
            # リクエストの送信
            self.session.sendRequest(request)
            
            # レスポンスの処理
            data_list = []
            
            while True:
                event = self.session.nextEvent(500)
                
                for msg in event:
                    if msg.hasElement("responseError"):
                        error = msg.getElement("responseError")
                        logger.error(f"Bloomberg response error: {error}")
                        continue
                        
                    # 新しいバージョンでは文字列で指定
                    if str(msg.messageType()) == 'ReferenceDataResponse':
                        self._process_reference_response(msg, data_list)
                        
                if event.eventType() == blpapi.Event.RESPONSE:
                    break
                    
            # DataFrameに変換
            if data_list:
                df = pd.DataFrame(data_list)
                logger.info(f"Retrieved {len(df)} reference data records")
                return df
            else:
                logger.warning("No reference data retrieved")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Error retrieving reference data: {e}")
            return pd.DataFrame()
            
    def _process_historical_response(self, msg: blpapi.Message, data_list: list[dict]):
        """
        ヒストリカルデータレスポンスを処理
        
        Args:
            msg: Bloombergメッセージ
            data_list: データを格納するリスト
        """
        security_data = msg.getElement("securityData")
        security = security_data.getElementAsString("security")
        
        if security_data.hasElement("securityError"):
            error = security_data.getElement("securityError")
            logger.error(f"Security error for {security}: {error}")
            return
            
        field_data_array = security_data.getElement("fieldData")
        
        for i in range(field_data_array.numValues()):
            field_data = field_data_array.getValueAsElement(i)
            
            data_point = {"security": security}
            
            # 日付の取得
            if field_data.hasElement("date"):
                date_element = field_data.getElement("date")
                if date_element.datatype() == blpapi.DataType.DATE:
                    date_value = date_element.getValueAsDatetime()
                    if hasattr(date_value, 'date'):
                        data_point["date"] = date_value.date()
                    else:
                        data_point["date"] = date_value
                else:
                    date_value = field_data.getElementAsDatetime("date")
                    if hasattr(date_value, 'date'):
                        data_point["date"] = date_value.date()
                    else:
                        data_point["date"] = date_value
                
            # フィールド値の取得
            for j in range(field_data.numElements()):
                element = field_data.getElement(j)
                field_name = str(element.name())
                
                if field_name != "date":
                    if element.isNull():
                        data_point[field_name] = None
                    else:
                        # データ型に応じて値を取得
                        if element.datatype() == blpapi.DataType.FLOAT64:
                            data_point[field_name] = element.getValueAsFloat()
                        elif element.datatype() == blpapi.DataType.INT32:
                            data_point[field_name] = element.getValueAsInteger()
                        elif element.datatype() == blpapi.DataType.INT64:
                            data_point[field_name] = element.getValueAsInt64()
                        elif element.datatype() == blpapi.DataType.DATE:
                            date_value = element.getValueAsDatetime()
                            if hasattr(date_value, 'date'):
                                data_point[field_name] = date_value.date()
                            else:
                                data_point[field_name] = date_value
                        else:
                            data_point[field_name] = element.getValueAsString()
                            
            data_list.append(data_point)
            
    def _process_reference_response(self, msg: blpapi.Message, data_list: list[dict]):
        """
        リファレンスデータレスポンスを処理
        
        Args:
            msg: Bloombergメッセージ
            data_list: データを格納するリスト
        """
        securities_data = msg.getElement("securityData")
        
        for i in range(securities_data.numValues()):
            security_data = securities_data.getValueAsElement(i)
            security = security_data.getElementAsString("security")
            
            if security_data.hasElement("securityError"):
                error = security_data.getElement("securityError")
                logger.error(f"Security error for {security}: {error}")
                continue
                
            field_data = security_data.getElement("fieldData")
            data_point = {"security": security, "date": datetime.now().date()}
            
            for j in range(field_data.numElements()):
                element = field_data.getElement(j)
                field_name = str(element.name())
                
                if element.isNull():
                    data_point[field_name] = None
                else:
                    # データ型に応じて値を取得
                    if element.datatype() == blpapi.DataType.FLOAT64:
                        data_point[field_name] = element.getValueAsFloat()
                    elif element.datatype() == blpapi.DataType.INT32:
                        data_point[field_name] = element.getValueAsInteger()
                    elif element.datatype() == blpapi.DataType.INT64:
                        data_point[field_name] = element.getValueAsInt64()
                    elif element.datatype() == blpapi.DataType.DATE:
                        date_value = element.getValueAsDatetime()
                        if hasattr(date_value, 'date'):
                            data_point[field_name] = date_value.date()
                        else:
                            data_point[field_name] = date_value
                    else:
                        data_point[field_name] = element.getValueAsString()
                        
            data_list.append(data_point)
            
    def batch_request(self, securities: list[str], fields: list[str],
                     start_date: str, end_date: str,
                     batch_size: int = 100,
                     request_type: str = "historical") -> pd.DataFrame:
        """
        大量の証券に対してバッチ処理でデータを取得
        
        Args:
            securities: 証券リスト
            fields: フィールドリスト
            start_date: 開始日
            end_date: 終了日
            batch_size: バッチサイズ（最大100）
            request_type: "historical" または "reference"
            
        Returns:
            pd.DataFrame: 取得したデータ
        """
        all_data = []
        
        for i in range(0, len(securities), batch_size):
            batch_securities = securities[i:i + batch_size]
            logger.info(f"Processing batch {i//batch_size + 1}/{(len(securities)-1)//batch_size + 1}")
            
            if request_type == "historical":
                batch_data = self.get_historical_data(batch_securities, fields,
                                                     start_date, end_date)
            else:
                batch_data = self.get_reference_data(batch_securities, fields)
                
            if not batch_data.empty:
                all_data.append(batch_data)
                
            # API制限を考慮して少し待機
            if i + batch_size < len(securities):
                time.sleep(0.5)
                
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            logger.info(f"Total records retrieved: {len(combined_df)}")
            return combined_df
        else:
            return pd.DataFrame()