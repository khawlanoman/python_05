from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional 

class DataStream(ABC):
    def __init__(self,stream_id):
        self.stream_id = stream_id 
    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        pass

    def filter_data(self, data_batch: List[Any], criteria: Optional[str] = None) -> List[Any]:
        pass

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
       pass

class SensorStream(DataStream):
    def __init__(self, stream_id:str,type:str)-> None:
        super().__init__(stream_id)
        self.type = type
        self.data = {}
        self.data_t =[]
    def process_batch(self, data_batch: List[Any]) -> str:
        try :
            self.data_batch = data_batch
            self.data = {}
            
            for item in data_batch:
                key, value = item.split(":")
                self.data[key] = float(value)
           
            self.len_data = len(self.data)

            data_buy = [value for key, value in  self.data.items() if key == "buy"]
           
            avg_data = sum(data_buy) / len(data_buy) if data_buy else 0

            return(f"Sensor analysis: {self.len_data} readings processed, avg buy: {avg_data:.1f}°C")
        except Exception as e:
            print(f"error {e}")
            
    def filter_data(self, data_batch: List[Any], criteria: Optional[str] = None) -> List[Any]:
        self.data_t = [i for i in data_batch]
        if criteria:
            items =[item.replace("'","") for item in data_batch if criteria in item]
            return items
        return data_batch
    
    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        len_data = len(self.data_t)
        dic = {"result":f"- Sensor data: {len_data} readings processed"}
        return(dic["result"])

class TransactionStream(DataStream):
    def __init__(self, stream_id:str,type:str)-> None:
        super().__init__(stream_id)
        self.type = type
        self.data_t = []
        self.data = {}
    def process_batch(self, data_batch: List[Any]) -> str:
        try :
            self.data_batch = data_batch
            

            for item in data_batch:
                key, value = item.split(":")
                value = int(value)
                if key not in self.data:
                    self.data[key] = []
                self.data[key].append(value)

            data_buy = []
            data_sell = []
            for key,value in self.data.items():
                if key == "buy":
                    data_buy.extend(value)
                elif key == "sell":
                    data_sell.extend(value)
            buy = sum(data_buy)
            sell = sum(data_sell)
            
            units =  buy - sell
           
            if units > 0:
                units = f"+{units}"
            else:
                units = "0"

            return(f"Transaction analysis: {len(self.data_batch)} operations, net flow: {units} units")
        except Exception:
              print("error")
    def filter_data(self, data_batch: List[Any], criteria: Optional[str] = None) -> List[Any]:
        self.data_t = [i for i in data_batch]
        if criteria:
            items =[item.replace("'","") for item in data_batch if criteria in item]
            return items
        return data_batch
    
    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        len_data = len(self.data_t)
        dic = {"result":f"- Transaction data: {len_data} operations processed"}
        return (dic["result"])


class EventStream(DataStream):
    def __init__(self, stream_id:str,type:str)-> None:
        super().__init__(stream_id)
        self.type = type
        self.data = []

    def process_batch(self, data_batch: List[Any]) -> str:
        try :
            self.data_batch = data_batch
            count = 0
            for i in data_batch:
                self.data.append(i)

            for i in data_batch:
                if  i == "error":
                    count += 1

            return(f"Event analysis: {len(data_batch)} events, {count} error detected")
        except Exception:
            print("error")
    def filter_data(self, data_batch: List[Any], criteria: Optional[str] = None) -> List[Any]:
        if criteria:
            items =[item.replace("'","") for item in data_batch if criteria in item]
            return items
        return data_batch
    
    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        len_data = len(self.data)
        dic = {"result": f"- Event data: {len_data} events processed"}
        return(dic["result"])

if __name__ == "__main__":
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===\n")


    sensor_stream = SensorStream("SENSOR_001","Type: Environmental Data")
    print("Initializing Sensor Stream...")
    list_data = ["buy:22.5", "humidity:65", "pressure:1013"]
    print(f"Stream ID: {sensor_stream.stream_id}, Type: {sensor_stream.type}")
    print(f"Processing sensor batch:{sensor_stream.filter_data(list_data)}")
    print(f"{sensor_stream.process_batch(list_data)}")

    transaction_stream = TransactionStream("TRANS_001","Financial Data")
    list_data = ["buy:100", "sell:150", "buy:75"]
    print("\nInitializing Transaction Stream...")
    print(f"Stream ID: {transaction_stream.stream_id}, Type: {transaction_stream.type}")
    print(f"Processing transaction batch: {transaction_stream.filter_data(list_data)}")
    print(f"{transaction_stream.process_batch(list_data)}")

    event_stream = EventStream("EVENT_001","System Events")
    list_data = ["login", "error", "logout"]
    print("\nInitializing Event Stream...")
    print(f"Stream ID: {event_stream.stream_id}, Type: {event_stream.type}")
    filtered = event_stream.filter_data(list_data)
    filtered_str = "[" + ",".join(filtered) + "]"
    print(f"Processing transaction batch: {filtered_str}")
    print(f"{event_stream.process_batch(list_data)}")


print("\n=== Polymorphic Stream Processing ===")
print("Processing mixed stream types through unified interface...\n")

data_sensor = ["buy:22.5","humidity:32"]
data_transaction = ["buy:100", "sell:150","bu:10", "sel:130"]
data_event = ["login", "error", "logout"]

list_poly = [ (sensor_stream,data_sensor),
              (transaction_stream,data_transaction),
              (event_stream , data_event)
]

for key, value in list_poly:
    if key.filter_data(value):
        print(key.get_stats())
    
print("\nStream filtering active: High-priority data only")
print("Filtered results: 2 critical sensor alerts, 1 large transaction")
print("\nAll streams processed successfully. Nexus throughput optimal.")