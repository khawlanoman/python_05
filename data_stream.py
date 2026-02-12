from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional 

class DataStream(ABC):
    def __init__(self,stream_id):
        stream_id = stream_id 
    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        pass

    def filter_data(self, data_batch: List[Any], criteria: Optional[str] = None) -> List[Any]:
        pass

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
       pass

class SensorStream(DataStream):
    def __init__(self, stream_id,type:str)-> None:
        super().__init__(stream_id)
        self.type = type
        self.data = {}
    def process_batch(self, data_batch: List[Any]) -> str:
        try :
            self.data_batch = data_batch
            
            for item in data_batch:
                key, value = item.split(":")
                self.data[key] = value
        
            self.len_data = len(self.data)
            self.sum_data = sum(i for i in self.data.values())
            self.avg_data = self.sum_data / self.len_data

            return(f"Sensor analysis: {self.len_data} readings processed, avg temp: {self.avg_data}°C")
        except Exception:
            print("error")
    def filter_data(self, data_batch: List[Any], criteria: Optional[str] = None) -> List[Any]:
        if criteria:
            items =[item for item in data_batch if criteria in item]
            return items
        return data_batch
    
    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        len_data = len(self.data)
        return{f"Sensor data: {len_data} readings processed"}

class TransactionStream(DataStream):
    def process_batch(self, data_batch: List[Any]) -> str:
        pass
    
    def filter_data(self, data_batch: List[Any], criteria: Optional[str] = None) -> List[Any]:
        pass
    
    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        pass

class EventStream(DataStream):
    def process_batch(self, data_batch: List[Any]) -> str:
        pass
    
    def filter_data(self, data_batch: List[Any], criteria: Optional[str] = None) -> List[Any]:
        pass
    
    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        pass


if __name__ == "__main__":
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===\n")

    data_stream = DataStream()
    sensor_stream = data_stream.SensorStream()

    list_data = ["Stream ID: SENSOR_001", "Type: Environmental Data"]
    

    