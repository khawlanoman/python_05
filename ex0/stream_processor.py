from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional  # noqa


class DataProcessor(ABC):

    @abstractmethod
    def process(self, data: Any) -> str:
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass


class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if not isinstance(data, List):
            return False
        list_data = (isinstance(i, (int, float)) for i in data)
        return list_data

    def process(self, data: Any) -> str:
        try:
            len_data = len(data)
            sum_data = sum(data)
            avg_data = sum_data / len_data
            return (f"Output: Processed {len_data} numeric "
                    f"values, sum= {sum_data}, avg={avg_data:.1f}")
        except ValueError as e:
            return (f"ValueError:{e}")

    def format_output(self, result: str) -> str:
        return (result)


class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if not isinstance(data, str):
            return False
        return data

    def process(self, data: Any) -> str:
        try:
            len_data = len(data)
            len_word_data = len(data.split(" "))
            return (f"Output: Processed text: {len_data} "
                    f"characters, {len_word_data} words")
        except ValueError as e:
            return (f"ValueError:{e}")

    def format_output(self, result: str) -> str:
        return (result)


class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if not isinstance(data, str):
            return False
        return True

    def process(self, data: Any) -> str:
        key, value = data.split(":", 1)
        key = key.strip()
        value = value.strip()

        if key == "ERROR":
            return (f"[ALERT] {key} level detected: {value}")
        else:
            return (f"[{key}] {key} level detected: {value}")

    def format_output(self, result: str) -> str:
        return (result)


if __name__ == "__main__":
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===\n")
    print("Initializing Numeric Processor...")
    numeric_processor = NumericProcessor()
    text_processor = TextProcessor()
    log_processor = LogProcessor()

    data_numeric = [1, 2, 3, 4, 5]
    data_text = "Hello Nexus World"
    data_log = "ERROR: Connection timeout"
    if numeric_processor.validate(data_numeric):
        print(f"Processing data: {data_numeric}")
        valid = "Validation: Numeric data verified"
        print(numeric_processor.format_output(valid))
        print(numeric_processor.process(data_numeric))
    else:
        print("Error : invalide data numeric")

    if text_processor.validate(data_text):
        print("\nInitializing Text Processor...")
        print(f'Processing data: "{data_text}"')
        print(text_processor.format_output("Validation: Text data verified"))
        print(text_processor.process(data_text))
    else:
        print("Error : invalide data text")

    if log_processor.validate(data_log):
        print("\nInitializing Log Processor...")
        print(f"Processing data: {data_log}")
        print(log_processor.format_output("Validation: Log entry verified"))
        print(f"Output : {log_processor.process(data_log)}")
    else:
        print("Error : invalide data log ")

print("\n=== Polymorphic Processing Demo ===")
print("Processing multiple data types through same interface...")
data_numeric = [1, 2, 3]
data_text = "Hello World!"
data_log = "INFO: System ready"

list_poly = [(numeric_processor, data_numeric),
             (text_processor, data_text),
             (log_processor, data_log)]
count = 1
for key, value in list_poly:
    if key.validate(value):
        result = key.process(value)
        print(f"Result {count}: {result}")
    count += 1
print("\nFoundation systems online. Nexus ready for advanced streams.")
