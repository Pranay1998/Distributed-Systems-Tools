service KeyValueService {
  string get(1: string key);
  void put(1: string key, 2: string value);
  void addToMap(1: map<string, string> values);
  void copy(1: string key, 2: string value, 3: i32 sequence);
}
