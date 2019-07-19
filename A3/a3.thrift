service KeyValueService {
  string get(1: string key);
  void put(1: string key, 2: string value);
  map<string,string> copy();
  void backupPut(1: string key, 2: string value);
}
