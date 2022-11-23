import { DataQuery, DataSourceJsonData } from '@grafana/data';


export interface GJSONPath {
  path: string
  alias?: string
}
export interface MqttQuery extends DataQuery {
  topic?: string;
  stream?: boolean;
  gjsonpaths?: GJSONPath[];
}

export interface MqttDataSourceOptions extends DataSourceJsonData {
  uri: string;
  username?: string;
}

export interface MqttSecureJsonData {
  password?: string;
}
