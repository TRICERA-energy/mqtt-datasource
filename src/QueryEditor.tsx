import React, { useState } from 'react';
import { Button, Input, InlineFieldRow, InlineField } from '@grafana/ui';
import { QueryEditorProps } from '@grafana/data';
import { DataSource } from './datasource';
import { GJSONPath, MqttDataSourceOptions, MqttQuery } from './types';
import { handlerFactory } from 'handleEvent';

type Props = QueryEditorProps<DataSource, MqttQuery, MqttDataSourceOptions>;

export const QueryEditor = (props: Props) => {
  const { query, onChange, onRunQuery } = props;
  const handleEvent = handlerFactory(query, onChange);
  const [paths, setPaths] = useState<GJSONPath[]>(query.gjsonpaths ?? []);

  const onQueryChanged = (event: React.ChangeEvent<HTMLInputElement>, path: "alias" | "path", key: number) => {
    handleEvent(`gjsonpaths[${key}].${path}`)(event);
    if (path === "alias") {
      paths[key].alias = event.currentTarget.value
    } else {
      paths[key].path = event.currentTarget.value
    }
    setPaths([...paths]);
  };

  const addGJSONPath = () => {
    if (paths.length === 0 || paths[paths.length - 1]) {
      paths.push({path: ''});
      setPaths([...paths]);
    }
  };

  const removeGJSONPath = (key: number) => {
    if (query.gjsonpaths) {
      query.gjsonpaths.splice(key, 1)
    }
    paths.splice(key, 1);
    setPaths([...paths]);
  };

  return (
    <>
      <InlineFieldRow>
        <InlineField label="Topic" labelWidth={8} grow>
          <Input
            name="topic"
            required
            placeholder='e.g. "home/bedroom/temperature"'
            value={query.topic}
            onBlur={onRunQuery}
            onChange={handleEvent('topic')}
          />
        </InlineField>
      </InlineFieldRow>
      {!!paths.length &&
        paths.map((value, key) => (
          <InlineFieldRow key={key}>
            <InlineField
              label="Filter"
              labelWidth={8}
              tooltip={
                <div>
                  A <a href="https://github.com/tidwall/gjson">GJSON Path</a> query that selects one or more values from
                  a JSON object.
                </div>
              }
              grow
            >
              <Input
                name="gjsonpath"
                placeholder="e.g. nested.valueA"
                onBlur={onRunQuery}
                onChange={(event: React.ChangeEvent<HTMLInputElement>) => onQueryChanged(event, "path", key)}
                value={value.path}
              />
            </InlineField>
            <InlineField
              label="Alias"
              labelWidth={8}
              grow
            >
              <Input
                name="alias"
                onBlur={onRunQuery}
                onChange={(event: React.ChangeEvent<HTMLInputElement>) => onQueryChanged(event, "alias", key)}
                value={value.alias}
              />
            </InlineField>
            <Button onClick={() => removeGJSONPath(key)} size="md" variant={'secondary'}>
              Remove Filter
            </Button>
          </InlineFieldRow>
        ))}
      <InlineFieldRow>
        <Button onClick={addGJSONPath} size="md" variant={'secondary'}>
          Add Filter
        </Button>
      </InlineFieldRow>
    </>
  );
};
