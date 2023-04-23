/*
 * Copyright (c) 2023 Kenneth R. Stott.
 */

import {SchemaResponse, TableName} from "@hasura/dc-api-types"
import {Casing, Config} from "../config";
import fs from "fs"
import {mapObject, mapObjectValues, tableNameEquals, unreachable} from "../util";
import path from "node:path";
import {loadCsv} from "./csv";
import {pascalCase} from "pascal-case";
import _ from "lodash";
import {loadXlsx} from "./xlsx";
import {loadJson} from "./json";

export type StaticData = {
    [tableName: string]: Record<string, string | number | boolean | null>[]
}

export const staticDataExists = async (name: string): Promise<boolean> => {
    return new Promise((resolve) => {
        fs.access(__dirname + "/" + name, fs.constants.R_OK, err => err ? resolve(false) : resolve(true));
    });
}


export const loadStaticData = async (name: string): Promise<StaticData> => {
    const staticData: Record<string, Record<string, string | number | boolean | null>[]> = {}
    const dbName = path.basename(name);
    const csvConfigPath = path.resolve(name, "config.json");
    const csvConfig = fs.existsSync(csvConfigPath) ? JSON.parse(fs.readFileSync(csvConfigPath).toString()) : {
        nulls: [],
        booleans: {positive: [], negative: []},
        schema: {
            tables: []
        }
    };
    schema[`$${dbName}`] = {tables: []};
    const files = fs.readdirSync(name)
    for (let i = 0; i < files.length; i++) {
        const file = files[i];
        if (path.extname(file).toLowerCase() == '.csv') {
            const {records, columns, primaryKey} = await loadCsv(path.resolve(name, file), csvConfig)
            staticData[path.parse(file).name] = records;
            schema[`$${dbName}`].tables.push({
                name: [path.parse(file).name],
                type: 'table',
                primary_key: primaryKey ? [primaryKey] : undefined,
                insertable: false,
                updatable: false,
                deletable: false,
                columns: Object.keys(columns).map((name) => {
                    return {
                        name,
                        type: columns[name] ?? 'string',
                        nullable: true,
                        insertable: false,
                        updatable: false,
                    }
                })
            });
        } else if (file !== 'config.json' && path.extname(file).toLowerCase() == '.json') {
            const {records, columns, primaryKey} = await loadJson(path.resolve(name, file), csvConfig)
            staticData[file.replace('.json', '')] = records;
            schema[`$${dbName}`].tables.push({
                name: [file.replace('.json', '')],
                type: 'table',
                primary_key: primaryKey ? [primaryKey] : undefined,
                insertable: false,
                updatable: false,
                deletable: false,
                columns: Object.keys(columns).map((name) => {
                    return {
                        name,
                        type: columns[name] ?? 'string',
                        nullable: true,
                        insertable: false,
                        updatable: false,
                    }
                })
            });
        } else if (path.extname(file).toLowerCase() == '.xlsx') {
            const results = await loadXlsx(path.resolve(name, file), csvConfig)
            results.forEach(({sheetName, records, columns, primaryKey}) => {
                const tableName = `${file.replace('.xlsx', '')}_${sheetName}`;
                staticData[tableName] = records;
                schema[`$${dbName}`].tables.push({
                    name: [tableName],
                    type: 'table',
                    primary_key: primaryKey ? [primaryKey] : undefined,
                    insertable: false,
                    updatable: false,
                    deletable: false,
                    columns: Object.keys(columns).map((name) => {
                        return {
                            name,
                            type: columns[name] ?? 'string',
                            nullable: true,
                            insertable: false,
                            updatable: false,
                        }
                    })
                });
            })
        }
    }
    schema[`$${dbName}`].tables.forEach((table, index) => {
        let overrideTable = csvConfig.schema?.tables?.find((oTable: {
            name: string[];
        }) => oTable?.name?.[0].toLowerCase() == table.name[0].toLowerCase())
        let columns = table.columns;
        if (overrideTable) {
            schema[`$${dbName}`].tables[index] = Object.assign(table, overrideTable, {columns});
            columns.forEach((column, columnIndex) => {
                let overrideColumn = overrideTable.columns?.find((oColumn: {
                    name: string;
                }) => oColumn.name == column.name);
                if (overrideColumn) {
                    columns[columnIndex] = Object.assign(column, overrideColumn);
                }
            })
        }
    })
    return staticData;
}

export const filterAvailableTables = (staticData: StaticData, config: Config): StaticData => {
    return Object.fromEntries(
        Object.entries(staticData).filter(([name, _]) => config.tables === null ? true : config.tables.indexOf(name) >= 0)
    );
}

export const getTable = (staticData: StaticData, config: Config): ((tableName: TableName) => Record<string, string | number | boolean | null>[] | undefined) => {
    const cachedTransformedData: StaticData = {};

    const lookupOriginalTable = (tableName: string): Record<string, string | number | boolean | null>[] => {
        switch (config.table_name_casing) {
            case "pascal_case": {
                const name = Object.keys(staticData).find(originalTableName => pascalCase(originalTableName) === tableName);
                if (name == undefined) throw new Error(`Unknown table name: ${tableName}`);
                return staticData[name];
            }
            case "lowercase": {
                const name = Object.keys(staticData).find(originalTableName => originalTableName.toLowerCase() === tableName);
                if (name == undefined) throw new Error(`Unknown table name: ${tableName}`);
                return staticData[name];
            }
            default:
                return unreachable(config.table_name_casing);
        }
    };

    const transformData = (tableData: Record<string, string | number | boolean | null>[]): Record<string, string | number | boolean | null>[] => {
        switch (config.column_name_casing) {
            case "pascal_case":
                return tableData.map(row => mapObject(row, ([column, value]) => [_.camelCase(column), value]));
            case "lowercase":
                return tableData.map(row => mapObject(row, ([column, value]) => [column.toLowerCase(), value]));
            default:
                return unreachable(config.column_name_casing);
        }
    };

    const lookupTable = (tableName: string): Record<string, string | number | boolean | null>[] => {
        const cachedData = cachedTransformedData[tableName];
        if (cachedData !== undefined)
            return cachedData;

        cachedTransformedData[tableName] = transformData(lookupOriginalTable(tableName));
        return cachedTransformedData[tableName];
    };

    return (tableName) => {
        if (config.schema) {
            return tableName.length === 2 && tableName[0] === config.schema
                ? lookupTable(tableName[1])
                : undefined;
        } else {
            return tableName.length === 1
                ? lookupTable(tableName[0])
                : undefined;
        }
    };
}

const schema: Record<string, SchemaResponse> = {};

const applyCasingTable = (casing: Casing) => (str: string): string => {
    switch (casing) {
        case "pascal_case":
            return pascalCase(str);
        case "lowercase":
            return str.toLowerCase();
        default:
            return unreachable(casing);
    }
}
const applyCasingColumn = (casing: Casing) => (str: string): string => {
    switch (casing) {
        case "pascal_case":
            return _.camelCase(str);
        case "lowercase":
            return str.toLowerCase();
        default:
            return unreachable(casing);
    }
}

export const getSchema = (config: Config): SchemaResponse => {
    const applyTableNameCasing = applyCasingTable(config.table_name_casing);
    const applyColumnNameCasing = applyCasingColumn(config.column_name_casing);

    const prefixSchemaToTableName = (tableName: TableName) =>
        config.schema
            ? [config.schema, ...tableName]
            : tableName;

    const filteredTables = schema[`$${config.db}`].tables.filter(table =>
        config.tables === null ? true : config.tables.map(n => [n]).find(tableNameEquals(table.name)) !== undefined
    );

    const prefixedTables = filteredTables.map(table => ({
        ...table,
        name: prefixSchemaToTableName(table.name.map(applyTableNameCasing)),
        primary_key: table.primary_key?.map(applyColumnNameCasing),
        foreign_keys: table.foreign_keys
            ? mapObjectValues(table.foreign_keys, constraint => ({
                ...constraint,
                foreign_table: prefixSchemaToTableName(constraint.foreign_table.map(applyTableNameCasing)),
                column_mapping: mapObject(constraint.column_mapping, ([outer, inner]) => [applyColumnNameCasing(outer), applyColumnNameCasing(inner)])
            }))
            : table.foreign_keys,
        columns: table.columns.map(column => ({
            ...column,
            name: applyColumnNameCasing(column.name),
        }))
    }));

    return {
        ...schema[`$${config.db}`],
        tables: prefixedTables
    };
};
