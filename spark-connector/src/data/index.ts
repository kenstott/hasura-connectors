import {Query, SchemaResponse, TableName} from "@hasura/dc-api-types"
import {Casing, Config} from "../config";
import fs from "fs"
import {mapObject, mapObjectValues, tableNameEquals, unreachable} from "../util";
import {pascalCase} from "pascal-case";
import _ from "lodash";
import {getTableRows} from "./spark/spark";
import {SparkRowResults} from "./spark/types";

export type StaticData = {
    [tableName: string]: Record<string, string | number | boolean | null>[]
}

export const schema: SchemaResponse = {
    tables: []
};

export const staticDataExists = async (name: string): Promise<boolean> => {
    return new Promise((resolve) => {
        fs.access(__dirname + "/" + name, fs.constants.R_OK, err => err ? resolve(false) : resolve(true));
    });
}

export const filterAvailableTables = (staticData: StaticData, config: Config): StaticData => {
    return Object.fromEntries(Object.entries(staticData).filter(([name, _]) => config.tables === null ? true : config.tables.indexOf(name) >= 0));
}

const lookupOriginalTable = async (tableName: string, staticData: StaticData, config: Config, query: Query | null): Promise<SparkRowResults> => {
    switch (config.table_name_casing) {
        case "pascal_case": {
            const name = Object.keys(staticData).find(originalTableName => pascalCase(originalTableName) === tableName);
            if (name == undefined) throw new Error(`Unknown table name: ${tableName}`);
            return await getTableRows(name, config, query);
        }
        case "lowercase": {
            const name = Object.keys(staticData).find(originalTableName => originalTableName.toLowerCase() === tableName);
            if (name == undefined) throw new Error(`Unknown table name: ${tableName}`);
            return await getTableRows(name, config, query);
        }
        default:
            return unreachable(config.table_name_casing);
    }
};

const transformData = ({total, rows}: SparkRowResults, config: Config): SparkRowResults => {
    switch (config.column_name_casing) {
        case "pascal_case":
            return {
                total, rows: rows?.map(row => mapObject(row, ([column, value]) => [_.camelCase(column), value]))
            };
        case "lowercase":
            return {
                total, rows: rows?.map(row => mapObject(row, ([column, value]) => [column.toLowerCase(), value]))
            };
        default:
            return unreachable(config.column_name_casing);
    }
};

const lookupTable = async (tableName: string, staticData: StaticData, config: Config, query: Query | null) => {
    return transformData(await lookupOriginalTable(tableName, staticData, config, query), config);
};

export const getTable = async (staticData: StaticData, config: Config): Promise<(tableName: TableName, query: Query | null) => Promise<SparkRowResults>> => {
    return async (tableName: TableName, query: Query | null): Promise<SparkRowResults> => {
        if (config.schema) {
            return tableName.length === 2 && tableName[0] === config.schema ? await lookupTable(tableName[1], staticData, config, query) : {total: 0};
        } else {
            return tableName.length === 1 ? await lookupTable(tableName[0], staticData, config, query) : {total: 0};
        }
    };
}

export const applyCasingTable = (casing: Casing) => (str: string): string => {
    switch (casing) {
        case "pascal_case":
            return pascalCase(str);
        case "lowercase":
            return str.toLowerCase();
        default:
            return unreachable(casing);
    }
}
export const applyCasingColumn = (casing: Casing) => (str: string): string => {
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

    const prefixSchemaToTableName = (tableName: TableName) => config.schema ? [config.schema, ...tableName] : tableName;

    const filteredTables = schema.tables.filter(table => config.tables === null ? true : config.tables.map(n => [n]).find(tableNameEquals(table.name)) !== undefined);

    const prefixedTables = filteredTables.map(table => ({
        ...table,
        name: prefixSchemaToTableName(table.name.map(applyTableNameCasing)),
        primary_key: table.primary_key?.map(applyColumnNameCasing),
        foreign_keys: table.foreign_keys ? mapObjectValues(table.foreign_keys, constraint => ({
            ...constraint,
            foreign_table: prefixSchemaToTableName(constraint.foreign_table.map(applyTableNameCasing)),
            column_mapping: mapObject(constraint.column_mapping, ([outer, inner]) => [applyColumnNameCasing(outer), applyColumnNameCasing(inner)])
        })) : table.foreign_keys,
        columns: table.columns.map(column => ({
            ...column, name: applyColumnNameCasing(column.name),
        }))
    }));

    return {
        ...schema, tables: prefixedTables
    };
};
