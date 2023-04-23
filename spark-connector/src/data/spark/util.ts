import {
    ApplyBinaryArrayComparisonOperator,
    BinaryComparisonOperator,
    Expression,
    Field,
    OrderByColumn,
    OrderByElement,
    ScalarValueComparison
} from "@hasura/dc-api-types";
import {applyCasingColumn, applyCasingTable, schema} from "../index";
import {Config} from "../../config";

export const changeOrderByColumnNames = (tableName: string, config: Config, orderBy?: OrderByElement[]) => {
    const tableNameCasing = applyCasingTable(config.table_name_casing);
    const columnCasing = applyCasingColumn(config.column_name_casing);
    const table = schema.tables.find((table) => tableNameCasing(table.name[0]) == tableNameCasing(tableName));
    return orderBy?.map((element) => {
        const columnName = table?.columns.find((column) => (element.target as OrderByColumn).column == columnCasing(column.name))?.name;
        return {columnName, direction: element.order_direction}
    })
}

export const getFieldNames = (tableName: string, config: Config, fields?: Record<string, Field> | null): string[] => {
    const tableNameCasing = applyCasingTable(config.table_name_casing);
    const columnCasing = applyCasingColumn(config.column_name_casing);
    const table = schema.tables.find((table) => tableNameCasing(table.name[0]) == tableNameCasing(tableName));
    return [...new Set(Object.keys(fields ?? {}).map((key: string) => table?.columns.find((column) => key == columnCasing(column.name))?.name ?? '')
        .filter(Boolean))];
}
export const changeWhereColumnNames = (tableName: string, config: Config, where?: Record<string, any>): Record<string, any> | undefined => {
    const tableNameCasing = applyCasingTable(config.table_name_casing);
    const columnCasing = applyCasingColumn(config.column_name_casing);
    const table = schema.tables.find((table) => tableNameCasing(table.name[0]) == tableNameCasing(tableName));
    return Object.keys(where ?? {}).reduce((acc: Record<string, any>, key): Record<string, any> => {
        const originalValue = where?.[key];
        if (key == 'expressions') {
            const expressions = originalValue as Expression[];
            acc[key] = expressions.map((e) => changeWhereColumnNames(tableName, config, e))
        } else if (key == 'expression') {
            acc[key] = changeWhereColumnNames(tableName, config, originalValue);
        } else if (key == 'column') {
            const name = table?.columns.find((column) => originalValue.name == columnCasing(column.name))?.name;
            acc[key] = {...originalValue, name}
        } else {
            acc[key] = where?.[key];
        }
        return acc;
    }, {} as Record<string, any>);
}
export const getOperator: Record<BinaryComparisonOperator, string> = {
    'less_than': '<',
    'greater_than': '>',
    'less_than_or_equal': '<=',
    'greater_than_or_equal': '>=',
    'equal': '=',
    'in': ''
}

function instanceOfApplyBinaryArrayComparisonOperator(object: any): object is ApplyBinaryArrayComparisonOperator {
    return object;
}

export const getValueString = (value: ScalarValueComparison | ApplyBinaryArrayComparisonOperator): string => {
    if (instanceOfApplyBinaryArrayComparisonOperator(value)) {
        return value.values.map((v) => {
            if (value.value_type == 'string') {
                return `"${v}"`
            } else {
                return `${v}`
            }
        }).join(", ");
    } else {
        if (value.value_type == 'string') {
            return `"${value.value}"`
        } else {
            return `${value.value}`
        }
    }
}