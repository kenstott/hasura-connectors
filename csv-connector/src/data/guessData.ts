/*
 * Copyright (c) 2023 Kenneth R. Stott.
 */

import moment from "moment/moment";
import {FileConfig} from "./fileConfig";

export const guessPrimaryKey = (
    values: ((string | number | boolean | null)[] | Record<string | number, string | number | boolean | null>)[],
    columns: string[]): string | undefined => {
    return columns.find((column: unknown, index: number) => {
        let uniqueValues = new Set(values.map((i: (string | number | boolean | null)[] | Record<string | number, string | number | boolean | null>) => i[index] || i[column as number]))
        return uniqueValues.size == values.length
    })
}
const guessDataTypes = (
    values: ((string | number | boolean | null)[] | Record<string | number, string | number | boolean | null>)[],
    columns: string[],
    config: FileConfig): Record<string, 'number' | 'DateTime' | 'string' | 'boolean'> => {
    return columns.reduce((arr: Record<string, 'number' | 'DateTime' | 'string' | 'boolean'>, key: unknown, columnIndex): Record<string, 'number' | 'DateTime' | 'string' | 'boolean'> => {
        const guesses = values.reduce((obj: Record<string, boolean>, record): Record<string, boolean> => {
            let typeCount = Object.keys(obj).length;
            if (typeCount < 2) {
                let value = record[columnIndex] || record[key as number];
                let stringValue = (value ?? '').toString();
                if (value !== null && config.nulls.indexOf(stringValue) == -1) {
                    if (config.booleans.positive.indexOf(stringValue) !== -1) {
                        obj['boolean'] = true;
                    } else if (config.booleans.negative.indexOf(stringValue) !== -1) {
                        obj['boolean'] = true;
                    }
                    if (!isNaN(Number(stringValue))) {
                        obj['number'] = true;
                    } else if (moment(stringValue).isValid()) {
                        obj['DateTime'] = true;
                    } else {
                        obj['string'] = true
                    }
                }
            }
            return obj;
        }, {});
        if (Object.keys(guesses).length == 0 || Object.keys(guesses).length > 1) {
            arr[key as string] = 'string';
        } else {
            arr[key as string] = Object.keys(guesses)[0] as any;
        }
        return arr
    }, {});
}
export const convertToTypedDataSet = (
    keys: string[],
    values: ((string | number | boolean | null)[] | Record<string | number, string | number | boolean | null>)[],
    config: FileConfig): {
    typedRecords: Record<string, any>[], typedColumns: Record<string, 'number' | 'DateTime' | 'string' | 'boolean'>
} => {
    let typedColumns = guessDataTypes(values, keys, config);
    let typedRecords = values.map((value) => {
        return keys.reduce((arr: Record<string, any>, key: unknown, index: number): Record<string, any> => {
            let rawValue = value[index] || value[key as number];
            let stringValue = (rawValue || '').toString();
            if (rawValue == null || config.nulls.indexOf(stringValue) !== -1) {
                arr[key as string] = null;
            } else {
                switch (typedColumns[key as string]) {
                    case 'number':
                        arr[key as string] = parseFloat(stringValue);
                        break;
                    case 'DateTime':
                        arr[key as string] = moment(stringValue).toDate();
                        break;
                    case 'boolean':
                        if (config.booleans.positive.indexOf(stringValue) !== -1) {
                            arr[key as string] = true;
                        }
                        if (config.booleans.negative.indexOf(stringValue) !== -1) {
                            arr[key as string] = false;
                        }
                        break;
                    default:
                        arr[key as string] = stringValue;
                        break;
                }
            }
            return arr;
        }, {})
    })
    return {typedRecords, typedColumns};
}