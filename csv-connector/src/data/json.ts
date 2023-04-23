/*
 * Copyright (c) 2023 Kenneth R. Stott.
 */

import {FileConfig} from "./fileConfig";
import path from "node:path";
import fs from "fs";
import {flatten} from 'flat';
import {convertToTypedDataSet, guessPrimaryKey} from "./guessData";

export interface JsonResult {
    records: Record<string, string | number | boolean | null>[],
    columns: Record<string, 'number' | 'boolean' | 'string' | 'DateTime' | null>,
    primaryKey: string | undefined,
    tableName: string
}

export const loadJson = (name: string, config: FileConfig): Promise<JsonResult> => {
    return new Promise<JsonResult>((resolve, reject) => {
        try {
            const tableName = path.basename(name).replace('.json', '');
            const records: Record<string, any>[] = JSON.parse(fs.readFileSync(name).toString());
            const flattedRecords: Record<string, any>[] = records.map((record) => flatten(record, {safe: true}))
            const columns = Object.keys(flattedRecords[0]);
            const {typedRecords, typedColumns} = convertToTypedDataSet(columns, flattedRecords, config);
            resolve({
                records: typedRecords,
                columns: typedColumns,
                primaryKey: guessPrimaryKey(flattedRecords, columns),
                tableName
            });
        } catch(error) {
            reject(error)
        }
    });
}