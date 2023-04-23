/*
 * Copyright (c) 2023 Kenneth R. Stott.
 */

import * as XLSX from 'xlsx';
import {convertToTypedDataSet, guessPrimaryKey} from "./guessData";
import {FileConfig} from "./fileConfig";

export interface XlsxSheetResult {
    records: Record<string, string | number | boolean | null>[],
    columns: Record<string, 'number' | 'boolean' | 'string' | 'DateTime' | null>,
    primaryKey: string | undefined,
    sheetName: string
}

export const loadXlsx = (name: string, config: FileConfig): Promise<XlsxSheetResult[]> => {
    return new Promise<XlsxSheetResult[]>((resolve, reject) => {
        try {
            const results: XlsxSheetResult[] = [];
            const workbook = XLSX.readFile(name);
            workbook.SheetNames.forEach((sheetName) => {
                const sheet = workbook.Sheets[sheetName];
                const data: Record<string, any>[] = XLSX.utils.sheet_to_json(sheet, {raw: false});
                const columns = Object.keys(data[0]);
                const {typedRecords, typedColumns} = convertToTypedDataSet(columns, data, config);
                results.push({
                    records: typedRecords,
                    columns: typedColumns,
                    primaryKey: guessPrimaryKey(data, columns),
                    sheetName
                });
            });
            resolve(results);
        } catch(error) {
            reject(error);
        }
    });
}