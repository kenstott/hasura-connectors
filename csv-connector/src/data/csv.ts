import fs from "fs";
import {parse} from "csv-parse";
import {convertToTypedDataSet, guessPrimaryKey} from "./guessData";
import {FileConfig} from "./fileConfig";

export interface CsvResult {
    records: Record<string, string | number | boolean | null>[],
    columns: Record<string, 'number' | 'boolean' | 'string' | 'DateTime' | null>
    primaryKey: string | undefined
}

export const loadCsv = (name: string, config: FileConfig): Promise<CsvResult> => {
    return new Promise<CsvResult>((resolve, reject) => {
        let columns: string[] = [];
        const records: string[][] = [];
        fs.createReadStream(name)
            .pipe(parse({delimiter: ",", from_line: 1}))
            .on("data", function (row: string[]) {
                if (columns.length == 0) {
                    columns = row
                } else {
                    records.push(row);
                }
            })
            .on("end", function () {
                console.log("finished");
                const {typedRecords, typedColumns} = convertToTypedDataSet(columns, records, config);
                resolve({
                    records: typedRecords,
                    columns: typedColumns,
                    primaryKey: guessPrimaryKey(records, columns)
                });
            })
            .on("error", function (error) {
                console.log(error.message);
                reject(error)
            });
    });
}