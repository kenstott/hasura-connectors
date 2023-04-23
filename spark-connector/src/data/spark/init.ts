/*
 * Copyright (c) 2023 Kenneth R. Stott.
 */

import {schema, StaticData} from "../index";
import {getTableMetadata} from "./metadata";
import {waitOnSessionResponse, waitOnStatementResponse} from "../livy";
import path from "node:path";
import fs from 'node:fs';
import axios from "axios";
import {SparkConfig} from "./types";

export let sparkSession: number = 0;
export let sparkConfig: SparkConfig;

const supportedFileTypes = ['.csv', '.json', '.xml']
const fileFormats: Record<string, string> = {
    '.csv': 'csv',
    '.json': 'json',
    '.xml': 'xml'
}
const supportedFile = (file: string) => {
    return supportedFileTypes.indexOf(path.extname(file).toLowerCase()) > -1 && path.basename(file) !== 'config.json'
}

const createSqlContext = async (files: string[]): Promise<void> => {
    files = files.filter(supportedFile)
    const code = files.map((file: string) => {
        // JSON files need the multiLine feature for loading
        const multiLine = path.extname(file) == '.json' ? '.option("multiLine", "true")' : ''
        // CSV files need the header feature for loading
        const header = path.extname(file) == '.csv' ? '.option("header", "true")' : ''
        const tableName = path.parse(file).name;
        const loadStatement = file.startsWith("local:") ? `.load("${file.replace("local:", "")}")` : `.load(org.apache.spark.SparkFiles.get("${path.basename(file)}"))`;
        return `
        // Load file into a dataframe within the spark sql context
        val ${tableName} = spark.sqlContext.read.format("${fileFormats[path.extname(file)]}")${multiLine}${header}.option("inferSchema", "true")${loadStatement}
        // Give it a table name to refer to it later
        ${tableName}.createOrReplaceTempView("${tableName}")
        `
    }).join("\n")
    const response = await axios.post(`${process.env.LIVY_URI}/sessions/${sparkSession}/statements`, {code})
    await waitOnStatementResponse(response);
}
const createSparkSession = async (files: string[] = []): Promise<number> => {
    const response = await axios.post(`${process.env.LIVY_URI}/sessions`, {
        "kind": "spark",
        files
    })
    return (await waitOnSessionResponse(response)).data.id;
}
export const loadSqlContext = async (name: string): Promise<StaticData> => {
    const sparkConfigPath = path.resolve(name, "config.json");
    sparkConfig = fs.existsSync(sparkConfigPath) ? JSON.parse(fs.readFileSync(sparkConfigPath).toString()) : {
        nulls: [],
        booleans: {positive: [], negative: []},
        remoteFiles: [],
        schema: {
            tables: []
        }
    };
    const files = fs.readdirSync(name).filter(supportedFile)
        .map((file) => `local:${path.resolve(name, file)}`)
        .concat(sparkConfig.remoteFiles || [])
    const staticData = files.reduce((arr: StaticData, file: string) => {
        arr[path.parse(file).name] = []
        return arr;
    }, {});
    sparkSession = await createSparkSession(files);
    await createSqlContext(files);
    for (let i = 0; i < files.length; i++) {
        const tableName = path.parse(files[i]).name;
        const metaData = await getTableMetadata(tableName);
        schema.tables.push({
            name: [tableName],
            type: 'table',
            insertable: false,
            updatable: false,
            deletable: false,
            columns: metaData.fields
                // filter out any JSON columns or Arrays - they are not supported by Hasura connector
                .filter((field) => typeof field.type == 'string')
                .map((field) => ({
                    name: field.name,
                    type: field.type,
                    nullable: field.nullable,
                    insertable: false,
                    updatable: false,
                }))
        })
    }
    schema.tables.forEach((table, index) => {
        let overrideTable = sparkConfig.schema?.tables?.find((oTable: {
            name: string[];
        }) => oTable?.name?.[0].toLowerCase() == table.name[0].toLowerCase())
        let columns = table.columns;
        if (overrideTable) {
            schema.tables[index] = Object.assign(table, overrideTable, {columns});
            columns.forEach((column, columnIndex) => {
                let overrideColumn = overrideTable?.columns?.find((oColumn: {
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