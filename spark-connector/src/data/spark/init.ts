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
import {server} from '../../index';

export let sparkSession: number = 0;
export let sparkConfig: SparkConfig;

const supportedFileTypes = ['.csv', '.json', '.xml', '.xlsx']
const fileFormats: Record<string, string> = {
    '.csv': 'csv',
    '.json': 'json',
    '.xml': 'com.databricks.spark.xml',
    '.xlsx': 'com.crealytics.spark.excel'
}
const supportedFile = (file: string) => {
    return supportedFileTypes.indexOf(path.extname(file).toLowerCase()) > -1 && path.basename(file) !== 'config.json'
}

const createSqlContext = async (files: string[]): Promise<void> => {
    files = files.filter(supportedFile)
    const codeOther = files.filter((file) => path.extname(file) != '.xlsx' && path.extname(file) != '.xml').map((file: string) => {
        // JSON files need the multiLine feature for loading
        const multiLine = path.extname(file) == '.json' ? '.option("multiLine", "true")' : ''
        // CSV files need the header feature for loading
        const header = path.extname(file) == '.csv' ? '.option("header", "true")' : ''
        const tableName = fixFileName(path.parse(file).name);
        const loadStatement = file.startsWith("local:") ? `.load("${file.replace("local:", "")}")` : `.load(org.apache.spark.SparkFiles.get("${path.basename(file)}"))`;
        return `
        // Load file into a dataframe within the spark sql context
        val ${tableName} = spark.read.format("${fileFormats[path.extname(file)]}")${multiLine}${header}.option("inferSchema", "true")${loadStatement}
        // Give it a table name to refer to it later
        ${tableName}.createOrReplaceTempView("${tableName}")
        `
    }).join("\n")
    const codeXlsx = files
        .filter((file) => path.extname(file) == '.xlsx')
        .map((file: string) => {
            const tableName = fixFileName(path.parse(file).name);
            const xlsxName = path.basename(file);
            const loadStatement = file.startsWith("local:") ? `.load("${file.replace("local:", "")}")` : `.load(org.apache.spark.SparkFiles.get("${path.basename(file)}"))`;
            return sparkConfig.xlsx?.[xlsxName]?.map(({sheet, address}) => {
                const dataAddress = `"'${sheet}'!${address || 'A1'}"`;
                return `
        // Load file into a dataframe within the spark sql context
        val ${tableName}${sheet} = spark.read.excel(header = true, inferSchema=true, dataAddress = ${dataAddress})${loadStatement}
        // Give it a table name to refer to it later
        ${tableName}${sheet}.createOrReplaceTempView("${tableName}${sheet}")
        `
            }).join("\n")
        }).join("\n")
    const codeXml = files
        .filter((file) => path.extname(file) == '.xml')
        .map((file: string) => {
            const tableName = fixFileName(path.parse(file).name);
            const xmlName = path.basename(file);
            const loadStatement = file.startsWith("local:") ? `.load("${file.replace("local:", "")}")` : `.load(org.apache.spark.SparkFiles.get("${path.basename(file)}"))`;
            return sparkConfig.xml?.[xmlName]?.map(({rowTag, xsd}) => {
                const loadXsdStatement = xsd
                    ? `val schema = XSDToSchema.read(${xsd.startsWith("local:") ? xsd.replace("local:", "") : "org.apache.spark.SparkFiles.get(\"" + path.basename(xsd) + "\")"}` : '';
                return `
        // Load file into a dataframe within the spark sql context
        ${loadXsdStatement}
        val ${tableName}${rowTag} = spark.read.format("${fileFormats['.xml']}")${loadXsdStatement ? ".schema(schema)" : ''}.option("inferSchema", "true").option("rowTag", "${rowTag}")${loadStatement}
        // Give it a table name to refer to it later
        ${tableName}${rowTag}.createOrReplaceTempView("${tableName}${rowTag}")
        `
            }).join("\n")
        }).join("\n")
    const code = `${codeXlsx ? `
import org.apache.spark.sql._
import com.crealytics.spark.excel._
` : ""}
${codeXml ? `
import com.databricks.spark.xml.util.XSDToSchema
` : ""}
${codeOther}
${codeXlsx}
${codeXml}
`
    server.log.info(`// Loading data into spark session...
    ${code}`)
    const response = await axios.post(`${process.env.LIVY_URI}/sessions/${sparkSession}/statements`, {code})
    await waitOnStatementResponse(response);
}
const createSparkSession = async (files: string[] = []): Promise<number> => {
    const response = await axios.post(`${process.env.LIVY_URI}/sessions`, {
        "kind": "spark",
        files,
        jars: sparkConfig.jars
    })
    return (await waitOnSessionResponse(response)).data.id;
}

export const fixFileName = (file: string): string => file.replace(/[\W_]+/g, "_");
export const loadSqlContext = async (name: string): Promise<StaticData> => {
    const sparkConfigPath = path.resolve(name, "config.json");
    sparkConfig = fs.existsSync(sparkConfigPath) ? JSON.parse(fs.readFileSync(sparkConfigPath).toString()) : {
        nulls: [],
        booleans: {positive: [], negative: []},
        xlsx: {},
        remoteFiles: [],
        schema: {
            tables: []
        }
    };
    const files = fs.readdirSync(name).filter(supportedFile)
        .map((file) => `local:${path.resolve(name, file)}`)
        .concat(sparkConfig.remoteFiles || []);
    const staticData = files.reduce((arr: StaticData, file: string) => {
        if (path.extname(file) == '.xlsx') {
            sparkConfig.xlsx?.[path.basename(file)].forEach((sheet) => {
                arr[fixFileName(path.parse(file).name) + sheet] = []
            })
        } else {
            arr[fixFileName(path.parse(file).name)] = []
        }
        return arr;
    }, {});
    sparkSession = await createSparkSession(files);
    await createSqlContext(files);
    for (let i = 0; i < files.length; i++) {
        const tableName = fixFileName(path.parse(files[i]).name);
        const fileName = path.basename(files[i]);
        const subName =
            sparkConfig.xlsx?.[fileName]?.map((i) => i.sheet) ||
            sparkConfig.xml?.[fileName]?.map((i) => i.rowTag) || ['']
        for(let i = 0; i < subName.length; i++) {
            const metaData = await getTableMetadata(`${tableName}${subName[i]}`);
            schema.tables.push({
                name: [`${tableName}${subName[i]}`],
                type: 'table',
                insertable: false,
                updatable: false,
                deletable: false,
                columns: (metaData.fields || [])
                    // filter out any JSON columns or Arrays - they are not supported by Hasura connector
                    .filter((field) => typeof field.type == 'string')
                    .map((field) => ({
                        name: field.name,
                        type: field.type as string,
                        nullable: field.nullable,
                        insertable: false,
                        updatable: false,
                    }))
            })
        }
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