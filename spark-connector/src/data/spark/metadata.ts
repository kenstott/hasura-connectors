/*
 * Copyright (c) 2023 Kenneth R. Stott.
 */

import axios from "axios";
import {server} from "../..";
import {waitOnStatementResponse} from "../livy";
import {sparkSession} from "./init";
import {ElementType, SparkTableMetadata} from "./types";

const metaDataFixes: Record<ElementType, ElementType> = {
    long: 'integer',
    double: 'float',
    struct: 'struct',
    string: 'string',
    integer: 'integer',
    array: 'array',
    timestamp: 'timestamp',
    float: 'float'
}

const fixMetaData = (data: SparkTableMetadata): SparkTableMetadata => {
    if (typeof data.type == 'object') {
        data.type = fixMetaData(data.type as SparkTableMetadata);
    } else {
        data.type = metaDataFixes[data.type as ElementType]
    }
    data.fields = data.fields?.map((i) => fixMetaData(i));
    if (typeof data.elementType == 'object') {
        data.elementType = fixMetaData(data.elementType as SparkTableMetadata);
    } else {
        data.elementType = metaDataFixes[data.elementType as ElementType];
    }
    return data;
}
export const getTableMetadata = async (tableName: string): Promise<SparkTableMetadata> => {
    const code = `println(${tableName}.schema.json)`
    server.log.info(`// Retrieving metadata inferred by spark...
    ${code}`)
    const response = await waitOnStatementResponse(await axios.post(`${process.env.LIVY_URI}/sessions/${sparkSession}/statements`, {code}));
    const metaData: SparkTableMetadata = JSON.parse(response.data.output.data?.['text/plain']);
    return fixMetaData(metaData);
}