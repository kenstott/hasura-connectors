/*
 * Copyright (c) 2023 Kenneth R. Stott.
 */

import {SchemaResponse} from "@hasura/dc-api-types";
import {RawScalarValue} from "../../query";

export interface SparkConfig {
    nulls?: string[],
    remoteFiles?: string[],
    coerceForeignKeys: {
        [table: string]: {
            [column: string]: 'integer' | 'string' | 'float'
        }
    },
    booleans?: {
        positive: string[],
        negative: string[]
    },
    xlsx?: Record<string, string[]>,
    jars?: string[],
    schema?: SchemaResponse
}

export interface SparkColumnMetadata {
    name: string;
    type: 'integer' | 'string';
    nullable: boolean;
    metadata: Record<string, any>
}

export interface SparkTableMetadata {
    type: 'struct',
    fields: SparkColumnMetadata[]
}

export interface SparkRowResults {
    total: number,
    rows?: Record<string, RawScalarValue>[]
}