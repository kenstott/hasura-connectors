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
    xml?: Record<string, { rowTag: string, xsd?: string }[]>,
    xlsx?: Record<string, { sheet: string, address?: string }[]>,
    jars?: string[],
    schema?: SchemaResponse
}

export interface SparkColumnMetadata {
    name: string;
    type: 'integer' | 'string';
    nullable: boolean;
    metadata: Record<string, any>
}

export type ElementType = 'struct' | 'long' | 'string' | 'double' | 'integer' | 'array' | 'timestamp' | 'float';

export interface SparkTableMetadata {
    type: ElementType | SparkTableMetadata;
    name: string;
    nullable: boolean;
    fields?: SparkTableMetadata[];
    elementType?: ElementType | SparkTableMetadata
}


export interface SparkRowResults {
    total: number,
    rows?: Record<string, RawScalarValue>[]
}