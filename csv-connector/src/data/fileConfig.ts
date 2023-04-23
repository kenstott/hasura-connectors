/*
 * Copyright (c) 2023 Kenneth R. Stott.
 */

import {SchemaResponse} from "@hasura/dc-api-types";

export interface FileConfig {
    nulls: string[]
    booleans: {
        positive: string[],
        negative: string[]
    }
    schema: SchemaResponse
}