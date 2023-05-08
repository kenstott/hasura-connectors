import {
    Aggregate,
    ApplyBinaryArrayComparisonOperator,
    BinaryArrayComparisonOperator,
    BinaryComparisonOperator,
    ColumnCountAggregate,
    ComparisonColumn,
    ComparisonValue,
    ExistsInTable,
    Expression,
    Field,
    OrderByElement,
    OrderByRelation,
    Query,
    QueryRequest,
    Relationship,
    ScalarValue,
    SingleColumnAggregate,
    TableInfo,
    TableName,
    TableRelationships,
    UnaryComparisonOperator
} from "@hasura/dc-api-types";
import {coerceUndefinedToNull, tableNameEquals, unreachable} from "./util";
import * as math from "mathjs";
import {SparkRowResults} from "./data/spark/types";
import DataLoader from 'dataloader';
import {Config} from "./config";
import {sparkConfig} from "./data/spark/init";
import {applyCasingColumn, applyCasingTable, schema} from "./data";

type RelationshipName = string

export type RawScalarValue = (string | number | boolean | null)

// This is a more constrained type for response rows that knows that the reference
// agent never returns custom scalars that are JSON objects
type ProjectedRow = {
    [fieldName: string]: RawScalarValue | QueryResponse
}

// We need a more constrained version of QueryResponse that uses ProjectedRow for rows
type QueryResponse = {
    aggregates?: Record<string, RawScalarValue> | null,
    rows?: ProjectedRow[] | null
}


async function fetchAllByKeys(
    relationship: Relationship,
    config: Config,
    performQuery: (tableName: TableName, query: Query | null) => Promise<QueryResponse>,
    keys: readonly any[],
    subquery: Query | null): Promise<Record<string, Record<string, any>> | undefined> {
    const tableName = relationship.target_table[0];
    const values = [...new Set(keys)].filter((i) => typeof i != 'number' || !isNaN(i));
    const where: ApplyBinaryArrayComparisonOperator = {
        type: 'binary_arr_op',
        operator: 'in',
        value_type: 'integer',
        column: {
            name: Object.entries(relationship.column_mapping)[0][1],
            column_type: 'integer'
        },
        values
    }
    const query: Query = {...subquery, limit: null, offset: null, where}
    const result = await performQuery([tableName], query)

    if (relationship.relationship_type == 'object') {
        return result.rows?.reduce((acc: Record<string, Record<string, any>>, row: Record<string, any>) => {
            const rowKey = row[where?.column.name ?? ''].toString();
            if (!acc[rowKey]) {
                acc[rowKey] = row;
            }
            return acc;
        }, {});
    }
    return result.rows?.reduce((acc: Record<string, Record<string, any>>, row: Record<string, any>) => {
        const rowKey = row[where?.column.name ?? ''].toString();
        if (!acc[rowKey]) {
            acc[rowKey] = []
        }
        acc[rowKey].push(row);
        return acc;
    }, {});
}

interface Key {
    relationship: Relationship,
    key: any,
    config: Config
    performQuery: (tableName: TableName, query: Query | null) => Promise<QueryResponse>
    query: Query | null,
    fieldName: string
}

async function batchFunction(key: readonly Key[]): Promise<any[]> {
    const {relationship, config, performQuery, query} = key[0];
    const results = await fetchAllByKeys(
        relationship,
        config,
        performQuery,
        key.map((i) => i.key),
        query
    );
    return key.map(i => {
        const value = results?.[i.key.toString()];
        if (!value) {
            return undefined;
        }
        if (Array.isArray(value)) {
            return value;
        }
        return [value];
    });
}

const loader = new DataLoader<Key, any>(batchFunction);
const prettyPrintBinaryComparisonOperator = (operator: BinaryComparisonOperator): string => {
    switch (operator) {
        case "greater_than":
            return ">";
        case "greater_than_or_equal":
            return ">=";
        case "less_than":
            return "<";
        case "less_than_or_equal":
            return "<=";
        case "equal":
            return "==";
        case "same_day_as":
            return "same_day_as";
        case "in_year":
            return "in_year";
        default:
            return unknownOperator(operator);
    }

};

const prettyPrintBinaryArrayComparisonOperator = (operator: BinaryArrayComparisonOperator): string => {
    switch (operator) {
        case "in":
            return "IN";
        default:
            return unknownOperator(operator);
    }

};

const prettyPrintUnaryComparisonOperator = (operator: UnaryComparisonOperator): string => {
    switch (operator) {
        case "is_null":
            return "IS NULL";
        default:
            return unknownOperator(operator);
    }

};

const prettyPrintComparisonColumn = (comparisonColumn: ComparisonColumn): string => {
    return (comparisonColumn.path ?? []).concat(comparisonColumn.name).map(p => `[${p}]`).join(".");
}

const prettyPrintComparisonValue = (comparisonValue: ComparisonValue): string => {
    switch (comparisonValue.type) {
        case "column":
            return prettyPrintComparisonColumn(comparisonValue.column);
        case "scalar":
            return comparisonValue.value === null ? "null" : comparisonValue.value.toString();
        default:
            return unreachable(comparisonValue["type"]);
    }
};

const prettyPrintTableName = (tableName: TableName): string => {
    return tableName.map(t => `[${t}]`).join(".");
};

const prettyPrintExistsInTable = (existsInTable: ExistsInTable): string => {
    switch (existsInTable.type) {
        case "related":
            return `RELATED TABLE VIA [${existsInTable.relationship}]`;
        case "unrelated":
            return `UNRELATED TABLE ${prettyPrintTableName(existsInTable.table)}`;
    }
};

export const prettyPrintExpression = (e: Expression): string => {
    switch (e.type) {
        case "and":
            return e.expressions.length
                ? `(${e.expressions.map(prettyPrintExpression).join(" && ")})`
                : "true";
        case "or":
            return e.expressions.length
                ? `(${e.expressions.map(prettyPrintExpression).join(" || ")})`
                : "false";
        case "not":
            return `!(${prettyPrintExpression(e.expression)})`;
        case "exists":
            return `(EXISTS IN ${prettyPrintExistsInTable(e.in_table)} WHERE (${prettyPrintExpression(e.where)}))`
        case "binary_op":
            return `(${prettyPrintComparisonColumn(e.column)} ${prettyPrintBinaryComparisonOperator(e.operator)} ${prettyPrintComparisonValue(e.value)})`;
        case "binary_arr_op":
            return `(${prettyPrintComparisonColumn(e.column)} ${prettyPrintBinaryArrayComparisonOperator(e.operator)} (${e.values.join(", ")}))`;
        case "unary_op":
            return `(${prettyPrintComparisonColumn(e.column)} ${prettyPrintUnaryComparisonOperator(e.operator)})`;
        default:
            return unreachable(e["type"]);
    }
};

const buildQueryForPathedOrderByElement = (orderByElement: OrderByElement, orderByRelations: Record<RelationshipName, OrderByRelation>): Query => {
    const [relationshipName, ...remainingPath] = orderByElement.target_path;
    if (relationshipName === undefined) {
        switch (orderByElement.target.type) {
            case "column":
                return {
                    fields: {
                        [orderByElement.target.column]: {
                            type: "column",
                            column: orderByElement.target.column,
                            column_type: "unknown"
                        } // Unknown column type here is a hack because we don't actually know what the column type is and we don't care
                    }
                };
            case "single_column_aggregate":
                return {
                    aggregates: {
                        [orderByElement.target.column]: {
                            type: "single_column",
                            column: orderByElement.target.column,
                            function: orderByElement.target.function,
                            result_type: orderByElement.target.result_type
                        }
                    }
                };
            case "star_count_aggregate":
                return {
                    aggregates: {
                        "count": {type: "star_count"}
                    }
                };
            default:
                return unreachable(orderByElement.target["type"]);
        }
    } else {
        const innerOrderByElement = {...orderByElement, target_path: remainingPath};
        const orderByRelation = orderByRelations[relationshipName];
        const subquery = {
            ...buildQueryForPathedOrderByElement(innerOrderByElement, orderByRelation.subrelations),
            where: orderByRelation.where
        }
        return {
            fields: {
                [relationshipName]: {type: "relationship", relationship: relationshipName, query: subquery}
            }
        };
    }
};

const extractResultFromOrderByElementQueryResponse = (orderByElement: OrderByElement, response: QueryResponse): RawScalarValue => {
    const [relationshipName, ...remainingPath] = orderByElement.target_path;
    const rows = response.rows ?? [];
    const aggregates = response.aggregates ?? {};

    if (relationshipName === undefined) {
        switch (orderByElement.target.type) {
            case "column":
                if (rows.length > 1)
                    throw new Error(`Unexpected number of rows (${rows.length}) returned by order by element query`);

                const fieldValue = rows.length === 1 ? rows[0][orderByElement.target.column] : null;
                if (fieldValue !== null && typeof fieldValue === "object")
                    throw new Error("Column order by target path did not end in a column field value");

                return coerceUndefinedToNull(fieldValue);

            case "single_column_aggregate":
                return aggregates[orderByElement.target.column];

            case "star_count_aggregate":
                return aggregates["count"];

            default:
                return unreachable(orderByElement.target["type"]);
        }
    } else {
        if (rows.length > 1)
            throw new Error(`Unexpected number of rows (${rows.length}) returned by order by element query`);

        const fieldValue = rows.length === 1 ? rows[0][relationshipName] : null;
        if (fieldValue === null || typeof fieldValue !== "object")
            throw new Error(`Found a column field value in the middle of a order by target path: ${orderByElement.target_path}`);

        const innerOrderByElement = {...orderByElement, target_path: remainingPath};
        return extractResultFromOrderByElementQueryResponse(innerOrderByElement, fieldValue);
    }
};

const makeFindRelationship = (allTableRelationships: TableRelationships[], tableName: TableName) => (relationshipName: RelationshipName): Relationship => {
    const relationship = allTableRelationships.find(r => tableNameEquals(r.source_table)(tableName))?.relationships?.[relationshipName];
    if (relationship === undefined)
        throw `No relationship named ${relationshipName} found for table ${tableName}`;
    else
        return relationship;
};

const createFilterExpressionForRelationshipJoin = (row: Record<string, RawScalarValue>, relationship: Relationship): Expression | null => {
    const columnMappings = Object.entries(relationship.column_mapping);
    const filterConditions: Expression[] = columnMappings
        .map(([outerColumnName, innerColumnName]): [RawScalarValue, string] => [row[outerColumnName], innerColumnName])
        .filter((x): x is [RawScalarValue, string] => {
            const [outerValue, _] = x;
            return outerValue !== null;
        })
        .map(([outerValue, innerColumnName]) => {
            const unknownScalarType = "unknown"; // We don't know what the type is and don't care since we never look at it anyway
            return {
                type: "binary_op",
                operator: "equal",
                column: {
                    path: [],
                    name: innerColumnName,
                    column_type: unknownScalarType,
                },
                value: {type: "scalar", value: outerValue, value_type: unknownScalarType}
            };
        });

    if (columnMappings.length === 0 || filterConditions.length !== columnMappings.length) {
        return null;
    } else {
        return {type: "and", expressions: filterConditions}
    }
};

const addRelationshipFilterToQuery = (row: Record<string, RawScalarValue>, relationship: Relationship, subquery: Query): Query | null => {
    const filterExpression = createFilterExpressionForRelationshipJoin(row, relationship);

    // If we have no columns to join on, or if some of the FK columns in the row contained null, then we can't join
    if (filterExpression === null) {
        return null;
    } else {
        const existingFilters = subquery.where ? [subquery.where] : []
        return {
            ...subquery,
            limit: relationship.relationship_type === "object" ? 1 : subquery.limit, // If it's an object relationship, we expect only one result to come back, so we can optimise the query by limiting the filtering stop after one row
            where: {type: "and", expressions: [filterExpression, ...existingFilters]}
        };
    }
};

const coerceForeignKey = (table: TableInfo | undefined, config: Config, name: string, value: any): any => {
    if (table) {
        const columnCasing = applyCasingColumn(config.column_name_casing);
        const physicalName = table?.columns.find((column) => name == columnCasing(column.name))?.name ?? ''
        switch (sparkConfig?.coerceForeignKeys[table.name[0]]?.[physicalName]) {
            case 'integer' :
                return parseInt(value);
            case 'float':
                return parseFloat(value);
            case 'string':
                return value.toString();
            default:
                return value;
        }
    } else {
        return value
    }
}

const projectRow = (
    tableName: string,
    fields: Record<string, Field>,
    findRelationship: (relationshipName: RelationshipName) => Relationship,
    performQuery: (tableName: TableName, query: Query | null) => Promise<QueryResponse>,
    config: Config) => async (row: Record<string, RawScalarValue>): Promise<ProjectedRow> => {
    const tableNameCasing = applyCasingTable(config.table_name_casing);
    const table = schema.tables.find((table) => tableNameCasing(table.name[0]) == tableNameCasing(tableName));
    const projectedRow: ProjectedRow = {};
    for (const [fieldName, field] of Object.entries(fields)) {

        switch (field.type) {
            case "column":
                projectedRow[fieldName] = coerceUndefinedToNull(row[field.column]);
                break;

            case "relationship":
                const relationship = findRelationship(field.relationship);
                const subquery = addRelationshipFilterToQuery(row, relationship, field.query);
                const keyName = Object.entries(relationship.column_mapping)[0][0];
                const key = coerceForeignKey(table, config, keyName, row[keyName])
                const loaderKey: Key = {relationship, config, key, performQuery, query: subquery, fieldName};
                let rows = await loader.load(loaderKey);
                const calculatedAggregates = subquery?.aggregates
                    ? calculateAggregates(rows, subquery.aggregates)
                    : null;
                if (rows && !Array.isArray(rows)) {
                    rows = [rows]
                }
                projectedRow[fieldName] = subquery ? {
                    aggregates: calculatedAggregates,
                    rows: rows?.filter(Boolean) || []
                } as any : {
                    aggregates: null,
                    rows: null
                }
                break;

            default:
                return unreachable(field["type"] as never);
        }
    }
    return projectedRow;
};

const starCountAggregateFunction = (rows: Record<string, RawScalarValue>[]): RawScalarValue => {
    return rows?.length || 0;
};

const columnCountAggregateFunction = (aggregate: ColumnCountAggregate) => (rows: Record<string, RawScalarValue>[]): RawScalarValue => {
    const nonNullValues = rows.map(row => row[aggregate.column]).filter(v => v !== null);

    return aggregate.distinct
        ? (new Set(nonNullValues)).size
        : nonNullValues.length;
};

const isNumberArray = (values: RawScalarValue[]): values is number[] => {
    return values.every(v => typeof v === "number");
};

const isComparableArray = (values: RawScalarValue[]): values is (number | string)[] => {
    return values.every(v => typeof v === "number" || typeof v === "string");
};

const isStringArray = (values: RawScalarValue[]): values is string[] => {
    return values.every(v => typeof v === "string");
};

const singleColumnAggregateFunction = (aggregate: SingleColumnAggregate) => (rows: Record<string, RawScalarValue>[]): RawScalarValue => {
    const values = rows.map(row => row[aggregate.column]).filter((v): v is Exclude<RawScalarValue, null> => v !== null);
    if (values.length === 0)
        return null;

    if (!isComparableArray(values)) {
        throw new Error(`Found non-comparable scalar values when computing ${aggregate.function}`);
    }
    switch (aggregate.function) {
        case "max":
            return values.reduce((prev, curr) => prev > curr ? prev : curr);
        case "min":
            return values.reduce((prev, curr) => prev < curr ? prev : curr);
    }

    if (isStringArray(values)) {
        switch (aggregate.function) {
            case "longest":
                return values.reduce((prev, curr) => prev.length > curr.length ? prev : curr);
            case "shortest":
                return values.reduce((prev, curr) => prev.length < curr.length ? prev : curr);
        }
    }

    if (!isNumberArray(values)) {
        throw new Error(`Found non-numeric scalar values when computing ${aggregate.function}`);
    }
    switch (aggregate.function) {
        case "avg":
            return math.mean(values);
        case "stddev_pop":
            return math.std(values, "uncorrected");
        case "stddev_samp":
            return math.std(values, "unbiased");
        case "stddev":
            return math.std(values, "unbiased");
        case "sum":
            return math.sum(values);
        case "var_pop":
            return math.variance(values, "uncorrected");
        case "var_samp":
            return math.variance(values, "unbiased");
        case "variance":
            return math.variance(values, "unbiased");
        default:
            return unknownAggregateFunction(aggregate.function);
    }
};

const getAggregateFunction = (aggregate: Aggregate): ((rows: Record<string, RawScalarValue>[]) => RawScalarValue) => {
    switch (aggregate.type) {
        case "star_count":
            return starCountAggregateFunction;
        case "column_count":
            return columnCountAggregateFunction(aggregate);
        case "single_column":
            return singleColumnAggregateFunction(aggregate);
    }
};

const calculateAggregates = (rows: Record<string, RawScalarValue>[], aggregateRequest: Record<string, Aggregate>): Record<string, RawScalarValue> => {
    return Object.fromEntries(Object.entries(aggregateRequest).map(([fieldName, aggregate]) => {
        const aggregateValue = getAggregateFunction(aggregate)(rows);
        return [fieldName, aggregateValue];
    }));
};

const makeForeachFilterExpression = (foreachFilterIds: Record<string, ScalarValue>): Expression => {
    const expressions: Expression[] = Object.entries(foreachFilterIds)
        .map(([columnName, columnScalarValue]) => (
            {
                type: "binary_op",
                operator: "equal",
                column: {
                    name: columnName,
                    column_type: columnScalarValue.value_type
                },
                value: {
                    type: "scalar",
                    value: columnScalarValue.value,
                    value_type: columnScalarValue.value_type,
                }
            }
        ));

    return expressions.length === 1
        ? expressions[0]
        : {type: "and", expressions};
}

const addKeyFieldsToQuery = (table: TableInfo | undefined, query: Query | null, config: Config): Query | null => {
    const columnNameCasing = applyCasingColumn(config.column_name_casing);
    if (table) {
        const _query = query || {fields: {}}
        table.primary_key?.map((physicalColumnName) => {
            const column = columnNameCasing(physicalColumnName);
            if (!_query.fields) {
                _query.fields = {}
            }
            if (!_query.fields[column]) {
                _query.fields[column] = {
                    type: 'column',
                    column: column,
                    column_type: table.columns.find((c) => c.name == physicalColumnName)?.type ?? 'string'
                }
            }
        })
        Object.entries(table.foreign_keys ?? {}).forEach(([_, constraint]) => {
            const physicalColumnName = Object.keys(constraint.column_mapping)[0];
            const column = columnNameCasing(physicalColumnName);
            if (!_query.fields) {
                _query.fields = {}
            }
            if (!_query.fields[column]) {
                _query.fields[column] = {
                    type: 'column',
                    column: column,
                    column_type: table.columns.find((c) => c.name == physicalColumnName)?.type ?? 'string'
                }
            }
        })
    }
    return query;
}

const addAgrregateFieldsToQuery = (table: TableInfo | undefined, query: Query | null, config: Config): Query | null => {
    const columnNameCasing = applyCasingColumn(config.column_name_casing);
    if (table) {
        const _query = query || {fields: {}, aggregates: {}}
        Object.entries(_query.aggregates ?? {}).forEach(([_, aggregate]) => {
            if (aggregate.type == 'single_column') {
                const column = aggregate.column;
                if (!_query.fields) {
                    _query.fields = {}
                }
                if (!_query.fields[column]) {
                    _query.fields[column] = {
                        type: 'column',
                        column: column,
                        column_type: table.columns.find((c) => columnNameCasing(c.name) == column)?.type ?? 'string'
                    }
                }
            }
        })
    }
    return query;
}
export const queryData = async (getTable: (tableName: TableName, query: Query | null) => Promise<SparkRowResults>, queryRequest: QueryRequest, config: Config): Promise<QueryResponse> => {
    const performQuery = async (parentQueryRowChain: Record<string, RawScalarValue>[], tableName: TableName, query: Query | null): Promise<QueryResponse> => {
        const tableNameCasing = applyCasingTable(config.table_name_casing);
        const table = schema.tables.find((table) => tableNameCasing(table.name[0]) == tableNameCasing(tableName[0]));
        query = addAgrregateFieldsToQuery(table, addKeyFieldsToQuery(table, query, config), config);
        const {
            /*total, */ // might be used later for pagination
            rows
        } = await getTable(tableName, query);
        if (rows === undefined) {
            throw `${tableName} is not a valid table`;
        }
        const findRelationship = makeFindRelationship(queryRequest.table_relationships, tableName);

        // Get the smallest set of rows required _for both_ row results and aggregation result
        const aggregatesLimit = query?.aggregates_limit ?? null;

        // Limit the set of input rows to appropriate size for row results and aggregation results
        const paginatedRowsForAggregation = aggregatesLimit != null ? rows.slice(0, aggregatesLimit) : rows;

        const projectedRows = query?.fields
            ? await Promise.all(rows.map(projectRow(tableName[0], query?.fields, findRelationship, performNewQuery, config)))
            : null;
        const calculatedAggregates = query?.aggregates
            ? calculateAggregates(paginatedRowsForAggregation, query?.aggregates)
            : null;
        return {
            aggregates: calculatedAggregates,
            rows: projectedRows,
        }
    }
    const performNewQuery = async (tableName: TableName, query: Query | null): Promise<QueryResponse> => await performQuery([], tableName, query);

    if (queryRequest.foreach) {
        return {
            rows: (await Promise.all(queryRequest.foreach.map(foreachFilterIds => {
                const foreachFilter = makeForeachFilterExpression(foreachFilterIds);
                const where: Expression = queryRequest.query.where
                    ? {type: "and", expressions: [foreachFilter, queryRequest.query.where]}
                    : foreachFilter;

                const filteredQuery = {
                    ...queryRequest.query,
                    where
                }
                return performNewQuery(queryRequest.table, filteredQuery);
            }))).map((query) => ({query}))
        };
    } else {
        return await performNewQuery(queryRequest.table, queryRequest.query);
    }
};

const unknownOperator = (x: string): never => {
    throw new Error(`Unknown operator: ${x}`)
};

const unknownAggregateFunction = (x: string): never => {
    throw new Error(`Unknown aggregate function: ${x}`)
};
