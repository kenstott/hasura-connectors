/*
 * Copyright (c) 2023 Kenneth R. Stott.
 */

import {
    AndExpression,
    AnotherColumnComparison,
    ApplyBinaryArrayComparisonOperator,
    ApplyBinaryComparisonOperator,
    Expression,
    NotExpression,
    OrExpression,
    Query,
    ScalarValueComparison
} from "@hasura/dc-api-types";
import {waitOnStatementResponse} from "../livy";
import axios from "axios";
import {changeOrderByColumnNames, changeWhereColumnNames, getFieldNames, getOperator, getValueString} from "./util";
import {sparkSession} from "./init";
import {Config} from "../../config";

const getWhereClause = (tableName: string, where?: Record<string, any>): string => {
    if (where?.type == 'and') {
        const andExpression = where as AndExpression
        const andExpressions = andExpression?.expressions.map((e) => getWhereClause(tableName, e)).map((e) => `(${e})`)
        return `(${andExpressions.join(' AND ')})`;
    } else if (where?.type == 'or') {
        const orExpression = where as OrExpression
        const orExpressions = orExpression?.expressions.map((e) => getWhereClause(tableName, e)).map((e) => `(${e})`)
        return `(${orExpressions.join(' OR')})`;
    } else if (where?.type == 'not') {
        const notExpression = where as NotExpression;
        return `NOT ${getWhereClause(tableName, notExpression.expression)}`;
    } else if (where?.type == 'binary_arr_op') {
        const binaryArrayComparison = where as ApplyBinaryArrayComparisonOperator;
        if (binaryArrayComparison.operator == 'in') {
            return `\`${binaryArrayComparison.column.name}\` in(${getValueString(binaryArrayComparison)})`;
        }
    } else if (where?.type == 'binary_op') {
        const binaryComparison = where as ApplyBinaryComparisonOperator;
        if (binaryComparison.value.type == 'scalar') {
            const scalarValue = binaryComparison.value as ScalarValueComparison;
            return `\`${binaryComparison.column.name}\` ${getOperator[binaryComparison.operator]} ${getValueString(scalarValue)}`;
        } else {
            const columnValue = binaryComparison.value as AnotherColumnComparison;
            return `\`${binaryComparison.column.name}\` ${getOperator[binaryComparison.operator]} ${columnValue.column.name}`;
        }
    }
    return '';
}
export const getTableRows = async (tableName: string, config: Config, query?: Query | null) => {
    const fieldClause = getFieldNames(tableName, config, query?.fields).map((i) => '`' + i + '`') ?? "*";
    const fieldStatement = fieldClause.join(', ');
    const sortClause = changeOrderByColumnNames(tableName, config, query?.order_by?.elements)?.map((i) => `\`${i.columnName}\` ${i.direction}`).join(',');
    const sortStatement = sortClause ? ` order by ${sortClause}` : '';
    const whereClause = getWhereClause(tableName, changeWhereColumnNames(tableName, config, query?.where) as Expression);
    const whereStatement = whereClause ? ` WHERE ${whereClause} ` : '';
    const paginationStatement = query?.limit ? `.slice(${query?.offset ?? 0},${(query?.offset ?? 0) + (query?.limit ?? 0)})` : '';

    // scala code for spark...
    const code =
        `
        val temp = spark.sqlContext.sql("select ${fieldStatement} from ${tableName}${whereStatement}${sortStatement}")
        println(temp.count);
        temp.toJSON.collect${paginationStatement}.foreach { println }
        `
    // response from Livy/Spark
    const response = await waitOnStatementResponse(await axios.post(`${process.env.LIVY_URI}/sessions/${sparkSession}/statements`, {code}));
    const rows = response.data.output.data?.['text/plain']
        .split('\n')
        .filter(Boolean) // get rid of blanks
        .slice(1) // first line is not part of data - it's a total count - which current isn't used but might be helpful for pagination
        .map((item: string) => JSON.parse(item)) // convert each line to JSON
    return {
        total: rows[0], // first row is
        rows: rows?.slice(1)
    };
}

