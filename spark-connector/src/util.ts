import {TableName} from "@hasura/dc-api-types";

export const coerceUndefinedToNull = <T>(v: T | undefined): T | null => v === undefined ? null : v;

export const unreachable = (x: never): never => {
    throw new Error(`Unreachable code reached! The types lied! 😭 Unexpected value: ${x}`)
};

export const zip = <T, U>(arr1: T[], arr2: U[]): [T, U][] => {
    const length = Math.min(arr1.length, arr2.length);
    const newArray = Array(length);
    for (let i = 0; i < length; i++) {
        newArray[i] = [arr1[i], arr2[i]];
    }
    return newArray;
};

export const mapObject = <T, U>(obj: Record<string, T>, fn: (entry: [string, T]) => [string, U]): Record<string, U> => {
    return Object.fromEntries(Object.entries(obj).map(fn));
}

export const mapObjectValues = <T, U>(obj: Record<string, T>, fn: (value: T, propertyName: string) => U): Record<string, U> => {
    return Object.fromEntries(Object.entries(obj).map(([prop, val]) => [prop, fn(val, prop)]));
}

export const tableNameEquals = (tableName1: TableName) => (tableName2: TableName): boolean => {
    if (tableName1.length !== tableName2.length)
        return false;

    return zip(tableName1, tableName2).every(([n1, n2]) => n1 === n2);
}
