import axios from "axios";
import {waitOnStatementResponse} from "../livy";
import {sparkSession} from "./init";
import {SparkTableMetadata} from "./types";

export const getTableMetadata = async (tableName: string): Promise<SparkTableMetadata> => {
    const code = `println(${tableName}.schema.json)`
    const response = await waitOnStatementResponse(await axios.post(`${process.env.LIVY_URI}/sessions/${sparkSession}/statements`, {code}));
    return JSON.parse(response.data.output.data?.['text/plain']);
}