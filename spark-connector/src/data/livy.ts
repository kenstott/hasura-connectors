/*
 * Copyright (c) 2023 Kenneth R. Stott.
 */

import axios, {AxiosResponse} from "axios";
import {server} from '../index'
import {sparkSession} from "./spark/init";

interface LivySessionResponse {
    state: 'idle' | 'waiting' | 'dead' | 'starting'
}

interface LivyStatementResponse {
    id: number;
    started: number;
    completed: number;
    output: {
        data?: Record<string, any>
    }
}

export const waitOnStatementResponse = async (response: AxiosResponse<LivyStatementResponse, any>): Promise<AxiosResponse<LivyStatementResponse, any>> => {
    while (response.data.completed == 0) {
        response = await axios.get(`${process.env.LIVY_URI}/sessions/${sparkSession}/statements/${response.data.id}`)
    }
    server.log.info(response.data?.output?.data?.["text/plain"] || '');
    return response;
}
export const waitOnSessionResponse = async (response: AxiosResponse<LivySessionResponse, any>): Promise<AxiosResponse<any, any>> => {
    while (response.data.state !== 'idle') {
        if (response.data.state == 'dead') {
            throw new Error("Livy server is responding with 'dead'")
        }
        response = await axios.get(`${process.env.LIVY_URI}/sessions/${sparkSession}`)
    }
    return response;
}