import axios, {AxiosResponse} from "axios";

import {sparkSession} from "./spark/init";

interface LivySessionResponse {
    state: 'idle' | 'waiting'
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
    return response;
}
export const waitOnSessionResponse = async (response: AxiosResponse<LivySessionResponse, any>): Promise<AxiosResponse<any, any>> => {
    while (response.data.state !== 'idle') {
        response = await axios.get(`${process.env.LIVY_URI}/sessions/${sparkSession}`)
    }
    return response;
}