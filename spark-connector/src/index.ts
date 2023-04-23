﻿import Fastify from 'fastify';
import FastifyCors from '@fastify/cors';
import {filterAvailableTables, getSchema, getTable, StaticData} from './data';
import {queryData} from './query';
import {getConfig} from './config';
import {capabilitiesResponse} from './capabilities';
import {
    CapabilitiesResponse,
    DatasetCreateCloneRequest,
    DatasetCreateCloneResponse,
    DatasetDeleteCloneResponse,
    DatasetGetTemplateResponse,
    QueryRequest,
    QueryResponse,
    SchemaResponse
} from '@hasura/dc-api-types';
import {cloneDataset, defaultDbStoreName, deleteDataset, getDataset, getDbStoreName} from './datasets';
import {config as dotConfig} from 'dotenv';
import {loadSqlContext} from "./data/spark/init";

dotConfig();


const port = Number(process.env.PORT) || 8100;
const server = Fastify({logger: {transport: {target: 'pino-pretty'}}});
let sparkMetadata: Record<string, StaticData> = {};

server.register(FastifyCors, {
    // Accept all origins of requests. This must be modified in
    // a production setting to be specific allowable list
    // See: https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Access-Control-Allow-Origin
    origin: true,
    methods: ["GET", "POST", "OPTIONS"],
    allowedHeaders: ["X-Hasura-DataConnector-Config", "X-Hasura-DataConnector-SourceName"]
});

server.get<{ Reply: CapabilitiesResponse }>("/capabilities", async (request, _response) => {
    server.log.info({headers: request.headers, query: request.body,}, "capabilities.request");
    return capabilitiesResponse;
});

server.get<{ Reply: SchemaResponse }>("/schema", async (request, _response) => {
    server.log.info({headers: request.headers, query: request.body,}, "schema.request");
    const config = getConfig(request);
    return getSchema(config);
});

server.post<{ Body: QueryRequest, Reply: QueryResponse }>("/query", async (request, _response) => {
    server.log.info({headers: request.headers, query: request.body,}, "query.request");

    const config = getConfig(request);
    const dbStoreName = config.db ? getDbStoreName(config.db) : defaultDbStoreName;

    if (!(dbStoreName in sparkMetadata))
        throw new Error(`Cannot find database ${config.db}`);

    const data = filterAvailableTables(sparkMetadata[dbStoreName], config);
    return await queryData(await getTable(data, config), request.body, config);
});

// Methods on dataset resources.
//
// Examples:
//
// > curl -H 'content-type: application/json' -XGET localhost:8100/datasets/templates/Chinook
// {"exists": true}
//
server.get<{
    Params: { name: string, },
    Reply: DatasetGetTemplateResponse
}>("/datasets/templates/:name", async (request, _response) => {
    server.log.info({headers: request.headers, query: request.body,}, "datasets.templates.get");
    return getDataset(request.params.name);
});

// > curl -H 'content-type: application/json' -XPOST localhost:8100/datasets/clones/foo -d '{"from": "Chinook"}'
// {"config":{"db":"$foo"}}
//
server.post<{
    Params: { name: string, },
    Body: DatasetCreateCloneRequest,
    Reply: DatasetCreateCloneResponse
}>("/datasets/clones/:name", async (request, _response) => {
    server.log.info({headers: request.headers, query: request.body,}, "datasets.clones.post");
    return cloneDataset(sparkMetadata, request.params.name, request.body);
});

// > curl -H 'content-type: application/json' -XDELETE 'localhost:8100/datasets/clones/foo'
// {"message":"success"}
//
server.delete<{
    Params: { name: string, },
    Reply: DatasetDeleteCloneResponse
}>("/datasets/clones/:name", async (request, _response) => {
    server.log.info({headers: request.headers, query: request.body,}, "datasets.clones.delete");
    return deleteDataset(sparkMetadata, request.params.name);
});

server.get("/health", async (request, response) => {
    server.log.info({headers: request.headers, query: request.body,}, "health.request");
    response.statusCode = 204;
});

process.on('SIGINT', () => {
    server.log.info("interrupted");
    process.exit(0);
});

const start = async () => {
    try {
        sparkMetadata = {[defaultDbStoreName]: await loadSqlContext(process.env.SPARK_CONNECTOR_FILES || 'src/data/test')};
        await server.listen({port: port, host: "0.0.0.0"});
    } catch (err) {
        server.log.fatal(err);
        process.exit(1);
    }
};
start().then(() => server.log.info('Started service'));
