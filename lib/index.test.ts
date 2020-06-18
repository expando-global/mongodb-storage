import test from 'ava';

import { db } from './db-connector';
import { prepareDb } from '../testing/prepare-db';
import * as Joi from 'typesafe-joi';
import _ from 'lodash';
import timekeeper from 'timekeeper';
import { makeMockRequestContext } from '../testing/mock-request-context';
import { createChangelog, makeStorage } from '.';

import { IndexSpecification, ObjectId } from 'mongodb';
import {
    IOrderDocument,
    OrderDocumentSchema,
} from '../testing/test-order-schema';

export const sleep = (ms = 1000) =>
    new Promise((resolve) => setTimeout(resolve, ms));

prepareDb(test);

class Abstraction {
    constructor(public name: string) {}

    shoutAtMama() {
        return `hey ${this.name}`;
    }

    toJSON() {
        return {
            name: `${this.name} was serialized`,
        };
    }
}

const TestSubResource = Joi.object({
    id: Joi.object().type(ObjectId).required(),
    hello: Joi.string().required(),
}).required();

type ISubresourceTest = Joi.Literal<typeof TestSubResource>;

const TestStorageSchema = Joi.object({
    someField: Joi.string().required(),
    someClass: Joi.object().type(Abstraction),
    subResource: Joi.array().items(TestSubResource).optional(),
    changelogs: Joi.any().optional(),
}).required();

type ITest = Joi.Literal<typeof TestStorageSchema>;

test.serial('storage creates a collection', async (t) => {
    t.plan(2);

    const hasCollection = async (name: string) => {
        const collectionNames = (await (await db()).collections()).map(
            (c) => c.collectionName,
        );
        return collectionNames.includes(name);
    };

    t.false(await hasCollection('storageTestCollection'));

    const TestStorage = makeStorage(
        'test',
        'storageTestCollection',
        TestStorageSchema,
        [],
    );

    await sleep(100);

    t.true(await hasCollection('storageTestCollection'));
});

test.serial('storage creates new and removes old indexes', async (t) => {
    t.plan(6);

    const identity = (i: IndexSpecification) => JSON.stringify(i.key);
    const hasIndex = async (index: IndexSpecification) => {
        const indexes = (
            await (await db())
                .collection('storageTestCollection')
                .listIndexes()
                .toArray()
        ).map(identity);

        return indexes.includes(identity(index));
    };

    const someFieldIndex = { key: { someField: -1 } };
    const companyIndex = { key: { companyId: 1 } };

    t.false(await hasIndex(someFieldIndex));
    t.false(await hasIndex(companyIndex));

    makeStorage('test', 'storageTestCollection', TestStorageSchema, [
        someFieldIndex,
        companyIndex,
    ]);

    await sleep(100);

    t.true(await hasIndex(someFieldIndex));
    t.true(await hasIndex(companyIndex));

    await sleep(10);

    makeStorage('test', 'storageTestCollection', TestStorageSchema, [
        someFieldIndex,
    ]);

    await sleep(100);

    t.true(await hasIndex(someFieldIndex));
    t.false(await hasIndex(companyIndex));
});

test.serial('storage maps internal `_id` to `id`', async (t) => {
    const rc = makeMockRequestContext();
    const testDoc = {
        someField: 'yomama',
    };

    const TestStorage = makeStorage<ITest>(
        'test',
        'storageTestCollection',
        TestStorageSchema,
        [],
    );

    await sleep(100);

    const res = await TestStorage.insertOne(rc, testDoc);

    t.is(res.someField, 'yomama');
    t.true(_.has(res, 'id'));
    t.false(_.has(res, '_id'));
});

test.serial('storage serializes object to JSON', async (t) => {
    const rc = makeMockRequestContext();
    const testDoc = {
        someField: 'yomama',
        someClass: new Abstraction('yomama'),
    };

    const TestStorage = makeStorage<ITest>(
        'test',
        'storageTestCollection',
        TestStorageSchema,
        [],
    );

    await sleep(100);

    const res = await TestStorage.insertOne(rc, testDoc);

    t.is(res.someField, 'yomama');
    t.deepEqual(res.someClass, {
        name: 'yomama was serialized',
    } as Abstraction);
});

test.serial('pagination works with sort', async (t) => {
    const testingStorage = makeStorage(
        'test',
        'testingCollection',
        Joi.object({}),
        [{ key: { _id: 1 } }],
    );
    const documents = [
        {
            _id: new ObjectId(4),
            lang: 'four',
        },
        {
            _id: new ObjectId(1),
            lang: 'one',
        },
        {
            _id: new ObjectId(3),
            lang: 'three',
        },
        {
            _id: new ObjectId(2),
            lang: 'two',
        },
    ];

    const expectedDocuments = documents.map((doc) => ({
        id: doc._id,
        ..._.omit(doc, '_id'),
    }));

    await (await db()).collection('testingCollection').insertMany(documents);

    // limiting number of documents returned
    t.deepEqual(await testingStorage.findMany({}, { id: -1 }, 2, 1), [
        expectedDocuments[0],
        expectedDocuments[2],
    ]);

    // getting second page
    t.deepEqual(await testingStorage.findMany({}, { id: -1 }, 2, 2), [
        expectedDocuments[3],
        expectedDocuments[1],
    ]);

    // getting page after the last page
    t.deepEqual(await testingStorage.findMany({}, { id: -1 }, 2, 3), []);

    // getting non-existing page
    t.deepEqual(await testingStorage.findMany({}, { id: -1 }, 2, 500), []);
});

test('creating a changelog', (t) => {
    const now = new Date();
    timekeeper.freeze(now);
    const rc = makeMockRequestContext();

    const originalDoc = {
        someField: 'hey',
        how: 'do',
        you: 'do',
        someNestedResource: [{ oh: 'no' }],
    };

    const changes = {
        someField: 'hello',
        how: 'do',
        someNestedResource: [{ oh: 'maybe', hi: 'howAreYou' }, { oh: 'yeah' }],
    };

    const result = createChangelog(rc, originalDoc, changes);

    t.deepEqual(result, {
        ip: '192.168.0.66',
        token: '******X-9a8dF2',
        endpoint: 'POST http://localhost:3333/resources/resourceId',
        timestamp: now,
        changes: [
            {
                kind: 'E',
                path: ['someField'],
                lhs: 'hey',
                rhs: 'hello',
            },
            {
                kind: 'A',
                path: ['someNestedResource'],
                index: 1,
                item: { kind: 'N', rhs: { oh: 'yeah' } },
            },
            {
                kind: 'E',
                path: ['someNestedResource', 0, 'oh'],
                lhs: 'no',
                rhs: 'maybe',
            },
            {
                kind: 'N',
                path: ['someNestedResource', 0, 'hi'],
                rhs: 'howAreYou',
            },
        ],
    });
});

test.serial('storage creates a changelog on insert & update', async (t) => {
    const now = new Date();
    timekeeper.freeze(now);
    const rc = makeMockRequestContext();
    const testDoc = {
        someField: 'yomama',
    };

    const TestStorage = makeStorage<ITest>(
        'test',
        'storageChangelogTestCollection',
        TestStorageSchema,
        [],
    );

    await sleep(100);

    const res = await TestStorage.insertOne(rc, testDoc);

    t.is(res.someField, 'yomama');
    t.true(_.has(res, 'id'));
    t.false(_.has(res, '_id'));

    t.true(_.has(res, 'changelogs'));
    t.is(res.changelogs.length, 1);
    t.true(_.has(res, 'changelogs.0.ip'));
    t.true(_.has(res, 'changelogs.0.token'));
    t.true(_.has(res, 'changelogs.0.endpoint'));
    t.deepEqual(res.changelogs[0].timestamp, now);
    t.deepEqual(_.get(res, 'changelogs.0.changes'), []);

    timekeeper.reset();

    const { originalDocument, commit } = await TestStorage.findOneAndUpdate(
        rc,
        testDoc,
    );

    if (!originalDocument) return t.fail('No original doc found');

    Object.assign(originalDocument, { someField: 'yodaddy' });
    const updRes = await commit(originalDocument);

    if (!updRes) return t.fail('No updated doc returned');

    t.true(_.has(updRes, 'changelogs'));
    t.is(updRes.changelogs.length, 2);
    t.true(_.has(updRes, 'changelogs.1.ip'));
    t.true(_.has(updRes, 'changelogs.1.token'));
    t.true(_.has(updRes, 'changelogs.1.endpoint'));
    t.notDeepEqual(updRes.changelogs[1].timestamp, now);
    t.is(_.get(updRes, 'changelogs.1.changes.length'), 1);
});

test.serial('storage finds many subdocuments', async (t) => {
    const rc = makeMockRequestContext();
    const testDoc = {
        someField: 'yomama',
        subResource: [
            {
                id: new ObjectId('5eccd9fdb9f8a700231b8a40'),
                hello: "it's me",
            },
            {
                id: new ObjectId('5eccd9fdb9f8a700231b8a41'),
                hello: 'how are you',
            },
        ],
    };

    const TestStorage = makeStorage<ITest>(
        'test',
        'subdocsChangelogTestCollection',
        TestStorageSchema,
        [],
    );

    await sleep(100);

    const res = await TestStorage.insertOne(rc, testDoc);

    t.is(res.someField, 'yomama');
    t.true(_.has(res, 'id'));
    t.false(_.has(res, '_id'));
    t.true(_.has(res, 'subResource'));

    const subRes = await TestStorage.findManySubdocuments<ISubresourceTest[]>(
        'subResource',
        {
            someField: 'yomama',
        },
    );

    t.is(subRes.length, 2);
    t.true(
        subRes.every((item) =>
            //@ts-ignore
            Object.keys(item).every((k) => ['id', 'hello'].includes(k)),
        ),
    );
});

test.serial('storage finds single subdocument', async (t) => {
    const rc = makeMockRequestContext();
    const testDoc = {
        someField: 'yomama',
        subResource: [
            {
                id: new ObjectId('5eccd9fdb9f8a700231b8a40'),
                hello: "it's me",
            },
            {
                id: new ObjectId('5eccd9fdb9f8a700231b8a41'),
                hello: 'how are you',
            },
        ],
    };

    const TestStorage = makeStorage<ITest>(
        'test',
        'subdocChangelogTestCollection',
        TestStorageSchema,
        [],
    );

    await sleep(100);

    const res = await TestStorage.insertOne(rc, testDoc);

    t.is(res.someField, 'yomama');
    t.true(_.has(res, 'subResource'));

    const subRes = await TestStorage.findOneSubdocument<ISubresourceTest>(
        'subResource',
        {
            someField: 'yomama',
        },
        {
            id: new ObjectId('5eccd9fdb9f8a700231b8a41'),
        },
    );

    t.deepEqual(subRes, {
        id: new ObjectId('5eccd9fdb9f8a700231b8a41'),
        hello: 'how are you',
    });
});

test.serial('getting many orders with filter', async (t) => {
    const rc = makeMockRequestContext();
    const orders = [
        {
            _id: new ObjectId('5ecfb65da04c74056eaa32d8'),
            companyId: new ObjectId('5c6a9bcfe051d00004b7056e'),
            purchaseDate: new Date('2020-05-26T13:02:21.435Z'),
            lastChanged: new Date('2020-05-26T13:02:21.435Z'),
            latestShipDate: new Date('2020-05-28T13:02:21.435Z'),
            statusDate: {},
            channel: 'alza_cz',
            channelOrderId: '123455',
            fulfillmentService: 'Seller',
            status: 'Pending',
            fulfillmentStatus: 'Unshipped',
            totalItemsCount: 2,
            unshippedCount: 0,
            shippedCount: 0,
            deliveredCount: 0,
            rejectedCount: 0,
        },
        {
            _id: new ObjectId('5ecfb65da04c74056eaa32d9'),
            companyId: new ObjectId('5c6a9bcfe051d00004b7056e'),
            purchaseDate: new Date('2020-05-28T13:02:21.435Z'),
            lastChanged: new Date('2020-05-28T13:02:21.435Z'),
            latestShipDate: new Date('2020-05-30T13:02:21.435Z'),
            statusDate: {},
            channel: 'amazon_de',
            channelOrderId: '123456',
            fulfillmentService: 'FBA',
            status: 'Pending',
            fulfillmentStatus: 'Unshipped',
            totalItemsCount: 2,
            unshippedCount: 0,
            shippedCount: 0,
            deliveredCount: 0,
            rejectedCount: 0,
        },
        {
            _id: new ObjectId('5ecfb65da04c74056eaa32d7'),
            companyId: new ObjectId('5c6a9bcfe051d00004b7056e'),
            purchaseDate: new Date('2020-05-26T13:02:21.435Z'),
            lastChanged: new Date('2020-05-28T13:02:21.435Z'),
            latestShipDate: new Date('2020-05-30T13:02:21.435Z'),
            statusDate: {},
            channel: 'amazon_es',
            channelOrderId: '123458',
            fulfillmentService: 'FBA',
            status: 'Pending',
            fulfillmentStatus: 'Unshipped',
            totalItemsCount: 2,
            unshippedCount: 0,
            shippedCount: 0,
            deliveredCount: 0,
            rejectedCount: 0,
        },
    ];

    const TestStorage = makeStorage<IOrderDocument>(
        'test',
        'ordersTestCollection',
        OrderDocumentSchema,
        [],
    );

    await sleep(100);

    await TestStorage.insertOne(rc, orders[0]);
    await TestStorage.insertOne(rc, orders[1]);

    const filterByOneChannel = {
        channel: 'alza_cz',
    };
    t.is(
        (await TestStorage.findMany(filterByOneChannel))[0].channelOrderId,
        '123455',
    );

    const filterByTwoChannels = {
        channel: ['alza_cz', 'amazon_de'],
    };
    t.deepEqual(
        (await TestStorage.findMany(filterByTwoChannels, { _id: -1 })).map(
            (order) => order.channelOrderId,
        ),
        ['123456', '123455'],
    );

    const filterByDate = {
        purchasedAfter: new Date('2020-05-27T13:02:21.435Z'),
    };
    t.is(
        (await TestStorage.findMany(filterByDate))[0].channelOrderId,
        '123456',
    );
});

test.serial('storage finds and updates subdocument', async (t) => {
    const rc = makeMockRequestContext();
    const testDoc = {
        someField: 'yomama',
        subResource: [
            {
                id: new ObjectId('5eccd9fdb9f8a700231b8a40'),
                hello: "it's me",
            },
            {
                id: new ObjectId('5eccd9fdb9f8a700231b8a41'),
                hello: 'how are you',
            },
        ],
    };

    const TestStorage = makeStorage<ITest>(
        'test',
        'subdocUpdateTestCollection',
        TestStorageSchema,
        [],
    );

    await sleep(100);

    const res = await TestStorage.insertOne(rc, testDoc);

    t.is(res.someField, 'yomama');
    t.true(_.has(res, 'subResource'));

    const {
        documentReference,
        subdocumentReference,
        commit,
    } = await TestStorage.findOneSubdocumentAndUpdate<ISubresourceTest>(
        rc,
        {
            someField: 'yomama',
        },
        'subResource',
        (subRes: ISubresourceTest) =>
            subRes.id.equals(new ObjectId('5eccd9fdb9f8a700231b8a41')),
    );

    if (!documentReference || !subdocumentReference)
        t.fail('Some of sub/doc references are null');

    t.deepEqual(documentReference?.someField, 'yomama');
    t.deepEqual(subdocumentReference, {
        id: new ObjectId('5eccd9fdb9f8a700231b8a41'),
        hello: 'how are you',
    });

    Object.assign(subdocumentReference, { hello: 'can you hear me' });

    // Check whether document is updated via subdocument reference
    // @ts-ignore
    t.deepEqual(documentReference?.subResource[1].hello, 'can you hear me');
    t.deepEqual(subdocumentReference, {
        id: new ObjectId('5eccd9fdb9f8a700231b8a41'),
        hello: 'can you hear me',
    });

    // @ts-ignore
    const updateResult = await commit(documentReference);

    t.deepEqual(updateResult.changelogs[1].changes, [
        {
            kind: 'E',
            path: ['subResource', 1, 'hello'],
            lhs: 'how are you',
            rhs: 'can you hear me',
        },
    ]);
});
