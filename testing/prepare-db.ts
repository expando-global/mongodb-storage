import { MongoMemoryServer } from 'mongodb-memory-server-core';
import { TestInterface } from 'ava';

import { connectDb, disconnectDb, db } from '../lib/db-connector';

const mongod = new MongoMemoryServer({
    binary: {
        version: '4.2.8',
    },
});

export function prepareDb(test: TestInterface) {
    test.before(async () => {
        await connectDb(await mongod.getUri());
    });

    test.afterEach(async () => {
        const colls = await (await db()).collections();
        colls.forEach((coll) => coll.deleteMany({}));
    });

    test.after.always(async () => {
        disconnectDb();
        mongod.stop();
    });
}
