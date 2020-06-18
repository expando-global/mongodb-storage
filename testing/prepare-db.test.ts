import test from 'ava';

import { db } from '../lib/db-connector';
import { prepareDb } from './prepare-db';

prepareDb(test);

test.serial('inserts into db', async (t) => {
    await (await db())
        .collection('randomCollection')
        .insertOne({ test: 'test' });

    const returned = await (await db())
        .collection('randomCollection')
        .findOne({ test: 'test' });

    t.is('test' in returned, true);
});

test.serial('db is empty by now', async (t) => {
    const returned = await (await db())
        .collection('randomCollection')
        .countDocuments();

    t.is(returned, 0);
});
